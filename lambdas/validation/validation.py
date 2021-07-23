import os
import io
import re
import json
import boto3
import logging
import http.client
import pandas as pd
from botocore.exceptions import ClientError


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

MANIFEST_REQUIRED_COLUMNS = ('filename', 'checksum', 'agha_study_id')
AWS_REGION = boto3.session.Session().region_name
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
SLACK_NOTIFY = os.environ.get('SLACK_NOTIFY')
EMAIL_NOTIFY = os.environ.get('EMAIL_NOTIFY')
SLACK_HOST = os.environ.get('SLACK_HOST')
SLACK_CHANNEL = os.environ.get('SLACK_CHANNEL')
MANAGER_EMAIL = os.environ.get('MANAGER_EMAIL')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
HEADERS = {'Content-Type': 'application/json'}
EMAIL_SUBJECT = '[AGHA service] Submission received'
AWS_ID_PATTERN = '[0-9A-Z]{21}'
EMAIL_PATTERN = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f"AWS:({AWS_ID_PATTERN})")
SSO_RE = re.compile(f"AWS:({AWS_ID_PATTERN}):({EMAIL_PATTERN})")

S3_CLIENT = boto3.client('s3')
SSM_CLIENT = boto3.client('ssm')
IAM_CLIENT = boto3.client('iam')
SES_CLIENT = boto3.client('ses',region_name=AWS_REGION)
SLACK_WEBHOOK_ENDPOINT = SSM_CLIENT.get_parameter(
    Name='/slack/webhook/endpoint',
    WithDecryption=True
    )['Parameter']['Value']


####
# Notifications
####
def log_and_store_file_message(message_text, files, message_store):
    # Notification only gets summary message; Lambda log gets both summary and full
    message_summary = f'{message_text}: {len(files)}'
    log_and_store_message(message_summary, message_store)
    if files:
        # NOTE(SW): using '\r' so that multi-line messages appears as a single line in Lambda log
        print(f'{message_text}:', *files, sep='\r\t')


def log_and_store_message(message, message_store):
    # NOTE(SW): message_store is updated in outer scopes
    print(message)
    message_store.append(message)


def notify_and_exit(messages, name, email, submission_prefix):
    send_notifications(messages, name, email, submission_prefix)
    sys.exit(1)


def send_notifications(messages, name, email, submission_prefix):
    if EMAIL_NOTIFY == 'yes':
        print(f'Sending notifications messages', end='\r')
        print(*messages, sep='\r')
        print(f'Sending email to {name}/{email}')
        email_response = send_email(
            recipients=[MANAGER_EMAIL, email],
            sender=SENDER_EMAIL,
            subject_text=EMAIL_SUBJECT,
            body_html=make_email_body_html(
                submission=submission_prefix,
                submitter=name,
                messages=messages)
        )
        print(f'Email send response: {email_response}')
    if SLACK_NOTIFY == 'yes':
        print(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = call_slack_webhook(
            topic='AGHA submission quick validation',
            title=f'Submission: {submission_prefix} ({name})',
            message='\n'.join(messages)
        )
        print(f'Slack call response: {slack_response}')


def send_email(recipients, sender, subject_text, body_html):
    try:
        # Provide the contents of the email.
        response = SES_CLIENT.send_email(
            Destination={
                'ToAddresses': recipients,
            },
            Message={
                'Subject': {
                    'Charset': 'utf8',
                    'Data': subject_text,
                },
                'Body': {
                    'Html': {
                        'Charset': 'utf8',
                        'Data': body_html,
                    }
                }
            },
            Source=sender,
        )
    # Display an error if something goes wrong
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        return "Email sent! Message ID:" + response['MessageId']


def call_slack_webhook(topic, title, message):
    connection = http.client.HTTPSConnection(SLACK_HOST)

    post_data = {
        "channel": SLACK_CHANNEL,
        "username": "Notice from AWS",
        "text": "*" + topic + "*",
        "icon_emoji": ":aws_logo:",
        "attachments": [{
            "title": title,
            "text": message
        }]
    }

    connection.request("POST", SLACK_WEBHOOK_ENDPOINT, json.dumps(post_data), HEADERS)
    response = connection.getresponse()
    connection.close()

    return response.status


def get_name_email_from_principalid(principal_id):
    if USER_RE.fullmatch(principal_id):
        user_id = re.search(USER_RE, principal_id).group(1)
        user_list = IAM_CLIENT.list_users()
        for user in user_list['Users']:
            if user['UserId'] == user_id:
                username = user['UserName']
        user_details = IAM_CLIENT.get_user(UserName=username)
        tags = user_details['User']['Tags']
        for tag in tags:
            if tag['Key'] == 'email':
                email = tag['Value']
        return username, email
    elif SSO_RE.fullmatch(principal_id):
        email = re.search(SSO_RE, principal_id).group(2)
        username = email.split('@')[0]
        return username, email
    else:
        print(f"Unsupported principalId format")
        return None, None


def make_email_body_html(submission, submitter, messages):
    body_html = f"""
    <html>
        <head></head>
        <body>
            <h1>{submission}</h1>
            <p>New AGHA submission rceived from {submitter}</p>
            <p>This is a generated message, please do not reply</p>
            <h2>Quick validation</h2>
            PLACEHOLDER
        </body>
    </html>"""
    insert = ''
    for msg in messages:
        insert += f"{msg}<br>\n"
    body_html = body_html.replace('PLACEHOLDER', insert)
    return body_html


####
# Manifest validation
####
def perform_manifest_validation(manifest_df, submission_prefix, validation_messages):
    # Entry count
    message = f"Entries in manifest: {len(manifest_df)}"
    log_and_store_message(message, validation_messages)
    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_present_s3 = set(get_listing(submission_prefix))
    log_and_store_file_message(message_text, files_present_s3, validation_messages)
    # Files missing from S3
    manifest_files = set(manifest_df['filename'].to_list())
    files_not_on_s3 = manifest_files.difference(files_present_s3)
    message_text = f'Entries in manifest, but not on S3'
    log_and_store_file_message(message_text, files_not_on_s3, validation_messages)
    # Extra files present on S3
    files_not_in_manifeset = files_present_s3.difference(manifest_files)
    message_text = f'Entries on S3, but not in manifest'
    log_and_store_file_message(message_text, files_not_in_manifeset, validation_messages)
    # Files present in manifest *and* S3
    files_in_both = manifest_files.intersection(files_present_s3)
    message_text = f'Entries common in manifest and S3'
    log_and_store_file_message(message_text, files_in_both, validation_messages)
    # Fail if there are extra files (other than manifest.txt) or missing files
    if 'manifest.txt' in files_not_in_manifeset:
        files_not_in_manifeset.remove('manifest.txt')
    messages_error = list()
    if files_not_on_s3:
        messages_error.append('ERROR: files listed in manifest were absent from S3')
    # NOTE(SW): failing on this might be too strict; we may want to re-run validation on some
    # files in-place. Though this would probably be triggered through a different entry point.
    # Strict manifest validation could be appropriate here in that case.
    if files_not_in_manifeset:
        messages_error.append('ERROR: files found on S3 absent from manifest.tsv')
    return not bool(messages_error), messages_error


def manifest_headers_ok(manifest_df, msgs):
    is_ok = True

    if manifest_df is None:
        msgs.append("No manifest to read!")
        return False

    for col_name in MANIFEST_REQUIRED_COLUMNS:
        if col_name not in manifest_df.columns:
            is_ok = False
            msgs.append(f"Column '{col_name}' not found in manifest!")
    return is_ok


####
# Misc
####
def get_listing(prefix: str):
    # get the S3 object listing for the prefix
    files = list()
    file_batch = S3_CLIENT.list_objects_v2(
        Bucket=STAGING_BUCKET,
        Prefix=prefix
    )
    if file_batch.get('Contents'):
        files.extend(extract_filenames(file_batch['Contents']))
    while file_batch['IsTruncated']:
        token = file_batch['NextContinuationToken']
        file_batch = S3_CLIENT.list_objects_v2(
            Bucket=STAGING_BUCKET,
            Prefix=prefix,
            ContinuationToken=token
        )
        files.extend(extract_filenames(file_batch['Contents']))

    return files


def extract_filenames(listing: list):
    filenames = list()
    for item in listing:
        filenames.append(os.path.basename(item['Key']))
    return filenames


####
# Event handling
####
def extract_s3_records_from_sns_event(sns_event):
    # extract the S3 records from the SNS records
    s3_recs = list()
    sns_records = sns_event.get('Records')
    if not sns_records:
        LOGGER.warning("No Records in SNS event! Aborting.")
        LOGGER.warning(json.dumps(sns_event))
        return

    for sns_record in sns_records:
        payload = sns_record['Sns']['Message']
        s3_event = json.loads(payload)
        if not s3_event:
            LOGGER.warning("No S3 event body in SNS Record! Aborting.")
            LOGGER.debug(f"SNS event: {sns_record}")
            continue
        s3_records = s3_event.get('Records')
        if not s3_records:
            LOGGER.warning("No Records in S3 event! Aborting.")
            LOGGER.debug(f"S3 event: {s3_event}")
            continue
        for s3_record in s3_records:
            s3_recs.append(s3_record)

    return s3_recs


def sns_handler(event, context):
    LOGGER.info(f"Start processing S3 (via SNS) event:")
    LOGGER.info(json.dumps(event))

    # we manually build a S3 event, which consists of a list of S3 Records
    s3_event_records = {
        "Records": extract_s3_records_from_sns_event(event)
    }

    handler(s3_event_records, context)


def handler(event, context):
    # Log event data
    LOGGER.info(f"Start processing S3 event:")
    LOGGER.info(json.dumps(event))
    # Ensure we only recieve one manifest record and that it is in the expected bucket
    validation_messages = ['Validation messages:']
    mf_records_n = len(event['Records'])
    if mf_records_n > 1:
        message = f'Expected one manifest record but got {mf_records_n}'
        log_and_store_message(message, validation_messages)
        sys.exit(1)
    [mf_record] = event["Records"]
    bucket_name = mf_record["s3"]["bucket"]["name"]
    if bucket_name != STAGING_BUCKET:
        message = 'Expected {STAGING_BUCKET} bucket but got {bucket_name}'
        log_and_store_message(message, validation_messages)
        sys.exit(1)

    # Pull data from manifest record
    obj_key = mf_record["s3"]["object"]["key"]
    submission_prefix = os.path.dirname(obj_key)
    print(f"Submission with prefix: {submission_prefix}")
    if mf_record.get('eventSource') == 'aws:s3' and mf_record.get('userIdentity'):
        principal_id = mf_record['userIdentity']['principalId']
        name, email = get_name_email_from_principalid(principal_id)
        print(f"Extracted name/email: {name}/{email}")
    else:
        name = 'unknown'
        email = None

    # Get manifest data from S3
    print(f"Getting manifest from : {STAGING_BUCKET}/{submission_prefix}")
    try:
        manifest_s3_key = f'{submission_prefix}/manifest.txt'
        manifest_obj = S3_CLIENT.get_object(Bucket=STAGING_BUCKET, Key=manifest_s3_key)
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_df = pd.read_csv(manifest_str, sep='\t', encoding='utf8')
    except Exception as e:
        message = f'ERROR: could not convert manifest into DataFrame:\n{e}'
        log_and_store_message(message, validation_messages)
        notify_and_exit(validation_messages, name, email, submission_prefix)
    # Check we have expected headers
    if not manifest_headers_ok(manifest_df, validation_messages):
        message = f'ERROR: manifest headers malformed'
        log_and_store_message(message, validation_messages)
        notify_and_exit(validation_messages, name, email, submission_prefix)

    # Validate manifest
    manifest_valid, manifest_error_messages = perform_manifest_validation(
        manifest_df,
        submission_prefix,
        validation_messages
    )
    if not manifest_valid:
        # Notify with failure message(s)
        plurality = 'message' if len(manifest_error_messages) == 0 else 'messages'
        errors = '\n'.join(manifest_error_messages)
        message_base = f'Manifest failed validation with the following {plurality}'
        message = f'{message_base}:\n{errors}'
        log_and_store_message(message, validation_messages)
        notify_and_exit(validation_messages, name, email, submission_prefix)
    else:
        # Notify with success message
        message = f'Manifest succesfully validated, continuing with file validation'
        log_and_store_message(message, validation_messages)
        send_notifications(validation_messages, name, email, submission_prefix)

    # TODO(SW): for each manifest entry create (or grab) dynamodb record, and then submit
    # Batch jobs as required with payload detailing partition key and tasks to run. For entries
    # that fail to submit for whatever reason, collect and send notification
