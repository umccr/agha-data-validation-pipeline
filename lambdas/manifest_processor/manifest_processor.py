#!/usr/bin/env python3

# NOTE(SW): since we're no longer aiming for full automation, it is probably a better idea just to
# completely decouple from dynamodb and only interact with it during result ingestion, which will
# occur in a specific lambda triggered by S3 events or Batch events (captured through CloudWatch).


import io
import json
import logging
import os
import re
import sys
import textwrap


import boto3
import botocore.exceptions
import pandas as pd


import shared


# Logging and message store
LOGGER = shared.LOGGER
MESSAGE_STORE = list()

# Get environment variables
# Lambda-specific
STAGING_BUCKET = shared.get_environment_variable('STAGING_BUCKET')
DYNAMODB_TABLE = shared.get_environment_variable('DYNAMODB_TABLE')
BATCH_QUEUE_NAME = shared.get_environment_variable('BATCH_QUEUE_NAME')
JOB_DEFINITION_NAME = shared.get_environment_variable('JOB_DEFINITION_NAME')
# Shared
SLACK_NOTIFY = shared.get_environment_variable('SLACK_NOTIFY')
EMAIL_NOTIFY = shared.get_environment_variable('EMAIL_NOTIFY')
SLACK_HOST = shared.get_environment_variable('SLACK_HOST')
SLACK_CHANNEL = shared.get_environment_variable('SLACK_CHANNEL')
MANAGER_EMAIL = shared.get_environment_variable('MANAGER_EMAIL')
SENDER_EMAIL = shared.get_environment_variable('SENDER_EMAIL')

# Get AWS clients, resources
CLIENT_BATCH = shared.get_client('batch')
CLIENT_IAM = shared.get_client('iam')
CLIENT_S3 = shared.get_client('s3')
CLIENT_SES = shared.get_client('ses')
CLIENT_SSM = shared.get_client('ssm')
RESOURCE_DYNAMODB = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)

# Get SSM value
SLACK_WEBHOOK_ENDPOINT = shared.get_ssm_parameter(
    '/slack/webhook/endpoint',
    CLIENT_SSM,
    with_decryption=True
)

# Email/name regular expressions
AWS_ID_RE = '[0-9A-Z]{21}'
EMAIL_RE = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f'AWS:({AWS_ID_RE})')
SSO_RE = re.compile(f'AWS:({AWS_ID_RE}):({EMAIL_RE})')

# Manifest field validation related
AGHA_ID_RE = re.compile('^A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?$|^unknown$')
MD5_RE = re.compile('^[0-9a-f]{32}$')
FLAGSHIPS = {
    'ACG',
    'BM',
    'CARDIAC',
    'CHW',
    'EE',
    'GI',
    'HIDDEN',
    'ICCON',
    'ID',
    'KidGen',
    'LD',
    'MCD',
    'MITO',
    'NMD'
}

# Other
EMAIL_SUBJECT = '[AGHA service] Submission received'
MANIFEST_REQUIRED_COLUMNS = {'filename', 'checksum', 'agha_study_id'}


# Collection of input/submission data
class SubmissionData:

    def __init__(self, record):
        self.record = record

        self.submitter_name = str()
        self.submitter_email = str()
        self.flagship = str()

        self.submission_prefix = str()
        self.bucket_name = str()
        self.manifest_key = str()

        self.manifest_data = pd.DataFrame()
        self.manifest_files = list()
        self.extra_files = list()


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(shared.get_context_info(context))}')

    # Parse event data and get record
    record = process_event_data(event)
    data = SubmissionData(record)

    # Get name and email from record
    if record.get('eventSource') == 'aws:s3' and 'userIdentity' in record and 'principalId' in record['userIdentity']:
        principal_id = record['userIdentity']['principalId']
        data.submitter_name, data.submitter_email = get_name_email_from_principalid(principal_id)
        LOGGER.info(f'Extracted name and email from record: {data.submitter_name} <{data.submitter_email}>')
    else:
        LOGGER.warning(f'Could not extract name and email: unsuitable event type/data')

    # Get manifest submission S3 key prefix
    data.bucket_name = record['s3']['bucket']['name']
    data.manifest_key = record['s3']['object']['key']
    data.submission_prefix = os.path.dirname(data.manifest_key)
    LOGGER.info(f'Submission with prefix: {data.submission_prefix}')

    # Obtain flagship from S3 key and require it to be a known value
    data.flagship, *others = data.manifest_key.split('/')
    if data.flagship not in FLAGSHIPS:
        flagships_str = '\r\t'.join(FLAGSHIPS)
        message = f'got unrecognised flagship \'{data.flagship}\', expected one from:\r\t{flagships_str}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)

    # NOTE(SW): here validation for upload subdirectory will be performed, requiring:
    #   - s3://<bucket>/<flagship>/<date>_<n>/{manifest.txt,*bam,*vcf.gz,etc}
    # where <date>_<n> is the upload subdirectory with format YYYYMMDD and where <n> is an
    # incremental counter to avoid colisions for multiple same-day deposits.

    # Collect manifest data and then validate
    data.manifest_data = retrieve_manifest_data(data)
    data.manifest_files, data.extra_files = validate_manifest(data)

    # Process each record and prepare Batch commands

    # NOTE(SW): Batch job submission is not done within the loop to avoid a sitation where some job
    # submissions may succeed and then one fails, causing the remainingi jobs to be not processed
    # at all. The current strategy is essentially 'all or nothing'.

    # NOTE(SW): this logic ignores files that are on S3 but absent from manifest

    batch_job_data = list()
    for manifest_file in data.manifest_files:
        job_data = process_manifest_entry(manifest_file, data)
        batch_job_data.append(job_data)

    # Submit Batch jobs
    for job_data in batch_job_data:
        command = ['bash', '-o', 'pipefail', '-c', job_data['command']]
        environment = [
            {'name': 'RESULTS_S3_BUCKET', 'value': STAGING_BUCKET},
            {'name': 'DYNAMODB_TABLE', 'value': DYNAMODB_TABLE},
        ]
        response = CLIENT_BATCH.submit_job(
            jobName=job_data['name'],
            jobQueue=BATCH_QUEUE_NAME,
            jobDefinition=JOB_DEFINITION_NAME,
            containerOverrides={
                'memory': 4000,
                'environment': environment,
                'command': command
            }
        )


def process_event_data(event):
    if 'Records' not in event:
        LOGGER.critical('no \'s3\' entry found in record')
        sys.exit(1)

    records_n = len(event['Records'])
    if records_n > 1:
        LOGGER.critical(f'Expected one manifest record but got {records_n}')
        sys.exit(1)
    [record] = event['Records']

    if 's3' not in record:
        LOGGER.critical('no \'s3\' entry found in record')
        sys.exit(1)
    record_s3 = record['s3']

    if 'bucket' not in record_s3:
        LOGGER.critical('S3 record missing bucket info')
        sys.exit(1)
    elif 'name' not in record_s3['bucket']:
        LOGGER.critical('S3 bucket record missing name info')
        sys.exit(1)

    if 'object' not in record_s3:
        LOGGER.critical('S3 record missing object info')
        sys.exit(1)
    elif 'key' not in record_s3['object']:
        LOGGER.critical('S3 object record missing key info')
        sys.exit(1)

    if record_s3['bucket']['name'] != STAGING_BUCKET:
        LOGGER.critical(f'expected {STAGING_BUCKET} bucket but got {record_s3["bucket"]["name"]}')
        sys.exit(1)
    return record


def retrieve_manifest_data(data):
    LOGGER.info(f'Getting manifest from: {data.bucket_name}/{data.manifest_key}')
    try:
        manifest_obj = CLIENT_S3.get_object(Bucket=data.bucket_name, Key=data.manifest_key)
    except botocore.exceptions.ClientError as e:
        message = f'could not retrieve manifest data from S3:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    try:
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_data = pd.read_csv(manifest_str, sep='\t', encoding='utf8')
    except Exception as e:
        message = f'could not convert manifest into DataFrame:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    return manifest_data


def validate_manifest(data):
    # Head validation
    columns_present = set(data.manifest_data.columns.tolist())
    columns_missing = MANIFEST_REQUIRED_COLUMNS.difference(columns_present)
    if columns_missing:
        plurality = 'column' if len(columns_missing) == 1 else 'columns'
        cmissing_str = '\r\t'.join(columns_missing)
        cfound_str = '\r\t'.join(columns_present)
        message_base = f'required {plurality} missing from manifest:'
        log_and_store_message(f'{message_base}\r\t{cmissing_str}\rGot:\r\t{cfound_str}', level='critical')
        notify_and_exit(data)

    # File discovery
    # Entry count
    log_and_store_message(f'Entries in manifest: {len(data.manifest_data)}')
    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_s3 = set(get_s3_filenames(data.bucket_name, data.submission_prefix))
    log_and_store_file_message(message_text, files_s3)
    # Files missing from S3
    files_manifest = set(data.manifest_data['filename'].to_list())
    files_missing_from_s3 = files_manifest.difference(files_s3)
    message_text = f'Entries in manifest, but not on S3'
    log_and_store_file_message(message_text, files_missing_from_s3)
    # Extra files present on S3
    files_missing_from_manifest = files_s3.difference(files_manifest)
    message_text = f'Entries on S3, but not in manifest'
    log_and_store_file_message(message_text, files_missing_from_manifest)
    # Files present in manifest *and* S3
    files_matched = files_manifest.intersection(files_s3)
    message_text = f'Entries common in manifest and S3'
    log_and_store_file_message(message_text, files_matched)
    # Fail if there are extra files (other than manifest.txt) or missing files
    if 'manifest.txt' in files_missing_from_manifest:
        files_missing_from_manifest.remove('manifest.txt')
    messages_error = list()
    if files_missing_from_s3:
        messages_error.append('files listed in manifest were absent from S3')
    # NOTE(SW): failing on this might be too strict; we may want to re-run validation on some
    # files in-place. Though this would probably be triggered through a different entry point.
    # Strict manifest validation could be appropriate here in that case.
    if files_missing_from_manifest:
        messages_error.append('files found on S3 absent from manifest.tsv')

    # Field validation
    for row in data.manifest_data.itertuples():
        # Study ID
        if not AGHA_ID_RE.match(row.agha_study_id):
            message = f'got malformed AGHA study ID for {row.Index} ({row.agha_study_id})'
            messages_error.append(message)
        # Checksum
        if not MD5_RE.match(row.checksum):
            message = f'got malformed MD5 checksum for {row.Index} ({row.checksum})'
            messages_error.append(message)

    # Check for errors
    if messages_error:
        plurality = 'message' if len(messages_error) == 1 else 'messages'
        errors = '\r\t'.join(messages_error)
        message_base = f'Manifest failed validation with the following {plurality}'
        message = f'{message_base}:\r\t{errors}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)

    # Notify with success message
    message = f'Manifest succesfully validated, continuing with file validation'
    log_and_store_message(message)
    send_notifications(
        MESSAGE_STORE,
        EMAIL_SUBJECT,
        data.submitter_name,
        data.submitter_email,
        data.submission_prefix,
    )
    # NOTE(SW): returning files that are on S3 and (1) in manifest, and (2) not in manifest. This
    # will allow flexbility in the future for any refactor if we decide to modify handling of
    # missing files
    return list(files_matched), list(files_missing_from_manifest)


def process_manifest_entry(filename, data):
    # NOTE(SW): it is not clear whether study id and filename is sufficiently unique. May need to
    # also include submission subdir (i.e. the date of upload). The answer should become apparent
    # when/after testing real data.

    # Grab file info, and construct partition key
    file_info = data.manifest_data.loc[data.manifest_data['filename']==filename].iloc[0]
    study_id = file_info['agha_study_id']
    key_partition = f'{study_id}_{filename}'

    # Get sort key and create record
    # NOTE(SW): currently creating new record if previous one exists. Could modify this behaviour
    # here to:
    #   - ignore and overwrite
    #   - set new keys, create new record
    #   - keep record and recompute only missing values
    #   - keep record and some specified set of values
    #   - allow user to have some choice
    file_number, records_existing = get_existing_records_and_filenumber(filename, key_partition)
    if records_existing:
        LOGGER.info(f'found existing records for {filename}: {records_existing}')
    key_sort = f'{filename}_{file_number}'
    s3_key = os.path.join(data.submission_prefix, filename)
    create_record(
        key_partition,
        key_sort,
        data.flagship,
        s3_key,
        file_number,
        file_info,
        records_existing
    )

    # Determine required jobs
    # NOTE(SW): given the above behaviour of creating new records if previous exists, we will
    # always need to run all jobs. Hardcode here for now.
    tasks_list = ['checksum', 'validate_filetype', 'create_index']
    tasks = ' '.join(tasks_list)

    # Construct command and job name
    name_raw = f'agha_validation__{key_partition}__{key_sort}'
    name = name_raw.replace('.', '_')
    command = textwrap.dedent(f'''
        validate_file.py \
          --partition_key {key_partition} \
          --sort_key {key_sort} \
          --tasks {tasks}
    ''')
    return {'name': name, 'command': command}


def get_s3_object_metadata(bucket, prefix):
    results = list()
    response = CLIENT_S3.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )
    if not (object_mdata := response.get('Contents')):
        message = f'could not retrieve files from S3 at s3://{bucket}{prefix}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    else:
        results.extend(object_mdata)
    while response['IsTruncated']:
        token = response['NextContinuationToken']
        response = CLIENT_S3.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=token
        )
        results.extend(object_mdata)
    return results


def list_s3_objects(bucket, prefix):
    object_metadata = get_s3_object_metadata(bucket, prefix)
    return [os.path.basename(md.get('Key')) for md in object_metadata]


def get_s3_filenames(bucket, prefix):
    filepaths = list_s3_objects(bucket, prefix)
    return [os.path.basename(fp) for fp in filepaths]


def get_existing_records_and_filenumber(filename, key_partition):
    records = get_records(key_partition)
    if records:
        records_current = [r for r in records if r['active']]
        assert len(records_current) == 1
        [record_current] = records_current
        file_number = record_current['file_number'] + 1
    else:
        file_number = 1
    return file_number, records


def get_records(partition_key):
    records = list()
    response = RESOURCE_DYNAMODB.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('partition_key').eq(partition_key)
    )
    if 'Items' not in response:
        message_key = f'partition key ({partition_key}) and sort key ({sort_key}) in {DYNAMODB_TABLE}'
        message = f'could not any records using {message_key}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = DYNAMODB_TABLE.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('partition_key').eq(partition_key),
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    return records


def create_record(
    key_partition,
    key_sort,
    flagship,
    s3_key,
    file_number,
    file_info,
    records_existing
):
    # Set old records to inactive
    for record in records_existing:
        response = RESOURCE_DYNAMODB.update_item(
            Key={
                'partition_key': record['partition_key'],
                'sort_key': record['sort_key'],
            },
            UpdateExpression='set active=:a',
            ExpressionAttributeValues={':a': False},
        )
    # Create new record
    record = {
        'partition_key': key_partition,
        'sort_key': key_sort,
        'active': True,
        'study_id': file_info['agha_study_id'],
        'flagship': flagship,
        'filename': file_info['filename'],
        'file_number': file_number,
        's3_bucket': STAGING_BUCKET,
        's3_key': s3_key,
        # Checksum
        'provided_checksum': file_info['checksum'],
        'calculated_checksum': 'not run',
        'validated_checksum': 'not run',
        # File type
        'calculated_filetype': 'not run',
        'validated_filetype': 'not run',
        # Index
        # NOTE(SW): disallowing indices for now
        'has_index': False,
        'index_result': 'not run',
        'index_file_name': 'na',
        'index_s3_bucket': 'na',
        'index_s3_key': 'na',
        # Misc
        'validation_result': 'not determined',
        'excluded': False,
    }
    LOGGER.info(f'created record for {file_info["filename"]}: {record}')
    RESOURCE_DYNAMODB.put_item(Item=record)


def log_and_store_file_message(message_text, files):
    # Notification only gets summary message; Lambda log gets both summary and full
    message_summary = f'{message_text}: {len(files)}'
    log_and_store_message(message_summary)
    if files:
        files_str = '\r\t'.join(files)
        LOGGER.info(f'{message_text}:\r\t{files_str}')


def log_and_store_message(message, level='info'):
    level_number = logging.getLevelName(level.upper())
    LOGGER.log(level_number, message)
    # Prefix message with 'ERROR' for display in notifications
    if level in {'error', 'critical'}:
        message = f'ERROR: {message}'
    MESSAGE_STORE.append(message)


def notify_and_exit(data):
    send_notifications(
        MESSAGE_STORE,
        EMAIL_SUBJECT,
        data.submitter_name,
        data.submitter_email,
        data.submission_prefix,
    )
    sys.exit(1)


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
        LOGGER.warning(f'Could not extract name and email: unsupported principalId format')
        return None, None


def send_notifications(messages, subject, submitter_name, submitter_email, submission_prefix):
    if EMAIL_NOTIFY == 'yes':
        LOGGER.info(f'Sending notifications messages', end='\r')
        LOGGER.info(*messages, sep='\r')
        LOGGER.info(f'Sending email to {submitter_name} <{submitter_email}>')
        recipients = [MANAGER_EMAIL, submitter_email]
        email_body = make_email_body_html(
            submission_prefix,
            submitter_name,
            messages
        )
        email_response = shared.send_email(
            recipients,
            EMAIL_SENDER,
            EMAIL_SUBJECT,
            email_body,
            SES_CLIENT
        )
    if SLACK_NOTIFY == 'yes':
        LOGGER.info(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = shared.call_slack_webhook(
            subject,
            f'Submission: {submission_prefix} ({submitter_name})',
            '\n'.join(messages),
            SLACK_HOST,
            SLACK_CHANNEL,
            SLACK_WEBHOOK_ENDPOINT
        )
        LOGGER.info(f'Slack call response: {slack_response}')
