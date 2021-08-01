import http.client
import logging
import os
import re
import sys


import boto3


# Set logging to info level
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Shared environment variables
# Set as required in Lambda functions with error handling via get_environment_variable()
SLACK_NOTIFY = None
EMAIL_NOTIFY = None
SLACK_HOST = None
SLACK_CHANNEL = None
MANAGER_EMAIL = None
SENDER_EMAIL = None

# AWS clients
# Set as required in Lambda functions with error handling via get_client()
CLIENT_BATCH = None
CLIENT_DYNAMODB = None
CLIENT_IAM = None
CLIENT_S3 = None
CLIENT_SES = None
CLIENT_SSM = None

# Shared SSM variables
# Set below with error handling
SLACK_WEBHOOK_ENDPOINT = None

# Email related
AWS_ID_PATTERN = '[0-9A-Z]{21}'
EMAIL_PATTERN = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f'AWS:({AWS_ID_PATTERN})')
SSO_RE = re.compile(f'AWS:({AWS_ID_PATTERN}):({EMAIL_PATTERN})')


def check_defined(*args):
    # Ensure arguments have been defined otherwise quit
    global_undef = list()
    global_dict = globals()
    for name in args:
        value = global_dict[name]
        if value != None:
            continue
        global_undef.append(name)
    if global_undef:
        # Attempt to get calling function name
        try:
            caller = sys._getframe().f_back.f_code.co_name
        except:
            caller = 'undetermined'
        # Log undefined variables
        names_str = '\r\t'.join(global_undef)
        LOGGER.critical(f'found undefined globals in caller \'{caller}\':\r\t{names_str}')
        sys.exit(1)


def get_environment_variable(name):
    if not (value := os.environ.get(name)):
        LOGGER.critical(f'could not find env variable {name}')
        sys.exit(1)
    return value


def get_client(service_name, region_name=None):
    # Get client
    try:
        client = boto3.client(service_name, region_name=region_name)
    except botocore.exceptions.ClientError as err:
        LOGGER.critical(err)
        sys.exit(1)
    # Check client
    if service_name == 'batch':
        check_batch()
    elif service_name == 'dynamodb':
        check_dynamodb()
    elif service_name == 'iam':
        check_iam()
    elif service_name == 's3':
        check_s3()
    elif service_name == 'ses':
        check_ses()
    else:
        raise NotImplemented(f'requesting client ({service_name}) with unimplemented check')
    return client


def check_batch():
    check_defined('CLIENT_BATCH')
    try:
        CLIENT_BATCH.describe_job_queues()
    except Exception as e:
        LOGGER.critical(f'could access Batch job queues:\r{e}')
        sys.exit(1)


def check_dynamodb():
    try:
        CLIENT_DYNAMODB.describe_table()
    except Exception as e:
        LOGGER.critical(f'could not find DynamoDB table {dynamodb_table}:\r{e}')
        sys.exit(1)


def check_iam():
    check_defined('CLIENT_IAM')
    try:
        CLIENT_IAM.list_users()
    except Exception as e:
        LOGGER.critical(f'could access IAM users:\r{e}')
        sys.exit(1)


def check_s3():
    check_defined('CLIENT_S3')
    try:
        buckets = CLIENT_S3.list_buckets()
    except Exception as e:
        LOGGER.critical(f'could access S3 buckets:\r{e}')
        sys.exit(1)


def check_ses():
    check_defined('CLIENT_SES')
    try:
        CLIENT_SES.list_identities()
    except Exception as e:
        LOGGER.critical(f'could access SES identities:\r{e}')
        sys.exit(1)


def get_ssm_parameter(name, with_decryption=False):
    check_defined('SSM_CLIENT')
    try:
        response = SSM_CLIENT.get_parameter(
            Name=name,
            WithDecryption=with_decryption,
        )
    except SSM.Client.exceptions.ParameterNotFound:
        LOGGER.critical(f'could not find SSM parameter \'{name}\'')
        sys.exit(1)
    if 'Parameter' not in response:
        LOGGER.critical(f'SSM response for \'{name}\' was malformed')
        sys.exit(1)
    return response['Parameter']['Value']


def send_notifications(messages, subject, submitter_name, submitter_email, submission_prefix):
    check_defined('EMAIL_NOTIFY', 'MANAGER_EMAIL', 'SENDER_EMAIL', 'SLACK_NOTIFY', 'SLACK_CHANNEL')
    if EMAIL_NOTIFY == 'yes':
        LOGGER.info(f'Sending notifications messages', end='\r')
        LOGGER.info(*messages, sep='\r')
        LOGGER.info(f'Sending email to {submitter_name} <{submitter_email}>')
        email_response = send_email(
            recipients=[MANAGER_EMAIL, submitter_email],
            sender=SENDER_EMAIL,
            subject_text=subject,
            body_html=make_email_body_html(
                submission=submission_prefix,
                submitter=name,
                messages=messages
            )
        )
    if SLACK_NOTIFY == 'yes':
        LOGGER.info(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = call_slack_webhook(
            topic=subject,
            title=f'Submission: {submission_prefix} ({submitter_name})',
            message='\n'.join(messages)
        )
        LOGGER.info(f'Slack call response: {slack_response}')


def send_email(recipients, sender, subject_text, body_html):
    check_defined('SES_CLIENT')
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
    except botocore.exceptions.ClientError as e:
        LOGGER.error(f'email failed to send: {email_response}')
    else:
        LOGGER.info(f'email sent, message ID: {response["MessageId"]}')


def call_slack_webhook(topic, title, message):
    check_defined('SLACK_HOST', 'SLACK_CHANNEL', 'SLACK_WEBHOOK_ENDPOINT')
    connection = http.client.HTTPSConnection(SLACK_HOST)
    post_data = {
        'channel': SLACK_CHANNEL,
        'username': 'Notice from AWS',
        'text': '*' + topic + '*',
        'icon_emoji': ':aws_logo:',
        'attachments': [{
            'title': title,
            'text': message
        }]
    }
    header = {'Content-Type': 'application/json'}
    connection.request('POST', SLACK_WEBHOOK_ENDPOINT, json.dumps(post_data), header)
    response = connection.getresponse()
    connection.close()
    return response.status


def get_name_email_from_principalid(principal_id):
    check_defined('IAM_CLIENT')
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


def make_email_body_html(submission, submitter, messages):
    body_html = f'''
    <html>
        <head></head>
        <body>
            <h1>{submission}</h1>
            <p>New AGHA submission rceived from {submitter}</p>
            <p>This is a generated message, please do not reply</p>
            <h2>Quick validation</h2>
            PLACEHOLDER
        </body>
    </html>'''
    insert = ''
    for msg in messages:
        insert += f'{msg}<br>\n'
    body_html = body_html.replace('PLACEHOLDER', insert)
    return body_html
