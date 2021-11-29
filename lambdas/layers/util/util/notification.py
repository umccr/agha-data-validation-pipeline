import json
import os
import logging
import re

import util
# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# User information class
class SubmitterInfo:

    def __init__(self):
        self.name = str()
        self.email = str()
        self.submission_prefix = str()

# Some constants for the notification
NOTIFICATION_LAMBDA_ARN = os.environ.get('NOTIFICATION_LAMBDA_ARN')
EMAIL_SUBJECT = '[AGHA service] Submission received'
MESSAGE_STORE = list()
SUBMITTER_INFO = SubmitterInfo()

CLIENT_IAM = util.get_client('iam')

def append_message(message):
    MESSAGE_STORE.append(message)

def send_notifications():

    client_lambda = util.get_client('lambda')

    notification_payload = {
        "messages": MESSAGE_STORE,
        "subject": EMAIL_SUBJECT,
        "submitter_info": SUBMITTER_INFO.__dict__
    }

    # Handle notification to another lambda
    try:
        client_lambda.invoke(
            FunctionName=NOTIFICATION_LAMBDA_ARN,
            InvocationType='Event',
            Payload=json.dumps(notification_payload)
        )
    except Exception as e:
        logger.error(f'Something went wrong when calling notification Lambda.\n Error: {e}')


def initialized_submitter_information(name="", email="", submission_prefix=""):
    SUBMITTER_INFO.name = name
    SUBMITTER_INFO.email = email
    SUBMITTER_INFO.submission_prefix = submission_prefix

def log_and_store_file_message(message_text, files):
    # Notification only gets summary message; Lambda log gets both summary and full
    message_summary = f'{message_text}: {len(files)}'
    log_and_store_message(message_summary)
    if files:
        files_str = '\r\t'.join(files)
        logger.info(f'{message_text}:\r\t{files_str}')


def log_and_store_message(message, level='info'):
    level_number = logging.getLevelName(level.upper())
    logger.log(level_number, message)
    # Prefix message with 'ERROR' for display in notifications
    if level in {'error', 'critical'}:
        message = f'ERROR: {message}'
    MESSAGE_STORE.append(message)


def notify_and_exit():
    if SUBMITTER_INFO:
        send_notifications()
    raise Exception


########################################################################################################################
# User account information
# Email/name regular expressions
AWS_ID_RE = '[0-9A-Z]{21}'
EMAIL_RE = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f'AWS:({AWS_ID_RE})')
SSO_RE = re.compile(f'AWS:({AWS_ID_RE}):({EMAIL_RE})')


def get_name_email_from_principalid(principal_id):
    if USER_RE.fullmatch(principal_id):
        user_id = re.search(USER_RE, principal_id).group(1)
        user_list = CLIENT_IAM.list_users()
        for user in user_list['Users']:
            if user['UserId'] == user_id:
                username = user['UserName']
        user_details = CLIENT_IAM.get_user(UserName=username)
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
        logger.warning(f'Could not extract name and email: unsupported principalId format')
        return None, None

def set_submitter_information_from_s3_event(event_record):

    if 'userIdentity' in event_record and 'principalId' in event_record['userIdentity']:
        principal_id = event_record['userIdentity']['principalId']
        SUBMITTER_INFO.name, SUBMITTER_INFO.email = get_name_email_from_principalid(principal_id)
        logger.info(f'Extracted name and email from record: {SUBMITTER_INFO.name} <{SUBMITTER_INFO.email}>')
    else:
        logger.warning(f'Could not extract name and email: unsuitable event type/data')