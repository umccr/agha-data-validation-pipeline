import json
import os
import logging

from lambdas.layers.util import util

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


def send_notifications():

    client_lambda = util.get_resource('lambda')

    notification_payload = {
        "messages": MESSAGE_STORE,
        "subject": EMAIL_SUBJECT,
        "submitter_info": SUBMITTER_INFO.__init__
    }

    # Handle notification to another lambda
    client_lambda.invoke(
        FunctionName=NOTIFICATION_LAMBDA_ARN,
        InvocationType='Event',
        Payload=json.dumps(notification_payload)
    )

def initialized_submitter_information(name, email, submission_prefiex):
    SUBMITTER_INFO.name = name
    SUBMITTER_INFO.email = email
    SUBMITTER_INFO.submission_prefix = submission_prefiex

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
        send_notifications(
            MESSAGE_STORE,
            EMAIL_SUBJECT,
            SUBMITTER_INFO,
        )
    raise Exception