#!/usr/bin/env python3
import logging
import re
import json

import util

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get environment variables
SLACK_NOTIFY = util.get_environment_variable('SLACK_NOTIFY')
EMAIL_NOTIFY = util.get_environment_variable('EMAIL_NOTIFY')
SLACK_HOST = util.get_environment_variable('SLACK_HOST')
SLACK_CHANNEL = util.get_environment_variable('SLACK_CHANNEL')
MANAGER_EMAIL = util.get_environment_variable('MANAGER_EMAIL')
SENDER_EMAIL = util.get_environment_variable('SENDER_EMAIL')

# Get AWS clients, resources
CLIENT_SES = util.get_client('ses')
CLIENT_SSM = util.get_client('ssm')

# Get SSM value
SLACK_WEBHOOK_ENDPOINT = util.get_ssm_parameter(
    '/slack/webhook/endpoint',
    CLIENT_SSM,
    with_decryption=True
)

# Other
EMAIL_SUBJECT = '[AGHA service] Submission received'

def handler(event, context):
    """
    Notification lambda will send notification to email/slack channel.
    The payload will contain submitter information, messasge, and 
    {
        "messages": messages,
        "subject" : subject,
        "submitter_info": submitter_info.__init__
    }

    :param event: S3 event to parse user 
    :param context: not used
    """
    messages = json.loads(event["submitter_info"])
    subject = json.loads(event["subject"])
    submitter_info = json.loads(event["submitter_info"])

    if EMAIL_NOTIFY == 'yes':
        logger.info(f'Sending email to {submitter_info.name} <{submitter_info.email}>')
        logger.info('\r'.join(messages))
        recipients = [MANAGER_EMAIL, submitter_info.email]
        email_body = util.make_email_body_html(
            submitter_info.submission_prefix,
            submitter_info.name,
            messages
        )
        email_response = util.send_email(
            recipients,
            SENDER_EMAIL,
            EMAIL_SUBJECT,
            email_body,
            CLIENT_SES,
        )
    if SLACK_NOTIFY == 'yes':
        logger.info(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = util.call_slack_webhook(
            subject,
            f'Submission: {submitter_info.submission_prefix} ({submitter_info.name})',
            '\n'.join(messages),
            SLACK_HOST,
            SLACK_CHANNEL,
            SLACK_WEBHOOK_ENDPOINT
        )
        logger.info(f'Slack call response: {slack_response}')