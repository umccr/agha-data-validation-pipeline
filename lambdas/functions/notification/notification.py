#!/usr/bin/env python3
import logging
import http.client
import json

import botocore.exceptions

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
    logger.info(f'Event processed:')
    print(event)

    messages = event["messages"]
    subject = event["subject"]
    submitter_info = event["submitter_info"]

    if EMAIL_NOTIFY == 'yes':
        logger.info(f'Sending email to {submitter_info["name"]} <{submitter_info["email"]}>')
        logger.info('\r'.join(messages))
        recipients = [MANAGER_EMAIL, submitter_info["email"]]
        email_body = make_email_body_html(
            submitter_info["submission_prefix"],
            submitter_info["name"],
            messages
        )
        email_response = send_email(
            recipients,
            SENDER_EMAIL,
            EMAIL_SUBJECT,
            email_body,
            CLIENT_SES,
        )

        logger.info('Email response:')
        logger.info(json.dumps(email_response, cls=util.DecimalEncoder))

    if SLACK_NOTIFY == 'yes':
        logger.info(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = call_slack_webhook(
            subject,
            f'Submission: {submitter_info["submission_prefix"]} ({submitter_info["name"]})',
            '\n'.join(messages),
            SLACK_HOST,
            SLACK_CHANNEL,
            SLACK_WEBHOOK_ENDPOINT
        )
        logger.info(f'Slack call response: {slack_response}')


def send_email(recipients, sender, subject_text, body_html, ses_client):
    try:
        response = ses_client.send_email(
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
    except botocore.exceptions.ClientError as e:
        logger.error(f'email failed to send: {e}')
        return e
    else:
        logger.info(f'email sent, message ID: {response["MessageId"]}')
        return response


def call_slack_webhook(topic, title, message, slack_host, slack_channel, slack_webhook_endpoint):
    connection = http.client.HTTPSConnection(slack_host)
    post_data = {
        'channel': slack_channel,
        'username': 'Notice from AWS',
        'text': '*' + topic + '*',
        'icon_emoji': ':aws_logo:',
        'attachments': [{
            'title': title,
            'text': message
        }]
    }
    header = {'Content-Type': 'application/json'}
    connection.request('POST', slack_webhook_endpoint, json.dumps(post_data), header)
    response = connection.getresponse()
    connection.close()
    return response.status


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
