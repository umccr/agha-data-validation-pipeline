import http.client
import logging
import os
import sys


import boto3


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def get_environment_variable(name):
    if not (value := os.environ.get(name)):
        LOGGER.critical(f'could not find env variable {name}')
        sys.exit(1)
    return value


def get_client(service_name, region_name=None):
    try:
        response = boto3.client(service_name, region_name=region_name)
    except Exception as err:
        LOGGER.critical(f'could not get AWS client for {service_name}:\r{err}')
        sys.exit(1)


def get_ssm_parameter(name, ssm_client, with_decryption=False):
    try:
        response = ssm_client.get_parameter(
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
        LOGGER.error(f'email failed to send: {response}')
    else:
        LOGGER.info(f'email sent, message ID: {response["MessageId"]}')


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
