import datetime
import http.client
import logging
import os
import subprocess
import sys


import boto3


LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


class StreamHandlerNewLine(logging.StreamHandler):
    '''Override emit so that we can use '\n' in file logs'''

    def emit(self, record):
        try:
            msg = self.format(record)
            msg = msg.replace('\r', '\n')
            stream = self.stream
            # issue 35046: merged two stream.writes into one.
            stream.write(msg + self.terminator)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


class FileHandlerNewLine(logging.FileHandler):
    '''Override emit so that we can use '\n' in file logs'''

    def emit(self, record):
        if self.stream is None:
            if self.mode != 'w' or not self._closed:
                self.stream = self._open()
        if self.stream:
            StreamHandlerNewLine.emit(self, record)


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
    return response


def get_resource(service_name, region_name=None):
    try:
        response = boto3.resource(service_name, region_name=region_name)
    except Exception as err:
        LOGGER.critical(f'could not get AWS resource for {service_name}:\r{err}')
        sys.exit(1)
    return response


def get_dynamodb_table_resource(dynamodb_table, region_name=None):
    return get_resource('dynamodb', region_name=region_name).Table(dynamodb_table)


def get_ssm_parameter(name, ssm_client, with_decryption=False):
    try:
        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=with_decryption,
        )
    except ssm_client.Client.exceptions.ParameterNotFound:
        LOGGER.critical(f'could not find SSM parameter \'{name}\'')
        sys.exit(1)
    if 'Parameter' not in response:
        LOGGER.critical(f'SSM response for \'{name}\' was malformed')
        sys.exit(1)
    return response['Parameter']['Value']


def get_context_info(context):
    attributes = {
        'function_name',
        'function_version',
        'invoked_function_arn',
        'memory_limit_in_mb',
        'aws_request_id',
        'log_group_name',
        'log_stream_name',
    }
    return {attr: getattr(context, attr) for attr in attributes}


def get_datetimestamp():
    return f'{get_datestamp()}_{get_timestamp()}'


def get_timestamp():
    return '{:%H%M%S}'.format(datetime.datetime.now())


def get_datestamp():
    return '{:%Y%m%d}'.format(datetime.datetime.now())


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


def execute_command(command):
    process_result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        encoding='utf-8'
    )
    return process_result
