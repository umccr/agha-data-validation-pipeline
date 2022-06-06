#!/usr/bin/env python3
import json
import logging
import os
import sys

import boto3
import http.client

import util
from util import s3, agha, batch

SLACK_HOST = 'hooks.slack.com'
SLACK_CHANNEL = '#agha-gdr'
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_ARN')
JOB_DEFINITION_ARN = os.environ.get('JOB_DEFINITION_ARN')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
S3_CLIENT = boto3.client('s3')
CLIENT_SSM = util.get_client('ssm')

# Get SSM value
SLACK_WEBHOOK_ENDPOINT = util.get_ssm_parameter(
    '/slack/webhook/endpoint',
    CLIENT_SSM,
    with_decryption=True
)


def handler(event, context):
    """
    This function will check
    """

    logger.info("Processing event:")
    logger.info(json.dumps(event, indent=4))

    # Parse Variable
    # Ref: https://docs.aws.amazon.com/batch/latest/userguide/batch_cwe_events.html
    status = event['detail']['status'].upper()
    job_definition = event['detail']['jobDefinition']
    command = event['detail']['container']['command']

    s3_key = ''
    filename = ''

    if JOB_DEFINITION_ARN == job_definition:
        try:
            # Find s3key from command
            for i in range(len(command)):
                if command[i] == '--s3_key':
                    s3_key = command[i + 1]
                    filename = s3.get_s3_filename_from_s3_key(s3_key)
                    break
        except IndexError:
            pass

        submission_prefix = os.path.dirname(s3_key).strip('/') + '/'  # Making sure trailing slash

        # FAILED status (Worst case, unlikely to failed here)
        if "FAILED" in status:
            # This does not mean the content of the file is failing
            # It is the batch job is incomplete may be due to AWS-resources (such as termination, or insufficient memory)

            message = 'The batch job for the following key failed. ' \
                      'This is due to the aws-resource rather than the data content.' \
                      'The possible cause are termination as part of spot-instance behaviour, ' \
                      'insufficient memory defined for this job or, ' \
                      'other incorrect configuration.' \
                      'Re-running batch-job may resolve this issue.\n' \
                      f'```{s3_key}```\n'

            aws_invoke_cmd = "" \
                             "Command to rerun task: \n" \
                             "```" \
                             "aws lambda invoke \\\n" \
                             "\t--function-name agha-gdr-batch-dynamodb_validation_manager_lambda \\\n" \
                             "\t--payload '{{\n" \
                             f"""\t\tmanifest_fp": "{submission_prefix}/manifest.txt",\n""" \
                             """\t\tinclude_fns": [\n""" \
                             f"""\t\t\t{filename}",\n""" \
                             "\t\t]\n" \
                             """\t}}' \\\n""" \
                             "\tresponse.json --profile=agha\n" \
                             "``` \n" \
                             "_NOTE: If 'aws --version' is in version 1 (aws-cli/1.X.XX), '--cli-binary-format raw-in-base64-out' flag may not be necessary._"

            print(aws_invoke_cmd)
            logger.info(f'Failed BatchJob: {message + aws_invoke_cmd}')
            send_slack_notification(heading='Batch Job Result',
                                    title='Fail Batch Job',
                                    message=message + aws_invoke_cmd)
            return

        # Currently, only accept SUCCEED
        elif "SUCCEEDED" in status:
            # Check if all items are all here
            fail_batch_job = batch.run_batch_check(staging_directory_prefix=submission_prefix)

            if len(fail_batch_job) > 0:
                logger.info('Submission incomplete, waiting for more event from the same submission.')
                return

            else:
                # Conclusion is ready to be taken here

                # Run status check
                fail_status_result_key = batch.run_status_result_check(submission_directory=submission_prefix)

                if len(fail_status_result_key) > 0:

                    message = "Submission contain fail data checks. Details as follows.\n" \
                              f"```{json.dumps(fail_status_result_key, indent=4)}```"

                    # Bad data is here
                    logger.info(message)
                    send_slack_notification(heading='Status Result',
                                            title='Fail Data Checks',
                                            message=message)

                else:
                    # Ready to be stored in the bucket
                    split_list = submission_prefix.strip('/').split('/')
                    flagship_code = split_list[0]
                    submission = split_list[1]

                    message = 'Submission ready to be stored in the store-bucket.\n'
                    aws_cmd = "The command to execute move to `agha-gdr-store-2.0`\n" \
                              "```" \
                              "aws lambda invoke \\\n" \
                              "\t--cli-binary-format raw-in-base64-out \\\n" \
                              "\t--function-name agha-gdr-validation-pipeline-data-transfer-manager \\\n" \
                              "\t--payload '{\n" \
                              f"""\t\t"flagship_code": "{flagship_code}",\n""" \
                              f"""\t\t"submission": "{submission}",\n""" \
                              f"""\t\t"run_all": true\n""" \
                              "\t}' \\\n" \
                              "\tresponse.json --profile=agha\n" \
                              "``` \n" \
                              "_NOTE: If 'aws --version' is in version 1 (aws-cli/1.X.XX), '--cli-binary-format raw-in-base64-out' flag may not be necessary._"
                    print(len(aws_cmd))
                    print(message + aws_cmd)
                    send_slack_notification(heading='Status Result', title='Succeed Data Checks', message=message + aws_cmd)

        else:
            logger.info('Should not be here in the first place')
            return


def send_slack_notification(heading: str, title: str, message: str):
    sys.exit(1)

    connection = http.client.HTTPSConnection(SLACK_HOST)
    post_data = {
        'channel': SLACK_CHANNEL,
        'username': 'AWS-AGHA',
        'text': '*' + heading + '*',  # After username line
        'icon_emoji': ':aws_logo:',
        'attachments': [{
            'title': title,  # First message line
            'text': message
        }]
    }
    header = {'Content-Type': 'application/json'}
    connection.request('POST', SLACK_WEBHOOK_ENDPOINT, json.dumps(post_data), header)
    response = connection.getresponse()
    connection.close()
    return response.status
