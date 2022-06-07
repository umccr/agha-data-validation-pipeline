#!/usr/bin/env python3
import json
import logging
import os

import boto3
import http.client

import util
from util import s3, batch, dynamodb

SLACK_HOST = 'hooks.slack.com'
SLACK_CHANNEL = '#agha-gdr'

# Environment Variable
S3_MOVE_JOB_DEFINITION_ARN = os.environ.get('S3_MOVE_JOB_DEFINITION_ARN')
VALIDATE_FILE_JOB_DEFINITION_ARN = os.environ.get('VALIDATE_FILE_JOB_DEFINITION_ARN')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')
REPORT_LAMBDA_ARN = os.environ.get('REPORT_LAMBDA_ARN')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
S3_CLIENT = boto3.client('s3')
CLIENT_SSM = util.get_client('ssm')
LAMBDA_CLIENT = boto3.client('lambda')

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
    job_definition_arn = event['detail']['jobDefinition']
    command = event['detail']['container']['command']

    s3_key = ''
    filename = ''

    if job_definition_arn == VALIDATE_FILE_JOB_DEFINITION_ARN:
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

            message = 'The batch job for the following file failed. ' \
                      'This is due to the aws-resource rather than the data content. ' \
                      'The possible cause are termination as part of spot-instance behaviour, ' \
                      'insufficient memory defined for this job or, ' \
                      'other incorrect configuration.' \
                      'Re-running batch-job may resolve this issue.\n' \
                      f'```{s3_key}```\n'

            aws_invoke_cmd = "" \
                             "Command to rerun task: \n" \
                             "```" \
                             f"""aws lambda invoke \\\n""" \
                             f"""\t--function-name agha-gdr-batch-dynamodb_validation_manager_lambda \\\n""" \
                             f"""\t--payload '{{\n""" \
                             f"""\t\tmanifest_fp": "{submission_prefix}/manifest.txt",\n""" \
                             f"""\t\tinclude_fns": [\n""" \
                             f"""\t\t\t{filename}",\n""" \
                             f"""\t\t]\n""" \
                             f"""\t}}' \\\n""" \
                             f"""\tresponse.json --profile=agha\n""" \
                             "``` \n" \
                             "_NOTE: If 'aws --version' is in version 1 (aws-cli/1.X.XX), '--cli-binary-format raw-in-base64-out' flag may not be necessary._"

            logger.info(f'Failed BatchJob: \n{message + aws_invoke_cmd}')
            send_slack_notification(heading=f'Batch Job (`{submission_prefix}`)',
                                    title='Status: FAILED',
                                    message=message + aws_invoke_cmd)
            return

        # Currently, only accept SUCCEED
        elif "SUCCEEDED" in status:
            # Check if all items are all here
            fail_batch_job = batch.run_batch_check(staging_directory_prefix=submission_prefix)
            logger.info(f'Fail batch job list: {json.dumps(fail_batch_job, indent=4)}')

            if len(fail_batch_job) > 0:
                logger.info('Submission incomplete, waiting for more event from the same submission.')
                return

            else:
                # Conclusion is ready to be taken here

                # Run status check
                fail_status_result_key = batch.run_status_result_check(submission_directory=submission_prefix)
                logger.info(f'Fail status data: {json.dumps(fail_status_result_key, indent=4)}')

                if len(fail_status_result_key) > 0:

                    message = "Submission contain fail data checks. Details as follows.\n" \
                              f"```{json.dumps(fail_status_result_key, indent=4)}```"

                    # Bad data is here
                    logger.info(message)
                    send_slack_notification(heading=f'Data Validation Check (`{submission_prefix}`)',
                                            title='Status: FAILED',
                                            message=message)

                else:
                    # Ready to be stored in the bucket
                    split_list = submission_prefix.strip('/').split('/')
                    flagship_code = split_list[0]
                    submission = split_list[1]

                    message = 'Data validation check succeeded for this submission.\n'
                    aws_cmd = "Run the following command to move the submission to store. \n" \
                              "```" \
                              f"""aws lambda invoke \\\n""" \
                              f"""\t--cli-binary-format raw-in-base64-out \\\n""" \
                              f"""\t--function-name agha-gdr-validation-pipeline-data-transfer-manager \\\n""" \
                              f"""\t--payload '{{\n""" \
                              f"""\t\t"flagship_code": "{flagship_code}",\n""" \
                              f"""\t\t"submission": "{submission}",\n""" \
                              f"""\t\t"run_all": true\n""" \
                              f"""\t}}' \\\n""" \
                              f"""\tresponse.json --profile=agha\n""" \
                              "``` \n" \
                              "_NOTE: If 'aws --version' is in version 1 (aws-cli/1.X.XX), '--cli-binary-format raw-in-base64-out' flag may not be necessary._"

                    logger.info(message + aws_cmd)
                    send_slack_notification(heading=f'Data Validation Check (`{submission_prefix}`)',
                                            title='Status: SUCCEEDED',
                                            message=message + aws_cmd)

        else:
            logger.info('Should not be here in the first place')
            return

    elif job_definition_arn == S3_MOVE_JOB_DEFINITION_ARN:
        # FAILED status (Worst case, unlikely to failed here)
        if "FAILED" in status:
            # Should not enter here
            # BatchRule does not suppose to send this event here
            logger.info('Something went wrong, BatchRule should not send this event here.')
            return
        elif "SUCCEEDED" in status:
            # Check all files in the submission are in the store bucket

            # A simple check is to check number of files in the bucket same s the expected dydb manifest
            s3_target = command[-1].strip(f'"s3://{STORE_BUCKET}/')  # Convert to s3Key
            submission_prefix = os.path.dirname(s3_target).strip('/') + '/'  # Making sure trailing slash

            try:

                metadata_store_list = s3.get_s3_object_metadata(bucket_name=STORE_BUCKET,
                                                                directory_prefix=submission_prefix)
                logger.info(
                    f's3-store metadata list: {json.dumps(metadata_store_list, indent=4, cls=util.JsonSerialEncoder)}')

                # Remove manifest.txt
                is_manifest_txt_exist = False
                is_manifest_orig_exist = False
                for s3_metadata in metadata_store_list:
                    key = s3_metadata['Key']

                    if key.endswith('manifest.txt'):
                        is_manifest_txt_exist = True
                    elif key.endswith('manifest.orig'):
                        is_manifest_orig_exist = True

                # If manifest.orig does not exist, move is incomplete waiting for moore files. Terminating ...
                if not is_manifest_orig_exist:
                    logger.info(f'Move is not finished, waiting for more files')
                    return

                # Check number of files exist match with dydb
                if is_manifest_txt_exist and is_manifest_orig_exist:
                    number_file_skipped = 2
                elif is_manifest_txt_exist or is_manifest_orig_exist:
                    number_file_skipped = 1
                else:
                    number_file_skipped = 0

                expected_list = dynamodb.get_batch_item_from_pk_and_sk(
                    table_name=DYNAMODB_STORE_TABLE_NAME,
                    partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                    sort_key_prefix=submission_prefix)
                logger.info(f'Expected list: {json.dumps(expected_list, indent=4, cls=util.JsonSerialEncoder)}')

            except ValueError as e:
                logger.error(f'ERROR: {e}')
                logger.error(f'Should not send this event. S3/Dydb could not lookup data from {submission_prefix}')
                return

            if len(expected_list) != (len(metadata_store_list) - number_file_skipped):
                logger.info(f'Expected{len(expected_list)} \nCurrent: {(len(metadata_store_list) - number_file_skipped)}')
                logger.info('Incomplete files, waiting for more events')
                return

            else:
                # Check store check via report lambda
                store_bucket_check_response = call_report_lambda({
                    "report_type": "store_bucket_check",
                    "payload": {"submission_prefix": submission_prefix}
                })
                store_bucket_check = json.loads(store_bucket_check_response['Payload'].read().decode("utf-8"))
                logger.info(f'Report check from report lambda: {store_bucket_check}')

                if store_bucket_check != 'OK. All file matched with original manifest.':
                    message = 'File in store does *not* contain all files defined in the original manifest. ' \
                              'Please check the submission manually. \n' \
                              f'```{submission_prefix}```\n'
                    logger.info(message)
                    send_slack_notification(heading=f'Data S3 Store (`{submission_prefix}`)',
                                            title='Status: FAILED',
                                            message=message)
                else:
                    message = 'File in store has all files defined in original manifest. '
                    aws_invoke_cmd = "" \
                                     f"Run the following command to remove index and uncompressed file from staging bucket. \n" \
                                     "```" \
                                     f"""aws lambda invoke \\\n""" \
                                     f"""\t--function-name agha-gdr-validation-pipeline-cleanup-manager \\\n""" \
                                     f"""\t--payload '{{\n""" \
                                     f"""\t\t"submission_directory": "{submission_prefix}\n""" \
                                     f"""\t}}' \\\n""" \
                                     f"""\tresponse.json --profile=agha\n""" \
                                     "``` \n" \
                                     "_NOTE: If 'aws --version' is in version 1 (aws-cli/1.X.XX), '--cli-binary-format raw-in-base64-out' flag may not be necessary._"
                    logger.info(message + aws_invoke_cmd)
                    send_slack_notification(heading=f'Data S3 Store (`{submission_prefix}`)',
                                            title='Status: SUCCEEDED',
                                            message=message + aws_invoke_cmd)


def call_report_lambda(payload: dict):
    response = LAMBDA_CLIENT.invoke(
        FunctionName=REPORT_LAMBDA_ARN,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    return response


def send_slack_notification(heading: str, title: str, message: str):
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
