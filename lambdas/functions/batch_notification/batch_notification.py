#!/usr/bin/env python3
import json
import logging
import os
import sys
import boto3
import http.client
import enum

import util
from util import batch, dynamodb

SLACK_HOST = "hooks.slack.com"
SLACK_CHANNEL = "#agha-gdr"

# Environment Variable
S3_MOVE_JOB_DEFINITION_ARN = os.environ.get("S3_MOVE_JOB_DEFINITION_ARN")
VALIDATE_FILE_JOB_DEFINITION_ARN = os.environ.get("VALIDATE_FILE_JOB_DEFINITION_ARN")
STORE_BUCKET = os.environ.get("STORE_BUCKET")
DYNAMODB_STORE_TABLE_NAME = os.environ.get("DYNAMODB_STORE_TABLE_NAME")
REPORT_LAMBDA_ARN = os.environ.get("REPORT_LAMBDA_ARN")
DATA_TRANSFER_MANAGER_LAMBDA_ARN = os.environ.get("DATA_TRANSFER_MANAGER_LAMBDA_ARN")
CLEANUP_MANAGER_LAMBDA_ARN = os.environ.get("CLEANUP_MANAGER_LAMBDA_ARN")
DYNAMODB_STAGING_TABLE_NAME = os.environ.get("DYNAMODB_STAGING_TABLE_NAME")
# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")
CLIENT_SSM = util.get_client("ssm")
LAMBDA_CLIENT = boto3.client("lambda")


class EventType(enum.Enum):
    VALIDATION_RESULT_UPLOAD = "VALIDATION_RESULT_UPLOAD"
    STORE_FILE_UPLOAD = "STORE_FILE_UPLOAD"


def handler(event, context):
    """
    This function will check and notify if check has been done and the result of it.

    event payload expected:
    {
        event_type: 'VALIDATION_RESULT',  # OR FILE_UPLOAD
        s3_key : 'ABCDE/121212/filename.fastq.gz'
    }
    """

    logger.info("Processing event:")
    logger.info(json.dumps(event, indent=4))

    event_type = event["event_type"]
    s3_key = event["s3_key"]

    submission_prefix = (
        os.path.dirname(s3_key).strip("/") + "/"
    )  # Making sure trailing slash

    if event_type == EventType.VALIDATION_RESULT_UPLOAD.value:
        # Check if all items are all here
        fail_batch_job = batch.run_batch_check(
            staging_directory_prefix=submission_prefix
        )
        logger.info(f"Fail batch job list: {json.dumps(fail_batch_job, indent=4)}")

        if len(fail_batch_job) > 0:
            logger.info(
                "Submission incomplete, waiting for more event from the same submission."
            )
            return

        else:
            # Conclusion is ready to be taken here
            # Run status check
            fail_status_result_key = batch.run_status_result_check(
                submission_directory=submission_prefix
            )
            logger.info(
                f"Fail status data: {json.dumps(fail_status_result_key, indent=4)}"
            )

            if len(fail_status_result_key) > 0:
                message = (
                    "Submission contain fail data checks. Details as follows.\n"
                    f"```{json.dumps(fail_status_result_key, indent=4)}```"
                )

                # Bad data is here
                logger.info(message)
                send_slack_notification(
                    heading=f"Data Validation Check (`{submission_prefix}`)",
                    title="Status: FAILED",
                    message=message,
                )
                return

            else:
                # Ready to be stored in the bucket
                split_list = submission_prefix.strip("/").split("/")
                flagship_code = split_list[0]
                submission = split_list[1]

                message = "Data validation check succeeded for this submission. Moving this to store bucket.\n"

                logger.info(message)

                # Will ignore notification to slack and just proceed with moving the data
                # send_slack_notification(
                #     heading=f"Data Validation Check (`{submission_prefix}`)",
                #     title="Status: SUCCEEDED",
                #     message=message,
                # )

                # Invoking lambda automatically
                util.call_lambda(
                    lambda_arn=DATA_TRANSFER_MANAGER_LAMBDA_ARN,
                    payload={
                        "flagship_code": flagship_code,
                        "submission": submission,
                        "run_all": True,
                    },
                )
                return

    elif event_type == EventType.STORE_FILE_UPLOAD.value:
        # Check number of file same as number of manifest

        manifest_file_records = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STORE_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
            sort_key_prefix=submission_prefix,
        )
        logger.info(
            f"Total number of {dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value}: {len(manifest_file_records)}"
        )

        current_file_records = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STORE_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
            sort_key_prefix=submission_prefix,
        )
        logger.info(
            f"Total number of {dynamodb.FileRecordPartitionKey.FILE_RECORD.value}: {len(current_file_records)}"
        )

        # Remove manifest.txt
        is_manifest_txt_exist = False
        is_manifest_orig_exist = False
        for s3_metadata in current_file_records:
            key = s3_metadata["sort_key"]

            if key.endswith("manifest.txt"):
                is_manifest_txt_exist = True
            elif key.endswith("manifest.orig"):
                is_manifest_orig_exist = True

        # If manifest.orig does not exist, move is incomplete waiting for moore files. Terminating ...
        if not is_manifest_orig_exist:
            logger.info(
                f"Move is not finished, waiting for more files (manifest.orig does not exist)"
            )
            return

        # Check number of files exist match with dydb
        if is_manifest_txt_exist and is_manifest_orig_exist:
            number_file_skipped = 2
        elif is_manifest_txt_exist or is_manifest_orig_exist:
            number_file_skipped = 1
        else:
            number_file_skipped = 0
        logger.info(f"Number of manifest files: {number_file_skipped}")

        # Check if number of files match
        # Manifest record is the expected number,
        if len(manifest_file_records) != (
            len(current_file_records) - number_file_skipped
        ):
            logger.info(
                "Total number of expected and current files does not match. Terminating ..."
            )
            return
        else:
            logger.info(
                "Total number of expected and current files match exclude manifest."
            )
            missing_store_file = batch.run_manifest_orig_store_check(submission_prefix)
            logger.info(
                f"Missing store file: {json.dumps(missing_store_file, indent=4)}"
            )
            if len(missing_store_file) > 0:
                message = (
                    "File in store does *not* contain all files defined in the original manifest. "
                    "Please check the submission manually. \n"
                    f"```{submission_prefix}```\n"
                )
                logger.info(message)
                send_slack_notification(
                    heading=f"Data S3 Store (`{submission_prefix}`)",
                    title="Status: FAILED",
                    message=message,
                )
                return
            else:

                # Checking if staging bucket have a readme docs
                # If exist means that it had been cleaned up before
                file_to_check = "README.txt"
                readme_dydb = dynamodb.get_item_from_exact_pk_and_sk(
                    DYNAMODB_STAGING_TABLE_NAME,
                    partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
                    sort_key=f"{submission_prefix}{file_to_check}",
                )

                if readme_dydb["Count"] > 0:
                    message = "CleanUp has been done for this submission."
                    logger.info(message)
                    return

                message = "Files in this submission are successfully stored."
                send_slack_notification(
                    heading=f"Data S3 Store (`{submission_prefix}`)",
                    title="Status: SUCCEEDED",
                    message=message,
                )

                # Invoking lambda automatically
                util.call_lambda(
                    lambda_arn=CLEANUP_MANAGER_LAMBDA_ARN,
                    payload={
                        "submission_directory": submission_prefix,
                    },
                )
                return


def send_slack_notification(heading: str, title: str, message: str):
    # Get SSM value
    slack_webhook_endpoint = util.get_ssm_parameter(
        "/slack/webhook/endpoint", CLIENT_SSM, with_decryption=True
    )

    connection = http.client.HTTPSConnection(SLACK_HOST)
    post_data = {
        "channel": SLACK_CHANNEL,
        "username": "AWS-AGHA",
        "text": "*" + heading + "*",  # After username line
        "icon_emoji": ":aws_logo:",
        "attachments": [{"title": title, "text": message}],  # First message line
    }
    header = {"Content-Type": "application/json"}
    connection.request("POST", slack_webhook_endpoint, json.dumps(post_data), header)
    response = connection.getresponse()
    connection.close()
    return response.status
