# Possible Use case
# 1. Check if data transfer has completed successfully
# 2. Check if batch job failing
# 3. Check if pipeline result failing
# 4. Report on ready to transfer file

import os
import logging
import json
import boto3

from util import agha, s3

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
MANIFEST_PROCESSOR_LAMBDA_ARN = os.environ.get('MANIFEST_PROCESSOR_LAMBDA_ARN')
FOLDER_LOCK_LAMBDA_ARN = os.environ.get('FOLDER_LOCK_LAMBDA_ARN')
S3_RECORDER_LAMBDA_ARN = os.environ.get('S3_RECORDER_LAMBDA_ARN')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')


def handler(event, context):
    """
    For temporary, this will check if data_transfer_manager lambda had successfully transferred file.
    event payload: {
        "check_type": "file_transfer",
        "submission_prefix": "AC/2020_02_02"
    }
    """


    # For file transfer check type
    # Parse event
    submission_prefix = event["submission_prefix"]

    # Get all files value in staging bucket
    try:
        file_list = s3.get_s3_object_metadata(STAGING_BUCKET, submission_prefix)

    except ValueError:

        message = json.dumps({
            "reason": "Something went wrong when looking up the directory. Please check the submission_prefix payload.",
        }, indent=4)

        logger.error(message)
        return message

    # Unexpected file list
    unexpected_file_list = []
    file_to_delete = []
    for metadata in file_list:
        file_key = metadata['Key']

        if agha.FileType.is_index_file(file_key) or \
                (agha.FileType.is_compressable_file(file_key) and not agha.FileType.is_compress_file(file_key)):

            file_to_delete.append(file_key)
            continue

        unexpected_file_list.append(file_key)

    json_report = dict()
    json_report['unexpected_file'] = unexpected_file_list
    json_report['file_to_delete'] = file_to_delete

    logger.info(json.dumps(json.dumps(json_report, indent=4)))

    return json_report


    # Checktype for ready to transfer
