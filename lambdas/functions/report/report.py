# Possible Use case
# 1. Check if data transfer has completed successfully
# 2. Check if batch job failing
# 3. Check if pipeline result failing
# 4. Report on ready to transfer file

import os
import logging
import json
import boto3

from util import agha, s3, batch

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')


def handler(event, context):
    """
    Purpose of the lambda will report data/metadata inside the pipeline
    event payload: {
        "report_type": "file_transfer_check",
        "payload": { ... }
    }
    * Each payload might be different depending on check_type selected.

    """

    ################################################################################
    # For file transfer check type
    ################################################################################
    if event.get('report_type') == 'file_transfer_check':
        """
        Payload expected in this type:
        {
            "submission_prefix": "AC/2022-02-22"
        }
        """
        logger.info('Checking for file_transfer_check')

        payload = FileTransferPayload()

        event_payload = event.get('payload')
        if event_payload is not None:
            payload.set_payload(**event_payload)

        if payload.submission_prefix is None:
            logger.error('Invalid payload')
            return "Invalid Payload"

        # Get all files value in staging bucket
        try:
            file_list = s3.get_s3_object_metadata(STAGING_BUCKET, payload.submission_prefix)

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

    ################################################################################
    # Checktype for ready to transfer
    ################################################################################
    elif event.get('report_type') == 'passed_validation':
        """
        OPTIONAL:
        {
            exception_postfix_filename_list:[]
        }
        """
        logger.info('Checking for passed validation')

        # Parse payload
        payload = event.get('payload')
        check_payload = PassedValidationPayload()
        if payload is not None:
            check_payload.set_payload(**payload)

        # Get submission directory in staging/store bucket
        staging_submission_directory = []
        store_submission_directory = []
        for flagship_code in agha.FlagShip.list_flagship_enum():
            query_directory = flagship_code + '/'

            staging_submission_directory.extend(s3.aws_s3_ls(STAGING_BUCKET, query_directory))
            store_submission_directory.extend(s3.aws_s3_ls(STORE_BUCKET, query_directory))

        # Remove keys that had already been submitted to the store bucket
        s3_to_check = list(set(staging_submission_directory) - set(store_submission_directory))

        # Define result array
        passed_submission_list = []

        logger.info(f'S3 key to check: {json.dumps(s3_to_check, indent=4)}')

        for submission_prefix in s3_to_check:
            # directory prefix for the submission (Stripping resource arn)

            if submission_prefix.startswith('TEST'):
                continue

            fail_status_check_result = batch.run_status_result_check(submission_prefix)

            fail_batch_job = batch.run_batch_check(staging_directory_prefix=submission_prefix,
                                                   exception_list=check_payload.exception_postfix_file)

            if len(fail_batch_job) < 0 and len(fail_status_check_result) < 0:
                passed_submission_list.append(submission_prefix)

        logger.info(f'Ready to transfer list: {passed_submission_list}')
        return {
            "ready_to_transfer_s3_key": passed_submission_list
        }


class FileTransferPayload:

    def __init__(self):
        self.submission_prefix = None

    def set_payload(self, submission_prefix=None):
        self.submission_prefix = submission_prefix


class PassedValidationPayload:

    def __init__(self):
        self.exception_postfix_file = None

    def set_payload(self, exception_postfix_filename_list=None):
        self.exception_postfix_file = exception_postfix_filename_list
