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
MANIFEST_PROCESSOR_LAMBDA_ARN = os.environ.get('MANIFEST_PROCESSOR_LAMBDA_ARN')
FOLDER_LOCK_LAMBDA_ARN = os.environ.get('FOLDER_LOCK_LAMBDA_ARN')
S3_RECORDER_LAMBDA_ARN = os.environ.get('S3_RECORDER_LAMBDA_ARN')

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

    # For file transfer check type
    if event.get('report_type') == 'file_transfer_check':
        """
        Payload expected in this type:
        {
            "submission_prefix": "AC/2022-02-22"
        }
        """

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
    elif event.get('report_type') == 'passed_validation':
        """
        OPTIONAL:
        {
            exception_postfix_filename_list:[]
        }
        """

        # Get a list of submitted file from s3 bucket policy
        s3_client = boto3.client('s3')
        get_bucket_policy_response = s3_client.get_bucket_policy(Bucket=STAGING_BUCKET)
        bucket_policy = json.loads(get_bucket_policy_response['Policy'])
        folder_lock_statement = s3.find_folder_lock_statement(bucket_policy)
        folder_lock_resource = folder_lock_statement.get('Resource')

        # Get exception file list when specified
        exception_postfix_file = event.get('exception_postfix_filename_list')

        # Define result array
        passed_submission_list = []

        for resource in folder_lock_resource:
            # directory prefix for the submission (Stripping resource arn)
            submission_prefix = resource.strip(f'arn:aws:s3:::{STAGING_BUCKET}/').strip('*')

            if submission_prefix.startswith('TEST'):
                continue

            print('submission_prefix: ', submission_prefix)
            fail_status_check_result = batch.run_status_result_check(submission_prefix)

            fail_batch_job = batch.run_batch_check(staging_directory_prefix=submission_prefix,
                                                   exception_list=exception_postfix_file)

            if len(fail_batch_job) < 0 and len(fail_status_check_result) < 0:
                passed_submission_list.append(submission_prefix)

        # print(passed_submission_list)
        return passed_submission_list
