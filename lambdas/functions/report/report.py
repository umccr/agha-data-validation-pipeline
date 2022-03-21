# Possible Use case
# 1. Check if data transfer has completed successfully
# 2. Check if batch job failing
# 3. Check if pipeline result failing
# 4. Report on ready to transfer file

import os
import logging
import json
import boto3

from util import agha, s3, batch, submission_data

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
            # All files submission might all be moved to store bucket
            # Checking submission in store bucket
            try:
                store_file_list = s3.get_s3_object_metadata(STORE_BUCKET, payload.submission_prefix)
                if len(store_file_list) > 0:
                    logger.info('All file has been transferred to store bucket')
                    file_list = []
                else:
                    raise ValueError('No data found')
            except ValueError:
                # Something really went wrong
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

    ################################################################################
    # Checktype for ready to transfer
    ################################################################################
    elif event.get('report_type') == 'store_bucket_check':
        """
        {
            "submission_prefix": "AC/2022-02-22"
        }
        """
        logger.info(f'\'store_bucket_check\' option selected.')
        logger.info(f'Processing event: {event}')

        payload = StoreBucketPayload()
        dict_payload = event.get('payload')

        payload.set_payload(**dict_payload)

        manifest_orig_key = payload.submission_prefix + 'manifest.orig'
        logger.debug(f'Manifest key: {manifest_orig_key}')

        manifest_orig_pd = submission_data.retrieve_manifest_data(bucket_name=STORE_BUCKET,
                                                                  manifest_key=manifest_orig_key)

        manifest_orig_filename_list = manifest_orig_pd['filename'].tolist()
        logger.info(f'List of filename in \'manifest.orig\': {json.dumps(manifest_orig_filename_list, indent=4)}')

        # Grab for filename from 'manifest.orig' that should exist in the bucket
        manifest_orig_filename_to_check = []
        for manifest_orig_filename in manifest_orig_filename_list:

            if submission_data.is_file_skipped(manifest_orig_filename, payload.exception_postfix_file):
                continue
            else:
                manifest_orig_filename_to_check.append(manifest_orig_filename)

        # Grab s3_key exist in store bucket
        store_metadata_list = s3.get_s3_object_metadata(bucket_name=STORE_BUCKET,
                                                        directory_prefix=payload.submission_prefix)
        store_filename_list = [metadata['Key'].split('/')[-1] for metadata in store_metadata_list]
        logger.info(f'Filename in store bucket: {json.dumps(store_filename_list, indent=4)}')

        # Comparing files in the manifest.orig but not in the manifest store
        manifest_orig_diff_store = list(set(manifest_orig_filename_list) - set(store_filename_list))
        logger.info(f'Comparing filename exist in original manifest but not in store bucket. '
                    f'Result {json.dumps(manifest_orig_diff_store, indent=4)}')

        # Checking file that could be altered and only the altered file is being stored (e.g. uncompressed file)
        uncompress_file = []
        for filename in manifest_orig_diff_store:
            if agha.FileType.is_compressable_file(filename) and not agha.FileType.is_compress_file(filename) and \
                    f"{filename}.gz" in store_filename_list:
                uncompress_file.append(filename)

        remove_uncompress_file = list(set(manifest_orig_diff_store) - set(uncompress_file))
        len_remove_uncompress_file = len(remove_uncompress_file)
        logger.info(f'Difference after removing uncompress file: {remove_uncompress_file}')
        logger.info(f'Total number of difference: {len_remove_uncompress_file}')

        if len_remove_uncompress_file > 0:
            message = f'Contain missing file. Expected file: {json.dumps(remove_uncompress_file, indent=4)}'
            logger.critical(message)
            return message
        else:
            message = 'OK. All file matched with original manifest.'
            logger.info(message)
            return message


class StoreBucketPayload:
    def __init__(self):
        self.submission_prefix = None
        self.exception_postfix_file = None

    def set_payload(self, submission_prefix=None, exception_postfix_file=None):

        # Mandatory payload
        if submission_prefix is None:
            raise ValueError(f'Incorrect payload')

        # Sanitize
        if not submission_prefix.endswith('/'):
            submission_prefix = submission_prefix + '/'

        self.submission_prefix = submission_prefix
        self.exception_postfix_file = exception_postfix_file


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
