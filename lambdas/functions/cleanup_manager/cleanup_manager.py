#!/usr/bin/env python3
import json
import logging
import os
import boto3

import util
from util import s3, agha

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
FOLDER_LOCK_LAMBDA_ARN = os.environ.get('FOLDER_LOCK_LAMBDA_ARN')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client('s3')


def handler(event, context):
    """
    This function will delete object from staging bucket.
    Object deleted will ony be index file and uncompressed file.

    {
        "submission_directory": "AC/2022-02-02/"
    }
    """

    submission_directory = event.get('submission_directory')

    if submission_directory is None:
        return 'Invalid payload'

    ################################################################################
    # First stage - Check if content of bucket is ok to delete
    ################################################################################
    # List of all files in the bucket
    try:
        metadata_list = s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET, directory_prefix=submission_directory)
        s3_key_list = [metadata['Key'] for metadata in metadata_list]
    except ValueError:
        logger.error(f'No \'{submission_directory}\' found in staging bucket')
        return {
            "reason": f'No \'{submission_directory}\' found in staging bucket'
        }

    deletion_key_list = []
    for s3_key in s3_key_list:

        # Allowed index and non-compressed but compressible file to be in the deletion list
        if agha.FileType.is_index_file(s3_key) or \
                (agha.FileType.is_compressable_file(s3_key) and not agha.FileType.is_compress_file(s3_key)):
            deletion_key_list.append(s3_key)

    ################################################################################
    # Second Stage - Delete files
    ################################################################################

    # If deletion key_list exist, delete the file
    if len(deletion_key_list) > 0:
        try:
            logger.info(f'Deleting file from staging bucket, List of s3_key: {json.dumps(deletion_key_list, indent=4)} ')
            res = s3.delete_s3_object_from_key(bucket_name=STAGING_BUCKET, key_list=deletion_key_list)
            logger.debug(f'Deletion response {res}')
            logger.info(f'Deletion job success!')

        except Exception as e:
            logger.error('Something went wrong on deleting s3 keys')
            logger.error(e)
            return f'Something went wrong. Error: {e}'
    else:
        logger.info(f'No data to delete, proceeding next stage')

    ################################################################################
    # Third stage - Create Readme to not use this bucket directory again.
    ################################################################################
    logger.info(f'Creating README.txt file')
    readme_key = submission_directory.strip('/') + '/' + 'README.txt'
    file_content = f"Submission in \'{submission_directory}\' has successfully stored in the store bucket.\n" \
                   f"Please check store bucket if you need to access the content.\n" \
                   f"If you need to upload/modify the content, please submit as a new submission.\n" \
                   f"This folder is now locked to prevent overlapping with succeeded submission. \n"

    s3.upload_s3_object_from_string(bucket_name=STAGING_BUCKET,
                                    byte_of_string=file_content,
                                    s3_key_destination=readme_key)
    logger.info('Uploaded README file')

    ################################################################################
    # Fourth stage - Invoke folder lock lambda
    ################################################################################
    logger.info('Locking folder')
    payload = [
        {
            "s3": {
                "bucket": {
                    "name": STAGING_BUCKET
                },
                "object": {
                    "key": readme_key
                }
            }
        }
    ]
    folder_lock_response = util.call_lambda(FOLDER_LOCK_LAMBDA_ARN, {"Records": payload})
    logger.debug(f'Lock lambda response: {folder_lock_response}')
    logger.info(f'Lock lambda has been triggered.')
