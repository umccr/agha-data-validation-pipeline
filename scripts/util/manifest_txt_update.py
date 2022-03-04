#!/usr/bin/env python3
import os
import argparse
import sys
import logging

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "lambdas", "layers", "util"
)
sys.path.append(SOURCE_PATH)

from util import dynamodb, s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Setting some variables

# Buckets
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
RESULT_BUCKET = os.environ.get('RESULT_BUCKET')

# Dynamodb
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STORE_TABLE_NAME')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_RESULT_TABLE_NAME')
DYNAMODB_ETAG_TABLE_NAME = os.environ.get('DYNAMODB_ETAG_TABLE_NAME')


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--s3_key_object', required=True, type=str,
                        help='s3 key')
    return parser.parse_args()


def update_manifest_file(args):
    # Strip filename to get submission prefix
    submission_prefix = os.path.dirname(args.s3_key_object) + '/'

    # Create new manifest file from dynamodb given
    manifest_item = dynamodb.get_batch_item_from_pk_and_sk(DYNAMODB_STORE_TABLE_NAME,
                                                           dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                           submission_prefix)
    # Define manifest file
    new_manifest_file = 'checksum\tfilename\tagha_study_id\n'  # First line is header

    # Iterate to manifest list
    for item in manifest_item:
        checksum = item['provided_checksum']
        filename = item['filename']
        agha_study_id = item['agha_study_id']

        new_manifest_file += f'{checksum}\t{filename}\t{agha_study_id}\n'
    manifest_destination_key = submission_prefix + 'manifest.txt'
    s3.upload_s3_object_from_string(bucket_name=STORE_BUCKET,
                                    byte_of_string=new_manifest_file,
                                    s3_key_destination=manifest_destination_key)

    logger.info(f'Uploaded dynamodb manifest.txt')


if __name__ == '__main__':
    args = get_arguments()
    update_manifest_file(args)
