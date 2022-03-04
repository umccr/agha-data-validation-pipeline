import sys
import os
import logging
import argparse
import json
import time
import botocore

from boto3.dynamodb.conditions import Attr

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "lambdas", "layers", "util"
)
sys.path.append(SOURCE_PATH)

from util import s3, dynamodb, agha, batch

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
    parser.add_argument('--sync_from', required=True, type=str,
                        help='Which bucket to sync to (staging/store bucket or staging/store manifest dynamodb table)')
    parser.add_argument('--sort_key_prefix', required=True, type=str,
                        help=' submission prefix')
    return parser.parse_args()


def find_file_base_name(result_filename):
    basename = result_filename

    # Remove '__log.txt' and '__results.json' postfix
    if basename.endswith('__log.txt'):
        basename = basename.strip('__log.txt')
    elif basename.endswith('__results.json'):
        basename = basename.strip('__results.json')

    # Remove any indexing file postfix
    if basename.endswith('.bai'):
        basename = basename.strip('.bai')
    elif basename.endswith('.tbi'):
        basename = basename.strip('.tbi')

    # Remove any compressing file postfix
    if basename.endswith('.gz'):
        basename = basename.strip('.gz')

    return basename


def sync_result_bucket(args):
    # TODO: Which bucket to sync to (staging/store bucket or staging/store manifest dynamodb table)
    sync_from = args.sync_from
    # TODO: Change the submission prefix
    sort_key_prefix = args.sort_key_prefix

    # Grab filename from main bucket
    if sync_from == STAGING_BUCKET or sync_from == STORE_BUCKET:
        metadata_list_main_bucket = s3.get_s3_object_metadata(bucket_name=sync_from, directory_prefix=sort_key_prefix)
        filename_main_bucket = [metadata['Key'].split('/')[-1] for metadata in metadata_list_main_bucket]

        # Removing index file from the main bucket
        # Index file is to be ignored as it will not have its own results/log file
        filename_main_bucket = [key for key in filename_main_bucket if not agha.FileType.is_index_file(key)]

    elif sync_from == DYNAMODB_STAGING_TABLE_NAME or sync_from == DYNAMODB_STORE_TABLE_NAME:
        item_res = dynamodb.get_batch_item_from_pk_and_sk(table_name=sync_from,
                                                          partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                          sort_key_prefix=sort_key_prefix)
        filename_main_bucket = [item['filename'] for item in item_res]
    else:
        print('Unexpected value, please specify the corrct sync_from variable')
        return

    # Remove any file ending with .gz
    filename_main_bucket = [find_file_base_name(filename) for filename in filename_main_bucket]
    # Grab results file data
    metadata_list_results_bucket = s3.get_s3_object_metadata(bucket_name=RESULT_BUCKET,
                                                             directory_prefix=sort_key_prefix)
    s3key_results_bucket = [metadata['Key'] for metadata in metadata_list_results_bucket]

    list_for_deletion = []
    for s3_key in s3key_results_bucket:
        result_filename = s3_key.split('/')[-1]

        basename = find_file_base_name(result_filename)

        # Appending list for removal if it is not in the main bucket
        if basename not in filename_main_bucket:
            list_for_deletion.append(s3_key)

    # Print result for deletion list before executing it
    print('File to delete from s3 results: ', json.dumps(list_for_deletion, indent=4))
    print('Number of files to delete: ', len(list_for_deletion))

    s3.delete_s3_object_from_key(RESULT_BUCKET, list_for_deletion)


if __name__ == '__main__':
    args = get_arguments()

    sync_result_bucket(args)
