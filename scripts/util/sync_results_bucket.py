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



def sync_result_bucket(args):

    # TODO: Which bucket to sync to (staging/store bucket or staging/store manifest dynamodb table)
    sync_from = args.sync_from
    # TODO: Change the submission prefix
    sort_key_prefix = args.sort_key_prefix

    # Grab filename from main bucket
    if sync_from == STAGING_BUCKET or sync_from == STORE_BUCKET:
        metadata_list_main_bucket = s3.get_s3_object_metadata(bucket_name=sync_from, directory_prefix=sort_key_prefix)
        filename_main_bucket = [metadata['Key'].split('/')[-1] for metadata in metadata_list_main_bucket]

    elif sync_from == DYNAMODB_STAGING_TABLE_NAME or sync_from == DYNAMODB_STORE_TABLE_NAME:
        item_res = dynamodb.get_batch_item_from_pk_and_sk(table_name=sync_from,
                                                          partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                          sort_key_prefix=sort_key_prefix)
        filename_main_bucket = [item['filename'] for item in item_res]
    else:
        print('Unexpected value, please specify the corrct sync_from variable')
        return

    # Grab results file data
    metadata_list_results_bucket = s3.get_s3_object_metadata(bucket_name=RESULT_BUCKET,
                                                             directory_prefix=sort_key_prefix)
    s3key_results_bucket = [metadata['Key'] for metadata in metadata_list_results_bucket]

    list_for_deletion = []

    for s3_key in s3key_results_bucket:
        result_filename = s3_key.split('/')[-1]

        # Strip ending data to get exact filename appear in the staging/store bucket
        if result_filename.endswith('__log.txt'):
            basename = result_filename.strip('__log.txt')
        elif result_filename.endswith('__results.json'):
            basename = result_filename.strip('__results.json')
        elif result_filename.endswith('.bai'):
            basename = result_filename.strip('.bai')
        elif result_filename.endswith('.tbi'):
            basename = result_filename.strip('.tbi')
        elif result_filename.endswith('.gz'):
            basename = result_filename.strip('.gz')
        else:
            continue

        # Appending list for removal if it is not in the main bucket
        if basename not in filename_main_bucket:
            list_for_deletion.append(s3_key)

    # Print result for deletion list before executing it
    print(' File to delete from s3 results:', json.dumps(list_for_deletion, indent=4))

    res = s3.delete_s3_object_from_key(RESULT_BUCKET, list_for_deletion)
    print(res)


if __name__ == '__main__':

    args = get_arguments()

    sync_result_bucket(args)
