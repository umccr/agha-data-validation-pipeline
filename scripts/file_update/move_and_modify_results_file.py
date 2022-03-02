#!/usr/bin/env python3
import os
import argparse
import sys
import json
import logging

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "lambdas", "layers", "util"
)
sys.path.append(SOURCE_PATH)

import util
from util import agha, s3, dynamodb

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
    parser.add_argument('--source_s3_key', required=True, type=str,
                        help='Source results s3 key')
    parser.add_argument('--target_s3_key', required=True, type=str,
                        help='Target results s3 key')
    return parser.parse_args()


def mode_and_modify_result(args):

    original_source_s3_key = args.source_s3_key
    original_target_s3_key = args.target_s3_key

    # Convert filename to submitted naming (convert to uncompress if submitted uncompress)
    if agha.FileType.is_compress_file(args.source_s3_key):
        # Find for non-compress file if it was used at the result stage
        dydb_res = dynamodb.get_item_from_exact_pk_and_sk(
            table_name=DYNAMODB_RESULT_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
            sort_key=f"{args.source_s3_key.strip('.gz')}__results.json"
        )
        if dydb_res['Count'] == 1:

            original_source_s3_key = f"{args.source_s3_key.strip('.gz')}"
            original_target_s3_key = f"{args.target_s3_key.strip('.gz')}"

    results_source_s3_key = args.source_s3_key + '__results.json'
    results_target_s3_key = args.target_s3_key + '__results.json'

    # Try to grab results list file
    try:
        results_list = s3.get_object_from_bucket_name_and_s3_key(bucket_name=RESULT_BUCKET,
                                                                 s3_key=results_source_s3_key)

    except s3.S3_CLIENT.exceptions.NoSuchKey as e:
        print(f"File not found error: {e}")
        print("Possible data has not gone through the validation or no data generated for this s3_key.")
        print("Exiting here ... ")
        sys.exit(1)

    # Modify content of the array
    new_s3_key = results_target_s3_key.strip('__results.json')
    for result_data in results_list:
        result_data['staging_s3_key'] = new_s3_key

    print(f'New results.json file: {json.dumps(results_list, indent=4, cls=util.JsonSerialEncoder)}')

    # Upload back to s3
    s3.upload_s3_object_from_string(bucket_name=RESULT_BUCKET,
                                    byte_of_string=json.dumps(results_list, indent=4, cls=util.JsonSerialEncoder),
                                    s3_key_destination=results_target_s3_key)
    # Delete old results files
    s3.delete_s3_object_from_key(bucket_name=RESULT_BUCKET,
                                 key_list=[results_source_s3_key])
    print('Modification completed')

    ######################################################################################################
    # Moving log files

    s3_uri_source=f"s3://{RESULT_BUCKET}/{original_source_s3_key}__log.txt"
    s3_uri_target=f"s3://{RESULT_BUCKET}/{original_target_s3_key}__log.txt"
    os.system(f"aws s3 mv {s3_uri_source} {s3_uri_target}")


if __name__ == '__main__':
    arg = get_arguments()
    mode_and_modify_result(arg)
