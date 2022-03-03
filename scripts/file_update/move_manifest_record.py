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
    parser.add_argument('--bucket_location', required=True, type=str, choices=[STAGING_BUCKET, STORE_BUCKET],
                        help='The bucket location of the file')
    parser.add_argument('--partition_key', required=True, type=str,
                        help='The partition key for the file')
    parser.add_argument('--old_sort_key', required=True, type=str,
                        help='Source sort key')
    parser.add_argument('--new_sort_key', required=True, type=str,
                        help='Target source key')
    parser.add_argument('--value_override', required=False, type=str,
                        help='Json String to override value')
    return parser.parse_args()


def move_and_update_manifest_record(event_payload):
    if event_payload.bucket_location == STORE_BUCKET:
        table_name = DYNAMODB_STORE_TABLE_NAME
        archive_table_name = DYNAMODB_ARCHIVE_STORE_TABLE_NAME
    else:
        table_name = DYNAMODB_STAGING_TABLE_NAME
        archive_table_name = DYNAMODB_ARCHIVE_STAGING_TABLE_NAME

    ################################################################################
    # Grab Manifest existing DyDb from
    logger.info('Fetching dynamodb record')
    dynamodb_res = dynamodb.get_item_from_exact_pk_and_sk(
        table_name=table_name,
        partition_key=event_payload.partition_key,
        sort_key=event_payload.old_sort_key,
    )

    dynamodb_item = dynamodb_res['Items'][0]

    ################################################################################
    # Manipulate any change value

    logger.info('Change dynamodb manifest record')
    # Change from json string payload (e.g. change flagship, agha_study_id )
    override_values = event_payload.value_override
    if override_values is not None:
        json_val = json.loads(override_values)
        for key in json_val:
            val = json_val[key]

            # Change to override values
            dynamodb_item[key] = val

    # Change to newly sort_key
    new_sort_key = event_payload.new_sort_key
    dynamodb_item['sort_key'] = new_sort_key

    # Change to new flagship
    new_flagship_code = new_sort_key.split('/')[0]
    dynamodb_item['flagship'] = new_flagship_code

    # Change submission field
    dynamodb_item['submission'] = os.path.dirname(new_sort_key)

    # Change date modified
    dynamodb_item['date_modified'] = util.get_datetimestamp()

    print(f'New dynamodb record: {json.dumps(dynamodb_item, indent=4)}')
    ################################################################################
    # Update DyDb

    logger.info("Adding new file to dydb")
    # Upload new dydb record
    dynamodb.batch_write_objects(
        table_name=table_name,
        object_list=[dynamodb_item]
    )

    dynamodb.batch_write_objects_archive(table_name=archive_table_name,
                                         object_list=[dynamodb_item],
                                         archive_log=s3.S3EventType.EVENT_OBJECT_CREATED.value)

    # Double-checking
    dynamodb_res = dynamodb.get_item_from_exact_pk_and_sk(
        table_name=table_name,
        partition_key=event_payload.partition_key,
        sort_key=event_payload.new_sort_key,
    )
    if dynamodb_res['Count'] == 1:
        logger.info('Success update new manifest')
    else:
        logger.error('Something wrong on updating new manifest data')
        sys.exit(1)


if __name__ == '__main__':
    event_payload = get_arguments()
    move_and_update_manifest_record(event_payload)
