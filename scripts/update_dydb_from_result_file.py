import sys
import logging
import util.s3 as s3
import util.dynamodb as dy
import util.dynamodb as dynamodb
import util.agha as agha
import util as util
import util.batch as batch
import json
import time
from boto3.dynamodb.conditions import Attr

import botocore

STORE_BUCKET_NAME = 'agha-gdr-store-2.0'
STAGING_BUCKET_NAME = 'agha-gdr-staging-2.0'
RESULTS_BUCKET_NAME = 'agha-gdr-results-2.0'

DYNAMODB_STAGING_TABLE_NAME = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"
DYNAMODB_RESULT_TABLE_NAME = "agha-gdr-result-bucket"
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = "agha-gdr-result-bucket-archive"


def update_dydb_from_result_file():
    # TODO: Which submission
    directory_prefix = "ICCon/2021-07-23/"

    s3_metadata_list = s3.get_s3_object_metadata(bucket_name=RESULTS_BUCKET_NAME, directory_prefix=directory_prefix)

    dynamodb_put_item_list = []
    dynamodb_archive_put_item_list = []

    counter = 0
    length_list = len(s3_metadata_list)
    for metadata in s3_metadata_list:
        counter += 1
        print(f'Checking file: {counter}/{length_list}')
        s3_staging_key = metadata['Key']

        # Only interested in '__results.json' file, skipping otherwise
        if not s3_staging_key.endswith('__results.json'):
            continue

        # There is a chance result is being replaced from the old one. Making sure result in dynamodb
        # is cleared out before reading the new one
        sort_key = s3_staging_key.strip('__result.json')
        delete_status_and_data_from_result_table(sort_key)

        print("Processing: ", s3_staging_key)

        # Reading new result
        result_list = s3.get_object_from_bucket_name_and_s3_key(s3_key=s3_staging_key,
                                                                bucket_name=RESULTS_BUCKET_NAME)

        # Iterate and create file record accordingly
        for batch_result in result_list:
            s3_key = batch_result['staging_s3_key']
            task_type = batch_result['task_type']

            # STATUS record
            partition_key = dynamodb.ResultPartitionKey.create_partition_key_with_result_prefix(
                data_type=dynamodb.ResultPartitionKey.STATUS.value,
                check_type=task_type)

            # Some intense validation checks here
            status = validate_batch_job_result(batch_result)

            # Writing validation results
            status_record = dynamodb.ResultRecord(partition_key=partition_key,
                                                  sort_key=s3_key,
                                                  date_modified=util.get_datetimestamp(),
                                                  value=status)
            dynamodb_put_item_list.append(status_record)

            # Archive
            archive_status_record = dynamodb.ArchiveResultRecord. \
                create_archive_result_record_from_result_record(status_record,
                                                                s3.S3EventType.EVENT_OBJECT_CREATED.value)
            dynamodb_archive_put_item_list.append(archive_status_record)

            # DATA record
            data_record = create_result_data_record_from_batch_result(batch_result)
            dynamodb_put_item_list.append(data_record)

            # Archive
            archive_data_record = dynamodb.ArchiveResultRecord. \
                create_archive_result_record_from_result_record(data_record,
                                                                s3.S3EventType.EVENT_OBJECT_CREATED.value)
            dynamodb_archive_put_item_list.append(archive_data_record)

    # Write record to db
    dynamodb.batch_write_records(DYNAMODB_RESULT_TABLE_NAME, dynamodb_put_item_list)
    dynamodb.batch_write_records(DYNAMODB_ARCHIVE_RESULT_TABLE_NAME, dynamodb_archive_put_item_list)

    print('Done!')

# Helper function
def create_result_data_record_from_batch_result(batch_result: dict):
    s3_key = batch_result['staging_s3_key']
    type = batch_result['task_type']
    value = batch_result['value']
    source_file = batch_result['source_file']

    partition_key = dynamodb.ResultPartitionKey.create_partition_key_with_result_prefix(
        data_type=dynamodb.ResultPartitionKey.DATA.value,
        check_type=type)

    if value == 'FILE':
        return dynamodb.ResultRecord(partition_key=partition_key,
                                     sort_key=s3_key,
                                     date_modified=util.get_datetimestamp(),
                                     value=source_file)
    else:
        return dynamodb.ResultRecord(partition_key=partition_key,
                                     sort_key=s3_key,
                                     date_modified=util.get_datetimestamp(),
                                     value=value)


def get_checksum_from_manifest_record(table_name, staging_s3_key):
    record_response = dynamodb.get_item_from_pk_and_sk(table_name=table_name,
                                                       partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                       sort_key_prefix=staging_s3_key)
    if 'Items' not in record_response:
        msg_key_text = f'partition key {staging_s3_key}.'

    record = util.replace_record_decimal_object(record_response.get('Items')[0])
    return record[dynamodb.ManifestFileRecordAttribute.PROVIDED_CHECKSUM.value]


def validate_batch_job_result(batch_result: dict):
    s3_key = batch_result['staging_s3_key']
    type = batch_result['task_type']
    status = batch_result['status']
    value = batch_result['value']

    if type == batch.Tasks.CHECKSUM_VALIDATION.value:
        calculated_checksum = value
        provided_checksum = get_checksum_from_manifest_record(DYNAMODB_STAGING_TABLE_NAME, s3_key)

        if calculated_checksum == provided_checksum:
            return "PASS"

    else:
        if status == "SUCCEED":
            return "PASS"

    return "FAIL"


def find_status_and_data_record(sort_key: str):
    record_found = []

    for task in batch.Tasks.tasks_to_list():

        for pk_type in [dynamodb.ResultPartitionKey.STATUS.value, dynamodb.ResultPartitionKey.DATA.value]:

            pk = dynamodb.ResultPartitionKey.create_partition_key_with_result_prefix(
                data_type=pk_type,
                check_type=task
            )

            get_res = dynamodb.get_item_from_exact_pk_and_sk(DYNAMODB_RESULT_TABLE_NAME, pk, sort_key)

            if get_res['Count'] > 0:
                record_found.extend(get_res['Items'])

    return record_found


def delete_status_and_data_from_result_table(sort_key: str):
    array_to_delete = find_status_and_data_record(sort_key)
    if array_to_delete:
        dynamodb.batch_delete_from_dictionary(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                              dictionary_list=array_to_delete)
        dynamodb.batch_write_objects_archive(table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
                                             object_list=array_to_delete,
                                             archive_log=s3.S3EventType.EVENT_OBJECT_REMOVED.value)


if __name__ == '__main__':
    update_dydb_from_result_file()
