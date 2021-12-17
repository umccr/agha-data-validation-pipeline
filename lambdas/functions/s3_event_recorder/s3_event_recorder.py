from typing import List

import logging
import json
import os
import decimal

import util
import util.s3 as s3
import util.dynamodb as dynamodb
import util.batch as batch

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

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to record events produced by S3 and insert/delete on DynamoDb Tables.
    Entry point for S3 event processing. An S3 event is essentially a dict with a list of S3 Records:
    {
        "Records": [
            {
                "eventSource": "aws:s3",
                "eventTime": "2021-06-07T00:33:42.818Z",
                "eventName": "ObjectCreated:Put",
                ...
                "s3": {
                    "bucket": {
                        "name": "bucket-name",
                        ...
                    },
                    "object": {
                        "key": "UMCCR-COUMN/SBJ00805/WGS/2021-06-03/umccrised/work/SBJ00805__SBJ00805_MDX210095_L2100459/oncoviruses/work/detect_viral_reference/host_unmapped_or_mate_unmapped_to_gdc.bam.bai",
                        "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                        ...
                    }
                }
            },
            ...
        ]
    }

    :param event: S3 event
    :param context: not used
    """

    logger.info("Start processing S3 event:")
    logger.info(json.dumps(event))

    # convert S3 event payloads into more convenient S3EventRecords
    s3_event_records: List[s3.S3EventRecord] = s3.parse_s3_event(event)

    # Ideally one s3 event will only contain one record event
    # REF: https://stackoverflow.com/questions/40765699/how-many-records-can-be-in-s3-put-event-lambda-trigger/40767563#40767563

    # Iterate through each record
    for s3_record in s3_event_records:

        # Create DynamoDb record
        db_record = dynamodb.FileRecord.create_file_record_from_s3_record(s3_record)
        logger.info(f"DynamoDb record has been created from s3 event:")
        logger.info(json.dumps(db_record.__dict__, cls=util.DecimalEncoder))

        # Distinguish between different buckets
        if s3_record.bucket_name == STAGING_BUCKET:

            # Append record to the list accordingly
            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_STAGING_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record)

            elif s3_record.event_type == s3.S3EventType.EVENT_OBJECT_REMOVED:

                delete_standard_file_record(file_record_table_name=DYNAMODB_STAGING_TABLE_NAME,
                                            archive_file_record_table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                            etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                            file_record=db_record)

                # Delete MANIFEST file record
                delete_manifest_file_record(manifest_record_table_name=DYNAMODB_STAGING_TABLE_NAME,
                                            manifest_record_archive_table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                            db_record=db_record)

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == STORE_BUCKET:

            # Append record to list accordingly
            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record)

            elif s3_record.event_type == s3.S3EventType.EVENT_OBJECT_REMOVED:

                # Delete FILE record
                delete_standard_file_record(file_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                            archive_file_record_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                            etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                            file_record=db_record)

                delete_manifest_file_record(manifest_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                            manifest_record_archive_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                            db_record=db_record)

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == RESULT_BUCKET:
            # NOTE: Current S3 event configuration only send create object event
            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_RESULT_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record)

                # Read content for result file
                if "__results.json" in s3_record.object_key:
                    result_list = s3.get_object_from_bucket_name_and_s3_key(s3_key=s3_record.object_key,
                                                                            bucket_name=s3_record.bucket_name)
                    logger.info(f'Grab data from result file')

                    dynamodb_put_item_list = []
                    dynamodb_archive_put_item_list = []
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

        else:
            logger.warning(f"Unsupported AGHA bucket: {s3_record.bucket_name}")

    return None


########################################################################################################################
# The following are helper function used for the handler

def write_standard_file_record(file_record_table_name: str, archive_file_record_table_name: str,
                               etag_table_name: str, file_record: dynamodb.FileRecord):
    """
    This function is aim to remove redundancy of writing dynamodb for a file record. This include writing FileRecord,
    ArchiveFileRecord, and eTag record.
    :param file_record_table_name: The table name to write
    :param archive_file_record_table_name: The table name to write
    :param etag_table_name: The table name to write
    :param file_record: The File record class
    :return:
    """
    # Write to database
    logger.info(f'Updating records at {file_record_table_name}')
    write_res = dynamodb.write_record_from_class(file_record_table_name, file_record)
    logger.info(f'Updating {file_record_table_name} table response:')
    logger.info(json.dumps(write_res, cls=util.DecimalEncoder))

    # Write Archive record
    db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
        file_record, s3.S3EventType.EVENT_OBJECT_CREATED.value)
    logger.info(f'Updating records at {archive_file_record_table_name}')
    write_res = dynamodb.write_record_from_class(archive_file_record_table_name, db_record_archive)
    logger.info(f'Updating {archive_file_record_table_name} table response:')
    logger.info(json.dumps(write_res, cls=util.DecimalEncoder))

    # Construct ETag record
    etag_record = dynamodb.ETagFileRecord(
        etag=file_record.etag,
        s3_key=file_record.s3_key,
        bucket_name=file_record.bucket_name)

    # Updating ETag record
    logger.info(f'Updating ETag file to the ETag record')
    write_res = dynamodb.write_record_from_class(etag_table_name, etag_record)
    logger.info(f'Updating {etag_table_name} table response:')
    logger.info(json.dumps(write_res, cls=util.DecimalEncoder))


def delete_standard_file_record(file_record_table_name: str, archive_file_record_table_name: str,
                                etag_table_name: str, file_record: dynamodb.FileRecord):
    """
    This function is aim to remove redundancy of writing dynamodb for a file record. This include writing FileRecord,
    ArchiveFileRecord, and eTag record.
    :param file_record_table_name: The table name to write
    :param archive_file_record_table_name: The table name to write
    :param etag_table_name: The table name to write
    :param file_record: The File record class
    :return:
    """

    # Get existing file item to delete
    logger.info(f'Getting item to delete at {file_record_table_name}')
    get_item_res = dynamodb.get_item_from_pk_and_sk(
        table_name=file_record_table_name,
        partition_key=file_record.partition_key,
        sort_key_prefix=file_record.sort_key)
    logger.info(f'Get Item response:')
    logger.info(json.dumps(get_item_res, cls=util.DecimalEncoder))

    # Delete FILE record
    logger.info(f'Updating records at {file_record_table_name}')
    delete_item = dynamodb.delete_record_from_record_class(file_record_table_name, file_record)
    logger.info(f'Delete the following record from {file_record_table_name} table')
    print(delete_item)

    # Archive database
    db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
        file_record=file_record, archive_log=s3.S3EventType.EVENT_OBJECT_REMOVED.value)
    logger.info(f'Updating records at {archive_file_record_table_name}. Archive table:')
    logger.info(json.dumps(db_record_archive.__dict__, cls=util.DecimalEncoder))
    write_res = dynamodb.write_record_from_class(archive_file_record_table_name, db_record_archive)
    logger.info(f'Updating {archive_file_record_table_name} table response:')
    logger.info(json.dumps(write_res, cls=util.DecimalEncoder))

    if get_item_res['Count'] > 0:
        logger.info('Existing record found')
        record_json = get_item_res['Items'][0]

        logger.info('Deleting record From eTag table')
        # Construct ETag record from deletion response
        etag_record = dynamodb.ETagFileRecord(
            etag=record_json["etag"],
            s3_key=record_json["s3_key"],
            bucket_name=record_json["bucket_name"])

        # delete from etag table
        logger.info(f'Deleting from {etag_table_name}')
        delete_item = dynamodb.delete_record_from_record_class(etag_table_name, etag_record)
        logger.info(f'Delete the following record from {etag_table_name} table')
        logger.info(json.dumps(delete_item, cls=util.DecimalEncoder))


def delete_manifest_file_record(manifest_record_table_name, manifest_record_archive_table_name, db_record):
    # Delete MANIFEST file record
    # Grab from existing record for archive record
    logger.info('Grab Manifest data before deletion')
    manifest_res = dynamodb.get_item_from_pk_and_sk(manifest_record_table_name,
                                                    db_record.partition_key,
                                                    dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value)
    logger.info(f'Get manifest record response:')
    logger.info(json.dumps(manifest_res, cls=util.DecimalEncoder))

    if manifest_res["Count"] == 1:
        logger.info(f'Deleting {manifest_res["Count"]} number of records')

        # Parsing records
        manifest_json = manifest_res["Items"][0]
        manifest_record = dynamodb.ManifestFileRecord(**manifest_json)

        # Delete from record
        delete_item = dynamodb.delete_record_from_record_class(manifest_record_table_name, manifest_record)
        logger.info(f'Delete the following record from {manifest_record_table_name} table')
        logger.info(json.dumps(delete_item, cls=util.DecimalEncoder))

        # Archive database
        db_record_archive = dynamodb.ArchiveManifestFileRecord. \
            create_archive_manifest_record_from_manifest_record(manifest_record,
                                                                s3.S3EventType.EVENT_OBJECT_REMOVED.value)
        logger.info(f'Updating records at {manifest_record_archive_table_name}')
        write_res = dynamodb.write_record_from_class(manifest_record_archive_table_name, db_record_archive)
        dynamodb.write_record_from_class(manifest_record_archive_table_name, db_record_archive)
        logger.info(f'Updating {manifest_record_archive_table_name} table response:')
        logger.info(json.dumps(write_res, cls=util.DecimalEncoder))


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


def get_checksum_from_manifest_record(table_name, staging_s3_key):
    record_response = dynamodb.get_item_from_pk_and_sk(table_name=table_name,
                                                       partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                       sort_key_prefix=staging_s3_key)
    logger.info(f'Manifest record response:')
    print(record_response)
    if 'Items' not in record_response:
        msg_key_text = f'partition key {staging_s3_key}.'
        logger.critical(f'could not retrieve DynamoDB entry with {msg_key_text}')

    record = util.replace_record_decimal_object(record_response.get('Items')[0])
    return record[dynamodb.ManifestFileRecordAttribute.PROVIDED_CHECKSUM.value]


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
