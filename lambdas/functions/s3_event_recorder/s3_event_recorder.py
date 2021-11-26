from typing import List

import logging
import json
import os

import util
import util.s3 as s3
import util.dynamodb as dynamodb

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
        logger.info(json.dumps(db_record.__dict__))

        # Construct ETag record
        etag_record = dynamodb.ETagFileRecord(
            etag=db_record.etag,
            s3_key=db_record.s3_key,
            bucket_name= s3_record.bucket_name)

        # Distinguish between different buckets
        if s3_record.bucket_name == STAGING_BUCKET:

            # Append record to the list accordingly
            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_STAGING_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record,
                                           etag_record=etag_record)


            elif s3_record.event_type == s3.S3EventType.EVENT_OBJECT_REMOVED:

                delete_standard_file_record(file_record_table_name=DYNAMODB_STAGING_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record,
                                           etag_record=etag_record)

                # Delete MANIFEST file record
                # Grab from existing record for archive record
                manifest_json = dynamodb.get_item_from_pk_and_sk(DYNAMODB_STAGING_TABLE_NAME,
                                                                   db_record.s3_key,
                                                                   dynamodb.FileRecordSortKey.MANIFEST_FILE_RECORD.value)
                manifest_record = dynamodb.ManifestFileRecord(**manifest_json)

                # Delete from record
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_STAGING_TABLE_NAME, manifest_record)
                logger.info(f'Delete the following record from {DYNAMODB_STAGING_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

                # Archive database
                db_record_archive = dynamodb.ArchiveManifestFileRecord.\
                    create_archive_manifest_record_from_manifest_record(manifest_record,
                                                                        s3.S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))



            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == STORE_BUCKET:

            # Append record to list accordingly
            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record,
                                           etag_record=etag_record)


            elif s3_record.event_type == s3.S3EventType.EVENT_OBJECT_REMOVED:

                # Delete FILE record
                delete_standard_file_record(file_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record,
                                           etag_record=etag_record)

                # Delete MANIFEST file record
                # Grab from existing record for archive record
                manifest_json = dynamodb.get_item_from_pk_and_sk(DYNAMODB_STORE_TABLE_NAME,
                                                                   db_record.s3_key,
                                                                   dynamodb.FileRecordSortKey.MANIFEST_FILE_RECORD.value)
                manifest_record = dynamodb.ManifestFileRecord(**manifest_json)

                # Delete from main record
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_STORE_TABLE_NAME, manifest_record)
                logger.info(f'Delete the following record from {DYNAMODB_STORE_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

                # Archive database
                db_record_archive = dynamodb.ArchiveManifestFileRecord.\
                    create_archive_manifest_record_from_manifest_record(manifest_record,
                                                                        s3.S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STORE_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STORE_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == RESULT_BUCKET:

            if s3_record.event_type == s3.S3EventType.EVENT_OBJECT_CREATED:

                write_standard_file_record(file_record_table_name=DYNAMODB_STORE_TABLE_NAME,
                                           archive_file_record_table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                           etag_table_name=DYNAMODB_ETAG_TABLE_NAME,
                                           file_record=db_record,
                                           etag_record=etag_record)

                # Read content for result file
                if "__results.json" in s3_record.object_key:
                    result_list = s3.get_object_from_bucket_name_and_s3_key(s3_key=s3_record.object_key,
                                                      bucket_name=s3_record.bucket_name)

                    dynamodb_put_item_list = []
                    # Iterate and create file record accordingly
                    for batch_result in result_list:

                        s3_key = batch_result['staging_s3_key']
                        type = batch_result['type']
                        status = batch_result['status']
                        value = batch_result['value']

                        # STATUS record

                        status_record = dynamodb.ResultRecord(partition_key=s3_key,
                                                              sort_key=dynamodb.ResultSortKeyPrefix.STATUS.value,
                                                              date_modified=util.get_datetimestamp(),
                                                              value=status)
                        dynamodb_put_item_list.append(status_record)

                        # DATA record
                        data_record = dynamodb.ResultRecord(partition_key=s3_key,
                                                            sort_key=dynamodb.ResultSortKeyPrefix.DATA.value,
                                                            date_modified=util.get_datetimestamp(),
                                                            value=value)
                        dynamodb_put_item_list.append(data_record)

                    # Write record to db
                    dynamodb.batch_write_records(DYNAMODB_RESULT_TABLE_NAME, dynamodb_put_item_list)

        else:
            logger.warning(f"Unsupported AGHA bucket: {s3_record.bucket_name}")

    return None

########################################################################################################################
# The following are helper function used for the handler

def write_standard_file_record(file_record_table_name:str, archive_file_record_table_name:str,
                               etag_table_name:str, file_record:dynamodb.FileRecord,
                               etag_record:dynamodb.ETagFileRecord):
    """
    This function is aim to remove redundancy of writing dynamodb for a file record. This include writing FileRecord,
    ArchiveFileRecord, and eTag record.
    :param file_record_table_name: The table name to write
    :param archive_file_record_table_name: The table name to write
    :param etag_table_name: The table name to write
    :param file_record: The File record class
    :param etag_record: The eTag record class
    :return:
    """
    # Write to database
    logger.info(f'Updating records at {file_record_table_name}')
    write_res = dynamodb.write_record_from_class(file_record_table_name, file_record)
    logger.info(f'Updating {file_record_table_name} table response:')
    logger.info(json.dumps(write_res))

    # Write Archive record
    db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
        file_record, s3.S3EventType.EVENT_OBJECT_CREATED.value)
    logger.info(f'Updating records at {archive_file_record_table_name}')
    write_res = dynamodb.write_record_from_class(archive_file_record_table_name, db_record_archive)
    logger.info(f'Updating {archive_file_record_table_name} table response:')
    logger.info(json.dumps(write_res))

    # Updating ETag record
    logger.info(f'Updating ETag file to the ETag record')
    write_res = dynamodb.write_record_from_class(etag_table_name,etag_record)
    logger.info(f'Updating {etag_table_name} table response:')
    logger.info(json.dumps(write_res))

def delete_standard_file_record(file_record_table_name:str, archive_file_record_table_name:str,
                               etag_table_name:str, file_record:dynamodb.FileRecord,
                               etag_record:dynamodb.ETagFileRecord):
    """
    This function is aim to remove redundancy of writing dynamodb for a file record. This include writing FileRecord,
    ArchiveFileRecord, and eTag record.
    :param file_record_table_name: The table name to write
    :param archive_file_record_table_name: The table name to write
    :param etag_table_name: The table name to write
    :param file_record: The File record class
    :param etag_record: The eTag record class
    :return:
    """

    # Delete FILE record
    logger.info(f'Updating records at {file_record_table_name}')
    delete_item = dynamodb.delete_record_from_record_class(file_record_table_name, file_record)
    logger.info(f'Delete the following record from {file_record_table_name} table')
    logger.info(json.dumps(delete_item))

    # Archive database
    db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
        file_record=file_record, archive_log=s3.S3EventType.EVENT_OBJECT_REMOVED.value)
    logger.info(f'Updating records at {archive_file_record_table_name}')
    write_res = dynamodb.write_record_from_class(archive_file_record_table_name, db_record_archive)
    dynamodb.write_record_from_class(archive_file_record_table_name, db_record_archive)
    logger.info(f'Updating {archive_file_record_table_name} table response:')
    logger.info(json.dumps(write_res))

    # delete from etag table
    logger.info(f'Updating records at {etag_table_name}')
    delete_item = dynamodb.delete_record_from_record_class(etag_table_name, etag_record)
    logger.info(f'Delete the following record from {etag_table_name} table')
    logger.info(json.dumps(delete_item))
