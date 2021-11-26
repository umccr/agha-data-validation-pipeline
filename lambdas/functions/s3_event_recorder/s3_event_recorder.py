from typing import List

import logging
import json
import os

from util.s3 import S3EventType, S3EventRecord, parse_s3_event
import util.dynamodb as dynamodb

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')

DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = os.environ.get(
    'DYNAMODB_ARCHIVE_STORE_TABLE_NAME')

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
    s3_event_records: List[S3EventRecord] = parse_s3_event(event)

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
            if s3_record.event_type == S3EventType.EVENT_OBJECT_CREATED:

                # Write to database
                logger.info(f'Updating records at {DYNAMODB_STAGING_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_STAGING_TABLE_NAME, db_record)
                logger.info(f'Updating {DYNAMODB_STAGING_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # Write Archive record
                db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
                    db_record, S3EventType.EVENT_OBJECT_CREATED)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # Updating ETag record
                logger.info(f'Updating ETag file to the ETag record')
                dynamodb.write_record_from_class(
                    DYNAMODB_ETAG_TABLE_NAME,
                    etag_record
                )

            elif s3_record.event_type == S3EventType.EVENT_OBJECT_REMOVED:

                # Delete FILE record
                logger.info(f'Updating records at {DYNAMODB_STAGING_TABLE_NAME}')
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_STAGING_TABLE_NAME, db_record)
                logger.info(f'Delete the following record from {DYNAMODB_STAGING_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

                # Archive database
                db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
                    db_record, S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

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
                                                                        S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # delete from etag table
                logger.info(f'Updating records at {DYNAMODB_ETAG_TABLE_NAME}')
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_ETAG_TABLE_NAME, etag_record)
                logger.info(f'Delete the following record from {DYNAMODB_ETAG_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == STORE_BUCKET:

            # Append record to list accordingly
            if s3_record.event_type == S3EventType.EVENT_OBJECT_CREATED:

                # Write to database
                logger.info(f'Updating records at {DYNAMODB_STORE_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_STORE_TABLE_NAME, db_record)
                logger.info(f'Updating {DYNAMODB_STORE_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # Write Archive record
                db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
                    db_record, S3EventType.EVENT_OBJECT_CREATED)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STORE_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STORE_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # Updating ETag record
                logger.info(f'Updating ETag file to the ETag record')
                write_res = dynamodb.write_record_from_class(
                    DYNAMODB_ETAG_TABLE_NAME,
                    etag_record
                )
                logger.info(f'Updating {DYNAMODB_ETAG_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

            elif s3_record.event_type == S3EventType.EVENT_OBJECT_REMOVED:

                # Delete FILE record
                # From main database
                logger.info(f'Updating records at {DYNAMODB_STORE_TABLE_NAME}')
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_STORE_TABLE_NAME, db_record)
                logger.info(f'Delete the following record from {DYNAMODB_STORE_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

                # Archive database
                db_record_archive = dynamodb.ArchiveFileRecord.create_archive_file_record_from_file_record(
                    db_record, S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STORE_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STORE_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

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
                                                                        S3EventType.EVENT_OBJECT_REMOVED.value)
                logger.info(f'Updating records at {DYNAMODB_ARCHIVE_STORE_TABLE_NAME}')
                write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, db_record_archive)
                logger.info(f'Updating {DYNAMODB_ARCHIVE_STORE_TABLE_NAME} table response:')
                logger.info(json.dumps(write_res))

                # delete from etag table
                logger.info(f'Updating records at {DYNAMODB_ETAG_TABLE_NAME}')
                delete_item = dynamodb.delete_record_from_record_class(DYNAMODB_ETAG_TABLE_NAME, etag_record)
                logger.info(f'Delete the following record from {DYNAMODB_ETAG_TABLE_NAME} table')
                logger.info(json.dumps(delete_item))

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        else:
            logger.warning(f"Unsupported AGHA bucket: {s3_record.bucket_name}")

    return None
