from typing import List

import logging
import json
import os
from util.s3 import S3EventType, S3EventRecord, parse_s3_event
import util.dynamodb as dyndb

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')


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
        db_record = dyndb.convert_s3_record_to_db_record(s3_record)
        logger.info(f"DynamoDb record has been created from s3 event:")
        logger.info(json.dumps(db_record.__dict__))

        # Distinguish between different buckets
        if s3_record.bucket_name == STAGING_BUCKET:

            # Append record to the list accordingly
            if s3_record.event_type == S3EventType.EVENT_OBJECT_CREATED:
                db_record_archive = dyndb.create_archive_record_from_db_record(
                    db_record, S3EventType.EVENT_OBJECT_CREATED.value)

                # Write to database
                logger.info(
                    f'Updating records at {DYNAMODB_STAGING_TABLE_NAME}')
                dyndb.write_record(DYNAMODB_STAGING_TABLE_NAME, db_record)
                logger.info(
                    f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                dyndb.write_record(
                    DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)

            elif s3_record.event_type == S3EventType.EVENT_OBJECT_REMOVED:
                db_record_archive = dyndb.create_archive_record_from_db_record(
                    db_record, S3EventType.EVENT_OBJECT_REMOVED.value)

                # Write to database
                logger.info(
                    f'Updating records at {DYNAMODB_STAGING_TABLE_NAME}')
                dyndb.delete_record(DYNAMODB_STAGING_TABLE_NAME, db_record)
                logger.info(
                    f'Updating records at {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME}')
                dyndb.write_record(
                    DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)

            else:
                logger.warning(
                    f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        elif s3_record.bucket_name == STORE_BUCKET:
            print("Under Development")
            # # Append record to list accordingly
            # if s3_record.event_type == S3EventType.EVENT_OBJECT_CREATED:
            #     store_db_records_create.append(db_record)

            # elif s3_record.event_type == S3EventType.EVENT_OBJECT_REMOVED:
            #     store_db_records_create.append(db_record)

            # else:
            #     logger.warning(f"Unsupported S3 event type {s3_record.event_type} for {s3_record}")

        else:
            logger.warning(f"Unsupported AGHA bucket: {s3_record.bucket_name}")

    return None
