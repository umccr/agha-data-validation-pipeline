
#!/usr/bin/env python3
import json
import logging
import os

# From layers
import util
from util import dynamodb, submission_data, notification, s3, agha

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
DYNAMODB_ETAG_TABLE_NAME = os.environ.get('DYNAMODB_ETAG_TABLE_NAME')
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')


# NOTE(SW): it seems reasonable to require some structuring of uploads in the format of
# <FLAGSHIP>/<DATE_TS>/<FILES ...>. Outcomes on upload wrt directory structure:
#   1. meets prescribed structure and we automatically launch validation jobs
#   2. differs from prescribed structure and data manager is notified to fix and then launch jobs
#
# Similarly, this logic could be applied to anything that might block or interfere with validation
# jobs. e.g. prohibited file types such as CRAM


# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to do a quick validation upon manifest file upload event and record to the database.

    What this lambda do:
    - check validity of the manifest
    - add Checksum from manifest to dynamodb
    - add StudyID from manifest to dynamodb
    - check and warn if file with the same etag has exist

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
            }
        ]
    }

    :param event: S3 event
    :param context: not used
    """

    logger.info(f"Start processing S3 event:")
    logger.info(json.dumps(event))

    s3_records = event.get('Records')
    if not s3_records:
        logger.warning("Unexpected S3 event format, no Records! Aborting.")
        return

    for event_record in s3_records:

        # Validate the event structure
        validate_event_data(event_record)

        # Store submission data into a class
        logger.info("Parsing s3 event record to class")
        data = submission_data.SubmissionData.create_submission_data_object_from_s3_event(event_record)

        # Set submitter information
        notification.set_submitter_information_from_s3_event(s3_records)

        # Pull file metadata from S3
        logger.info('Grab s3 object metadata')
        data.file_metadata = s3.get_s3_object_metadata(data.bucket_name, data.submission_prefix)
        logger.info('File metadata content:')
        print(data.file_metadata)

        # Collect manifest data and then validate
        logger.info('Retrieve manifest metadata')
        data.manifest_data = submission_data.retrieve_manifest_data(data.bucket_name, data.manifest_s3_key)
        file_list, data.files_extra = submission_data.validate_manifest(data)
        logger.info(f'Processing {len(file_list)} number of file_lisst, and {len(data.files_extra)} \
                    number of files_extra')

        for filename in file_list:

            partition_key = f'{data.submission_prefix}/{filename}'

            # Variables from manifest data
            agha_study_id = submission_data.find_study_id_from_manifest_df_and_filename(data.manifest_data, filename)
            provided_checksum = submission_data.find_checksum_from_manifest_df_and_filename(data.manifest_data, filename)
            logger.info(f"Variables extracted from manifest file for '{filename}'.")
            logger.info(f"AGHA_STUDY_ID:{agha_study_id}, PROVIDED_CHECKSUM:{provided_checksum}")

            # Search if file exist at s3
            logger.info('Getting dynamodb item from file_record partition and sort key')
            file_record_response = dynamodb.get_item_from_pk_and_sk(DYNAMODB_STAGING_TABLE_NAME, partition_key,
                                                                           dynamodb.FileRecordSortKey.FILE_RECORD.value)
            logger.info('file_record_response')
            logger.info(json.dumps(file_record_response, cls=util.DecimalEncoder))

            # If no File record found database. Warn and exit the application
            if file_record_response['Count'] == 0:

                notification.log_and_store_message(f"No such file found at bucket:{DYNAMODB_STAGING_TABLE_NAME}\
                 s3_key:{partition_key}", 'warning')
                notification.notify_and_exit()

            file_record_json = file_record_response['Items'][0]

            # Check if the file eTag has appear else than this staging bucket and warn if so.
            logger.info('Check if the same Etag has exist in the database')
            etag_response = dynamodb.get_item_from_pk(DYNAMODB_ETAG_TABLE_NAME, file_record_json["etag"])
            logger.info('eTag query response:')
            logger.info(json.dumps(etag_response, cls=util.DecimalEncoder))

            if etag_response['Count']>1:
                notification.log_and_store_message("File with the same eTag appear at multiple location", 'warning')
                for each_etag_appearance in etag_response['Items']:
                    # Parsing...
                    s3_key = each_etag_appearance['s3_key']
                    bucket_name = each_etag_appearance['bucket_name']

                    notification.log_and_store_message(f"bucket_name: {bucket_name}, s3_key: {s3_key}", 'warning')

            # Create Manifest type record
            manifest_record = dynamodb.ManifestFileRecord(
                partition_key=partition_key,
                sort_key=dynamodb.FileRecordSortKey.MANIFEST_FILE_RECORD.value,
                flagship=agha.FlagShip.from_name(partition_key.split("/")[0]).preferred_code(),
                filename=filename,
                filetype=agha.FileType.from_name(filename).get_name(),
                submission=data.submission_prefix,
                date_modified=util.get_datetimestamp(),
                provided_checksum=provided_checksum,
                agha_study_id=agha_study_id,
                is_in_manifest="True",
                validation_status="PASS"
            )

            # Update item at the record
            logger.info(f'Updating {DYNAMODB_STAGING_TABLE_NAME} DynamoDB table')
            write_res = dynamodb.write_record_from_class(DYNAMODB_STAGING_TABLE_NAME, manifest_record)
            logger.info(f'Updating {DYNAMODB_STAGING_TABLE_NAME} table response:')
            logger.info(json.dumps(write_res, cls=util.DecimalEncoder))

            # Updating archive record
            logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} DynamoDB table')
            archive_manifest_record = dynamodb.ArchiveManifestFileRecord.\
                create_archive_manifest_record_from_manifest_record(manifest_record, 'CREATE')
            write_res = dynamodb.write_record_from_class(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, archive_manifest_record)
            logger.info(f'Updating {DYNAMODB_ARCHIVE_STAGING_TABLE_NAME} table response:')
            logger.info(json.dumps(write_res, cls=util.DecimalEncoder))



def validate_event_data(event_record):
    if 's3' not in event_record:
        logger.critical('no \'s3\' entry found in record')
        raise ValueError
    record_s3 = event_record['s3']

    if 'bucket' not in record_s3:
        logger.critical('S3 record missing bucket info')
        raise ValueError
    elif 'name' not in record_s3['bucket']:
        logger.critical('S3 bucket record missing name info')
        raise ValueError

    if 'object' not in record_s3:
        logger.critical('S3 record missing object info')
        raise ValueError
    elif 'key' not in record_s3['object']:
        logger.critical('S3 object record missing key info')
        raise ValueError

    if record_s3['bucket']['name'] != STAGING_BUCKET:
        logger.critical(f'expected {STAGING_BUCKET} bucket but got {record_s3["bucket"]["name"]}')
        raise ValueError

