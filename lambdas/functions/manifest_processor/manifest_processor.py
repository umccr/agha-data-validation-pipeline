# !/usr/bin/env python3
import json
import logging
import os

# From layers
import sys

import util
from util import dynamodb, submission_data, notification, s3, agha, batch

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
DYNAMODB_ETAG_TABLE_NAME = os.environ.get('DYNAMODB_ETAG_TABLE_NAME')
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
VALIDATION_MANAGER_LAMBDA_ARN = os.environ.get('VALIDATION_MANAGER_LAMBDA_ARN')
AUTORUN_VALIDATION_JOBS = os.environ.get('AUTORUN_VALIDATION_JOBS')

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

    structure#2
    For manual re-trigger
    {
        "bucket_name": "somebucketname",
        "manifest_fp": "FLAGSHIP/SUBMISSION/manifest.txt",
        "email_report_to": "john.doe@email.com",
        "skip_auto_validation": "true",
        "skip_update_dynamodb": "true",
        "skip_send_notification": "true",
        "skip_checksum_validation": "true",
        "exception_postfix_filename": ["metadata.txt", ".md5", etc.],
    }

    :param event: S3 event
    :param context: not used
    """

    # Reset notification variable (in case value cached between lambda)
    notification.MESSAGE_STORE = list()
    notification.SUBMITTER_INFO = notification.SubmitterInfo()

    logger.info(f"Start processing S3 event:")
    logger.info(json.dumps(event, indent=4, cls=util.JsonSerialEncoder))

    # If trigger manually, construct the same s3 format
    if event.get('manifest_fp') != None:
        event['Records'] = [
            {
                "s3": {
                    "bucket": {
                        "name": event.get('bucket_name'),
                    },
                    "object": {
                        "key": event.get('manifest_fp')
                    }
                },
                "email_report_to": event.get("email_report_to")
            }
        ]

    if event.get('exception_postfix_filename') != None:
        exception_filename = event.get('exception_postfix_filename')
    else:
        exception_filename = []

    if event.get('skip_checksum_validation') == 'true':
        skip_checksum_validation = True
    else:
        skip_checksum_validation = False

    s3_records = event.get('Records')
    if not s3_records:
        logger.warning("Unexpected S3 event format, no Records! Aborting.")
        return

    for event_record in s3_records:

        # DynamoDB manifest ist
        staging_dynamodb_batch_write_list = []
        archive_staging_dynamodb_batch_write_list = []
        duplicate_etag_list = []

        # Validate the event structure
        validate_event_data(event_record)

        # Store submission data into a class
        logger.info("Parsing s3 event record to class")
        data = submission_data.SubmissionData.create_submission_data_object_from_s3_event(event_record)

        # Set submitter information
        notification.set_submitter_information_from_s3_event(event_record)

        # Pull file metadata from S3
        logger.info('Grab s3 object metadata')
        data.file_metadata = s3.get_s3_object_metadata(data.bucket_name, data.submission_prefix)
        logger.info('File metadata content:')
        print(data.file_metadata)

        # Collect manifest data and then validate
        logger.info('Retrieve manifest metadata')
        data.manifest_data = submission_data.retrieve_manifest_data(data.bucket_name, data.manifest_s3_key)

        try:
            file_list, data.files_extra = submission_data.validate_manifest(data, exception_filename,
                                                                            skip_checksum_check=skip_checksum_validation)

        except ValueError as e:

            # Update DynamoDb regarding manifest checks status
            manifest_status_record = dynamodb.ManifestStatusCheckRecord(
                sort_key=data.manifest_s3_key,
                status=dynamodb.ManifestStatusCheckValue.FAIL.value,
                additional_information=json.loads(str(e))
            )

            dynamodb.write_record_from_class(DYNAMODB_STAGING_TABLE_NAME, manifest_status_record)
            manifest_status_record_archive = manifest_status_record.create_archive_dictionary('ObjectCreated/Update')
            dynamodb.write_record_from_dict(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, manifest_status_record_archive)

            notification.notify_and_exit()
            raise ValueError(e)
            # Update DynamoDb regarding manifest checks status

        # Create status pass when no error raised
        manifest_status_record = dynamodb.ManifestStatusCheckRecord(
            sort_key=data.manifest_s3_key,
            status=dynamodb.ManifestStatusCheckValue.PASS.value,
        )

        logger.info(f'Processing {len(file_list)} number of file_list, and {len(data.files_extra)} \
                    number of files_extra')

        notification.log_and_store_message('<br>Below, additional information on duplicate files, if any.<br>')

        for filename in file_list:

            sort_key = f'{data.submission_prefix}/{filename}'

            # Variables from manifest data
            agha_study_id = submission_data.find_study_id_from_manifest_df_and_filename(data.manifest_data, filename)
            provided_checksum = submission_data.find_checksum_from_manifest_df_and_filename(data.manifest_data,
                                                                                            filename)
            logger.info(f"Variables extracted from manifest file for '{filename}'.")
            logger.info(f"AGHA_STUDY_ID:{agha_study_id}, PROVIDED_CHECKSUM:{provided_checksum}")

            # Search if file exist at s3
            logger.info('Getting dynamodb item from file_record partition and sort key')
            file_record_response = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                                    partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
                                                                    sort_key_prefix=sort_key)
            logger.info('file_record_response')
            logger.info(json.dumps(file_record_response, indent=4, cls=util.JsonSerialEncoder))

            # If no File record found database. Warn and exit the application
            if file_record_response['Count'] == 0:
                notification.log_and_store_message(f"No such file found at bucket:{DYNAMODB_STAGING_TABLE_NAME}\
                 s3_key:{sort_key}", 'warning')
                notification.notify_and_exit()

            file_record_json = file_record_response['Items'][0]

            # Check if the file eTag has appear else than this staging bucket and warn if so.
            logger.info('Check if the same Etag has exist in the database')
            etag_response = dynamodb.get_item_from_pk(DYNAMODB_ETAG_TABLE_NAME, file_record_json["etag"])
            logger.info('eTag query response:')
            logger.info(json.dumps(etag_response, indent=4, cls=util.JsonSerialEncoder))

            if etag_response['Count'] > 1:
                notification.log_and_store_message("File with the same eTag appear at multiple location", 'warning')
                for each_etag_appearance in etag_response['Items']:
                    # Parsing...
                    s3_key = each_etag_appearance['s3_key']
                    bucket_name = each_etag_appearance['bucket_name']

                    notification.log_and_store_message(f"bucket_name: {bucket_name}, s3_key: {s3_key}", 'warning')

                duplicate_etag_list.append(etag_response['Items'])

            # Create Manifest type record
            manifest_record = dynamodb.ManifestFileRecord(
                partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                sort_key=sort_key,
                flagship=agha.FlagShip.from_name(sort_key.split("/")[0]).preferred_code(),
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
            staging_dynamodb_batch_write_list.append(manifest_record.__dict__)
            archive_manifest_record = dynamodb.ArchiveManifestFileRecord. \
                create_archive_manifest_record_from_manifest_record(manifest_record, 'CREATE')
            archive_staging_dynamodb_batch_write_list.append(archive_manifest_record.__dict__)

        if len(duplicate_etag_list) > 0:
            manifest_status_record.additional_information = {
                "topic": "Duplicate files found in this submission with other submissions",
                "data": duplicate_etag_list
            }

        staging_dynamodb_batch_write_list.append(manifest_status_record.__dict__)
        archive_staging_dynamodb_batch_write_list.append(
            manifest_status_record.create_archive_dictionary('Create/Update'))

        # Update dynamodb batch if not skipped
        if not event.get('skip_update_dynamodb') == 'true':
            dynamodb.batch_write_objects(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                         object_list=staging_dynamodb_batch_write_list)
            dynamodb.batch_write_objects(table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                                         object_list=archive_staging_dynamodb_batch_write_list)

        # Send notification to submitter for the submission if not skipped
        if not event.get("skip_send_notification") == 'true':
            notification.send_notifications()

        if event.get("skip_auto_validation") == 'true':
            skip_auto_validation = True
        else:
            skip_auto_validation = False

        if AUTORUN_VALIDATION_JOBS == 'yes' and not skip_auto_validation:

            # Invoke validation manager for automation
            client_lambda = util.get_client('lambda')

            # Construct to an expected payload:
            # {
            #     "manifest_fp": "cardiac/20210711_170230/manifest.txt",
            #     "manifest_dynamodb_key_prefix": "cardiac/20210711_170230/"
            # }

            validation_payload = {
                "manifest_fp": event_record['s3']['object']['key'],
                "manifest_dynamodb_key_prefix": data.submission_prefix
            }

            if len(exception_filename) > 0:
                validation_payload['exception_postfix_filename'] = exception_filename

            if skip_checksum_validation:
                validation_payload['tasks_skipped'] = [batch.Tasks.CHECKSUM_VALIDATION.value]

            lambda_res = client_lambda.invoke(
                FunctionName=VALIDATION_MANAGER_LAMBDA_ARN,
                InvocationType='Event',
                Payload=json.dumps(validation_payload)
            )
            logger.info(f'Invoke lambda validation manager. Response:')
            logger.info(json.dumps(lambda_res, indent=4, cls=util.JsonSerialEncoder))


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
