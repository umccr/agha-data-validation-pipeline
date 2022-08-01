# !/usr/bin/env python3
import json
import logging
import os
import pandas as pd

# From layers
import sys

import util
from util import dynamodb, submission_data, notification, s3, agha, batch

DYNAMODB_STAGING_TABLE_NAME = os.environ.get("DYNAMODB_STAGING_TABLE_NAME")
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    "DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"
)
DYNAMODB_ETAG_TABLE_NAME = os.environ.get("DYNAMODB_ETAG_TABLE_NAME")
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
VALIDATION_MANAGER_LAMBDA_ARN = os.environ.get("VALIDATION_MANAGER_LAMBDA_ARN")
FOLDER_LOCK_LAMBDA_ARN = os.environ.get("FOLDER_LOCK_LAMBDA_ARN")
AUTORUN_VALIDATION_JOBS = os.environ.get("AUTORUN_VALIDATION_JOBS")

# NOTE(SW): it seems reasonable to require some structuring of uploads in the format of
# <FLAGSHIP>/<DATE_TS>/<FILES ...>. Outcomes on upload wrt directory structure:
#   1. meets prescribed structure, and we automatically launch validation jobs
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

    What this lambda does:
    - check validity of the manifest
    - add Checksum from manifest to dynamodb
    - add StudyID from manifest to dynamodb
    - Raise an error and unlock bucket if manifest check failed / any duplicate file

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
        "skip_duplication_check": "true",
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

    ####################################################
    # Validate event
    ####################################################

    # If trigger manually, construct the same s3 format
    if event.get("manifest_fp") is not None:
        event["Records"] = [
            {
                "s3": {
                    "bucket": {
                        "name": event.get("bucket_name"),
                    },
                    "object": {"key": event.get("manifest_fp")},
                },
                "email_report_to": event.get("email_report_to"),
            }
        ]

    validate_manual_trigger_payload(event)

    if event.get("exception_postfix_filename") is not None:
        exception_filename = event.get("exception_postfix_filename")
    else:
        exception_filename = []

    # To check checksum validation
    if event.get("skip_checksum_validation"):
        skip_checksum_validation = True
    else:
        skip_checksum_validation = False

    # Triggering validation lambda options
    if event.get("skip_auto_validation"):
        skip_auto_validation = True
    else:
        skip_auto_validation = False

    # Triggering validation lambda options
    if event.get("skip_duplication_check"):
        skip_duplication_check = True
    else:
        skip_duplication_check = False

    s3_records = event.get("Records")
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
        data = (
            submission_data.SubmissionData.create_submission_data_object_from_s3_event(
                event_record
            )
        )

        # Set submitter information
        notification.set_submitter_information_from_s3_event(event_record)

        # Pull file metadata from S3
        data.file_metadata = s3.get_s3_object_metadata(
            data.bucket_name, data.submission_prefix
        )
        logger.info(
            f"File metadata in the current s3 content: {json.dumps(data.file_metadata, indent=4, cls=util.JsonSerialEncoder)}"
        )

        # Collect manifest data and then validate
        data.manifest_data = submission_data.retrieve_manifest_data(
            data.bucket_name, data.manifest_s3_key
        )
        logger.info(
            f"Current manifest filename data: {json.dumps(data.manifest_data['filename'].tolist(), indent=4)}"
        )

        ####################################################
        # Validate Manifest
        ####################################################
        try:
            file_list, data.files_extra = submission_data.validate_manifest(
                data, exception_filename, skip_checksum_check=skip_checksum_validation
            )
        except ValueError as e:
            # Update DynamoDb regarding manifest checks status
            manifest_status_record = dynamodb.ManifestStatusCheckRecord(
                sort_key=data.manifest_s3_key,
                status=dynamodb.ManifestStatusCheckValue.FAIL.value,
                additional_information=json.loads(str(e)),
            )

            dynamodb.write_record_from_class(
                DYNAMODB_STAGING_TABLE_NAME, manifest_status_record
            )
            manifest_status_record_archive = (
                manifest_status_record.create_archive_dictionary("ObjectCreated/Update")
            )
            dynamodb.write_record_from_dict(
                DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, manifest_status_record_archive
            )

            notification.MESSAGE_STORE.append("")  # Appending empty line
            notification.log_and_store_message(
                f"Trigger of validation pipeline has been disabled. Please check the above error.",
                "critical",
            )

            # UNLOCK submission in staging bucket to be fixed
            payload = {
                "task": "FOLDER_UNLOCK",
                "submission_prefix": data.manifest_s3_key,
            }
            util.call_lambda(FOLDER_LOCK_LAMBDA_ARN, payload)
            notification.log_and_store_message(f"Unlocking submission directory.")

            notification.notify_and_exit()
            raise ValueError(e)

        # Create status pass when no error raised
        manifest_status_record = dynamodb.ManifestStatusCheckRecord(
            sort_key=data.manifest_s3_key,
            status=dynamodb.ManifestStatusCheckValue.PASS.value,
        )

        logger.info(
            f"Processing {len(file_list)} number of file_list, and {len(data.files_extra)} \
                    number of files_extra"
        )

        # Grab dynamodb FILE record
        staging_file_record = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STAGING_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
            sort_key_prefix=data.submission_prefix,
        )
        staging_file_record_df = pd.json_normalize(staging_file_record)

        for filename in file_list:

            sort_key = f"{data.submission_prefix}/{filename}"

            # Variables from manifest data
            agha_study_id = submission_data.find_study_id_from_manifest_df_and_filename(
                data.manifest_data, filename
            )
            provided_checksum = (
                submission_data.find_checksum_from_manifest_df_and_filename(
                    data.manifest_data, filename
                )
            )
            logger.debug(f"Variables extracted from manifest file for '{filename}'.")
            logger.debug(
                f"AGHA_STUDY_ID:{agha_study_id}, PROVIDED_CHECKSUM:{provided_checksum}"
            )

            # Search if file exist at s3
            query_submission_df = staging_file_record_df.loc[
                (staging_file_record_df["sort_key"] == sort_key)
            ]

            if len(query_submission_df) == 0:
                notification.log_and_store_message(
                    f"No such file found at bucket:{DYNAMODB_STAGING_TABLE_NAME}\
                 s3_key:{sort_key}",
                    "warning",
                )

                # UNLOCK submission in staging bucket to be fixed
                payload = {
                    "task": "FOLDER_UNLOCK",
                    "submission_prefix": data.manifest_s3_key,
                }
                util.call_lambda(FOLDER_LOCK_LAMBDA_ARN, payload)
                notification.log_and_store_message(f"Unlocking submission directory.")

                notification.notify_and_exit()
            logger.debug(f"File check '{sort_key}' in s3 bucket: OK.")

            file_etag = query_submission_df["etag"].values[0]

            # Check if the file eTag has appear else than this staging bucket and warn if so.
            logger.info("Check if the same Etag has exist in the database")
            etag_response = dynamodb.get_item_from_pk(
                DYNAMODB_ETAG_TABLE_NAME, file_etag
            )
            logger.debug("eTag query response:")
            logger.debug(
                json.dumps(etag_response, indent=4, cls=util.JsonSerialEncoder)
            )

            if etag_response["Count"] > 1:
                s3_duplicate_list = []
                for each_etag_appearance in etag_response["Items"]:
                    # Parsing...
                    s3_key = each_etag_appearance["s3_key"]
                    bucket_name = each_etag_appearance["bucket_name"]

                    s3_uri = s3.create_s3_uri_from_bucket_name_and_key(
                        bucket_name, s3_key
                    )
                    s3_duplicate_list.append(s3_uri)

                duplicate_etag_list.append(s3_duplicate_list)

            # Create Manifest type record
            manifest_record = dynamodb.ManifestFileRecord(
                partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                sort_key=sort_key,
                flagship=agha.FlagShip.from_name(
                    sort_key.split("/")[0]
                ).preferred_code(),
                filename=filename,
                filetype=agha.FileType.from_name(filename).get_name(),
                submission=data.submission_prefix,
                date_modified=util.get_datetimestamp(),
                provided_checksum=provided_checksum,
                agha_study_id=agha_study_id,
                is_in_manifest="True",
                validation_status="PASS",
            )

            # Update item at the record
            staging_dynamodb_batch_write_list.append(manifest_record.__dict__)
            archive_manifest_record = dynamodb.ArchiveManifestFileRecord.create_archive_manifest_record_from_manifest_record(
                manifest_record, "CREATE"
            )
            archive_staging_dynamodb_batch_write_list.append(
                archive_manifest_record.__dict__
            )

        # Construct to an expected payload:
        # {
        #     "manifest_fp": "cardiac/20210711_170230/manifest.txt",
        #     "manifest_dynamodb_key_prefix": "cardiac/20210711_170230/"
        # }

        ####################################################
        # Exception payload
        ####################################################

        # Construct payload
        validation_payload = {
            "manifest_fp": event_record["s3"]["object"]["key"],
            "manifest_dynamodb_key_prefix": data.submission_prefix,
        }
        if len(exception_filename) > 0:
            validation_payload["exception_postfix_filename"] = exception_filename
        if skip_checksum_validation:
            validation_payload["tasks_skipped"] = [
                batch.Tasks.CHECKSUM_VALIDATION.value
            ]

        number_of_unrecognized_file = len(data.files_extra)
        notification.log_and_store_message(
            f"Number of unrecognized filetype: {number_of_unrecognized_file}"
        )
        if len(data.files_extra) > 0:
            report_info = {
                "topic": "UNSUPPORTED extension file type found.",
                "data": data.files_extra,
            }
            if isinstance(manifest_status_record.additional_information, list):
                manifest_status_record.additional_information.append(report_info)
            else:
                manifest_status_record.additional_information = [report_info]

            notification.MESSAGE_STORE.append("")  # Appending empty line
            notification.log_and_store_message(
                "Trigger of validation pipeline has been disabled due to UNRECOGNIZED filetype found.",
                "critical",
            )
            notification.log_and_store_message(
                f"Please check/resubmit submitted file.", "critical"
            )
            notification.log_and_store_message(
                f"To proceed with this submission, please contact the GDR administrator or data manger.",
                "critical",
            )

            # List all duplicates file
            notification.MESSAGE_STORE.append("")  # Appending empty line
            notification.log_and_store_message(
                "The following list are files of unrecognized file."
            )
            list_of_duplicate_files_email_format = json.dumps(
                data.files_extra, indent=4, sort_keys=True
            )
            notification.log_and_store_message(list_of_duplicate_files_email_format)
            notification.MESSAGE_STORE.append("")  # Appending empty line
            # Skip the auto validation
            skip_auto_validation = True

        # Detecting duplicate payload
        number_of_duplicates = len(duplicate_etag_list)
        notification.log_and_store_message(
            f"<br>Number of duplicates file found in this submission with other submissions: {number_of_duplicates}"
        )

        ####################################################
        # Check for duplication
        ####################################################
        if skip_duplication_check:
            logger.info("Skipping duplication check")
            pass
        else:
            if number_of_duplicates > 0:
                report_info = {
                    "topic": "Duplicate files found in this submission with other submissions",
                    "data": duplicate_etag_list,
                }

                if isinstance(manifest_status_record.additional_information, list):
                    manifest_status_record.additional_information.append(report_info)
                else:
                    manifest_status_record.additional_information = [report_info]

                critical_message = (
                    "Trigger of validation pipeline has been disabled due to duplicate submission has been found."
                    + "\n"
                    + "Please check/resubmit submitted file to prevent duplication"
                    + "\n"
                    + "If the duplication file is intended,please contact the data manager for further actions."
                )

                notification.log_and_store_message(
                    critical_message,
                    "critical",
                )
                notification.log_and_store_message(
                    f"Please check/resubmit submitted file to prevent duplication",
                    "critical",
                )

                # List all duplicates file
                notification.log_and_store_message(
                    "The following list are files with the same eTag at multiple location.<br>"
                )
                list_of_duplicate_files_email_format = json.dumps(
                    duplicate_etag_list, indent=4, sort_keys=True
                )
                notification.log_and_store_message(list_of_duplicate_files_email_format)

                # UNLOCK submission in staging bucket to be fixed
                payload = {
                    "task": "FOLDER_UNLOCK",
                    "submission_prefix": data.manifest_s3_key,
                }
                util.call_lambda(FOLDER_LOCK_LAMBDA_ARN, payload)
                notification.log_and_store_message(f"Unlocking submission directory.")

                notification.notify_and_exit()
                raise ValueError("Duplication found. Terminating...")

        ####################################################
        # Update DynamoDb data
        ####################################################
        staging_dynamodb_batch_write_list.append(manifest_status_record.__dict__)
        archive_staging_dynamodb_batch_write_list.append(
            manifest_status_record.create_archive_dictionary("Create/Update")
        )

        # Update dynamodb batch if not skipped
        if not event.get("skip_update_dynamodb"):
            # Clear previous result if any (will happen if manifest get re-upload)
            exiting_manifest_record = dynamodb.get_batch_item_from_pk_and_sk(
                table_name=DYNAMODB_STAGING_TABLE_NAME,
                partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                sort_key_prefix=data.submission_prefix,
            )
            dynamodb.batch_delete_from_dictionary(
                table_name=DYNAMODB_STAGING_TABLE_NAME,
                dictionary_list=exiting_manifest_record,
            )
            dynamodb.batch_write_objects_archive(
                table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                object_list=archive_staging_dynamodb_batch_write_list,
                archive_log="ObjectRemoved",
            )

            # Append new result
            dynamodb.batch_write_objects(
                table_name=DYNAMODB_STAGING_TABLE_NAME,
                object_list=staging_dynamodb_batch_write_list,
            )
            dynamodb.batch_write_objects_archive(
                table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
                object_list=archive_staging_dynamodb_batch_write_list,
                archive_log="ObjectCreated",
            )
        else:
            logger.info(f"'skip_update_dynamodb' payload is True. Skipping ...")

        ####################################################
        # Triggering other lambda
        ####################################################
        if AUTORUN_VALIDATION_JOBS == "yes" and not skip_auto_validation:

            notification.MESSAGE_STORE.append("")
            notification.log_and_store_message("Continuing with file validation.")

            # Invoke validation manager for automation
            client_lambda = util.get_client("lambda")

            lambda_res = client_lambda.invoke(
                FunctionName=VALIDATION_MANAGER_LAMBDA_ARN,
                InvocationType="Event",
                Payload=json.dumps(validation_payload),
            )
            print(lambda_res)
        else:
            notification.log_and_store_message(
                "Validation pipeline for this submission is disabled."
            )

        # Send notification to submitter for the submission if not skipped
        if not event.get("skip_send_notification"):
            notification.send_notifications()
        else:
            logger.info(f"'skip_send_notification' payload is True. Skipping ...")


def validate_event_data(event_record):
    if "s3" not in event_record:
        logger.critical("no 's3' entry found in record")
        raise ValueError
    record_s3 = event_record["s3"]

    if "bucket" not in record_s3:
        logger.critical("S3 record missing bucket info")
        raise ValueError
    elif "name" not in record_s3["bucket"]:
        logger.critical("S3 bucket record missing name info")
        raise ValueError

    if "object" not in record_s3:
        logger.critical("S3 record missing object info")
        raise ValueError
    elif "key" not in record_s3["object"]:
        logger.critical("S3 object record missing key info")
        raise ValueError

    if record_s3["bucket"]["name"] != STAGING_BUCKET:
        logger.critical(
            f'expected {STAGING_BUCKET} bucket but got {record_s3["bucket"]["name"]}'
        )
        raise ValueError


def validate_manual_trigger_payload(event_payload: dict) -> None:
    # Sanitize to string of bool to bool()
    for args in [
        "skip_auto_validation",
        "skip_update_dynamodb",
        "skip_send_notification",
        "skip_checksum_validation",
        "skip_duplication_check",
        "exception_postfix_filename",
    ]:

        if (bool_payload := event_payload.get(args)) is not None:
            if isinstance(bool_payload, str):
                try:
                    event_payload[args] = json.loads(bool_payload.lower())
                except json.JSONDecodeError:
                    raise ValueError(f"'{args}' has an invalid boolean")

            event_payload[
                "skip_auto_validation"
            ] = True  # Any skipping would disable auto validation trigger

    # Check if exception_postfix_filename is a list
    if (list_args := event_payload.get("exception_postfix_filename")) is not None:
        if not isinstance(list_args, list):
            raise ValueError(f"'exception_postfix_filename' is not a list")
