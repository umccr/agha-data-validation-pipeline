#!/usr/bin/env python3
import json
import logging
import os
import time
import pandas as pd

import util
import util.dynamodb as dynamodb
import util.submission_data as submission_data
import util.notification as notification
import util.s3 as s3
import util.batch as batch
import util.agha as agha

DYNAMODB_STAGING_TABLE_NAME = os.environ.get("DYNAMODB_STAGING_TABLE_NAME")
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    "DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"
)
DYNAMODB_RESULT_TABLE_NAME = os.environ.get("DYNAMODB_RESULT_TABLE_NAME")
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = os.environ.get(
    "DYNAMODB_ARCHIVE_RESULT_TABLE_NAME"
)
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
RESULTS_BUCKET = os.environ.get("RESULTS_BUCKET")

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to invoke batch job with given event.

    There are 2 event structure to invoke this function, as follows.

    Structure #1:
    {
      "manifest_fp": "cardiac/2019-09-11/manifest.txt",
      "include_fns": [
        "180316_NB501544_0318_ML180402_18W000101_STAR-20180228_SSQXTCRE.merge.dedup.realign.recal.bam"
      ]
    }

    Structure #2:
    {
      "filepaths": [
        "cardiac/20210711_170230/20210824_051333_0755318/19W001053.bam__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001053.individual.norm.vcf.gz__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001056.bam__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001056.individual.norm.vcf.gz__results.json"
      ],
      "output_prefix": "cardiac/20210711_170230/20210824_manual_run/"
    }

    Structure #3 (Use dynamodb manifest prefix for filepath):
    {
      "manifest_fp": "cardiac/20210711_170230/manifest.txt",
      "manifest_dynamodb_key_prefix": "cardiac/20210711_170230/"
    }

    Additional exception from manifest file
    {
        "exception_postfix_filename": ["metadata.txt", ".md5", etc.]
        "skip_update_dynamodb": "true",
        "tasks_skipped":['CHECKSUM_VALIDATION'],
    }

    :param event: payload to process and run batchjob
    :param context: not used
    """

    dynamodb_result_update = list()

    logger.info("Processing event at validation manager lambda:")
    logger.info(json.dumps(event, indent=4))

    # Parse event data and get record
    validate_event_data(event)
    data = submission_data.SubmissionData(bucket_name=STAGING_BUCKET)

    manifest_record_dynamodb_staging = None
    manifest_record_dynamodb_df = None

    manifest_fp = event.get("manifest_fp")
    if manifest_fp is not None and "manifest_dynamodb_key_prefix" in event:
        submission_prefix = os.path.dirname(event["manifest_fp"])
        manifest_record_dynamodb_staging = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STAGING_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
            sort_key_prefix=submission_prefix,
        )
        manifest_record_dynamodb_df = pd.json_normalize(
            manifest_record_dynamodb_staging
        )

        # Find filename included list
        filename_list = manifest_record_dynamodb_df["filename"].tolist()
        non_index_file_list = [
            filename
            for filename in filename_list
            if not agha.FileType.is_index_file(filename)
        ]
        event["include_fns"] = non_index_file_list

    if "manifest_fp" in event:
        data = handle_input_manifest(data, event, event["include_fns"])
    elif "filepaths" in event:
        data = handle_input_filepaths(data, event)
    else:
        assert False

    ################################################################################
    # Process each record and prepare Batch commands
    batch_job_data = list()

    file_record_dynamodb_staging = dynamodb.get_batch_item_from_pk_and_sk(
        table_name=DYNAMODB_STAGING_TABLE_NAME,
        partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
        sort_key_prefix=data.submission_prefix,
    )
    file_record_dynamodb_df = pd.json_normalize(file_record_dynamodb_staging)

    # Possibility of being fetched at above line, having if-condition to prevent re-fetch
    if manifest_record_dynamodb_staging is None:
        manifest_record_dynamodb_staging = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STAGING_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
            sort_key_prefix=data.submission_prefix,
        )
        manifest_record_dynamodb_df = pd.json_normalize(
            manifest_record_dynamodb_staging
        )

    for filename in data.filename_accepted:

        # Get partition key and existing records
        sort_key = f"{data.submission_prefix}/{filename}"

        # Skipping exception validation defined in payload
        if event.get("exception_postfix_filename") is not None:
            postfix_exception_list = event.get("exception_postfix_filename")
            if (
                len(
                    [
                        filename
                        for postfix in postfix_exception_list
                        if filename.endswith(postfix)
                    ]
                )
                > 0
            ):
                continue

        # Find checksum file from file record
        try:
            manifest_record = util.get_record_from_given_field_and_panda_df(
                panda_df=manifest_record_dynamodb_df,
                fieldname_lookup="sort_key",
                fieldvalue_lookup=sort_key,
            )
            provided_checksum = manifest_record["provided_checksum"]
        except:
            message = (
                f"No or more than one manifest record found for '{sort_key}'. Aborting!"
            )
            logger.error(message)
            raise Exception

        # Replace tasks with those specified by user if available
        if event.get("tasks") is None:
            tasks_list = batch.get_tasks_list(filename)
        else:
            tasks_list = event.get("tasks")

        if event.get("tasks_skipped") is not None:
            tasks_list = list(set(tasks_list) - set(event.get("tasks_skipped")))

        # Find size file from file record
        try:
            file_record = util.get_record_from_given_field_and_panda_df(
                panda_df=file_record_dynamodb_df,
                fieldname_lookup="sort_key",
                fieldvalue_lookup=sort_key,
            )
            filesize = int(file_record["size_in_bytes"])
        except:
            message = (
                f"No or more than one file record found for '{sort_key}'. Aborting!"
            )
            logger.error(message)
            raise Exception

        # Create job data
        logger.debug(f"Creating batch job for, s3_key:{sort_key}")
        job_data = batch.create_job_data(
            s3_key=sort_key,
            partition_key=dynamodb.ResultPartitionKey.FILE.value,
            checksum=provided_checksum,
            tasks_list=tasks_list,
            output_prefix=data.output_prefix,
            filesize=filesize,
        )

        batch_job_data.append(job_data)

        # Create dydb record
        for task_type in tasks_list:
            # STATUS record
            partition_key = (
                dynamodb.ResultPartitionKey.create_partition_key_with_result_prefix(
                    data_type=dynamodb.ResultPartitionKey.STATUS.value,
                    check_type=task_type,
                )
            )
            running_status = dynamodb.ResultRecord(
                sort_key=sort_key,
                partition_key=partition_key,
                value=batch.StatusBatchResult.RUNNING.value,
            )
            dynamodb_result_update.append(running_status)

    # Clear result objects in the Result bucket if exists. (To prevent mix results between old and new validations)
    logger.info(
        "Check results bucket if any object need to be deleted to prevent validation result overlap."
    )
    try:
        existing_object_metadata_results = s3.get_s3_object_metadata(
            bucket_name=RESULTS_BUCKET, directory_prefix=f"{data.submission_prefix}/"
        )
        list_of_keys = [
            metadata["Key"] for metadata in existing_object_metadata_results
        ]
        logger.info(
            f"List of keys to be deleted in results bucket: {json.dumps(list_of_keys, indent=4, cls=util.JsonSerialEncoder)}"
        )
        s3.delete_s3_object_from_key(bucket_name=RESULTS_BUCKET, key_list=list_of_keys)
    except ValueError:
        logger.info(f"No existing results found in '{RESULTS_BUCKET}' bucket.")

    # Update status of dynamodb to RUNNING (To flush dydb if previous result is in dynamodb)
    # Executed before batch submitted, in case submitting batch from lambda takes time and the first job submitted
    # has completed, it will override with RUNNING status
    if not event.get("skip_update_dynamodb") == "true":
        dynamodb.batch_write_records(
            table_name=DYNAMODB_RESULT_TABLE_NAME, records=dynamodb_result_update
        )
        dynamodb.batch_write_record_archive(
            table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
            records=dynamodb_result_update,
            archive_log="ObjectCreated",
        )

    # Submit Batch jobs
    logger.info(
        f"Submitting batch job to queue. batch job data list ({len(batch_job_data)}):"
    )
    logger.info(json.dumps(batch_job_data))
    for i, job_data in enumerate(batch_job_data):
        # Submit job to batch
        batch_res = batch.submit_batch_job(job_data)

        # Sleep for 1 second every 10th job
        # This is to prevent AWS Batch SubmitJob throttling limit of 50 jobs per second
        # Putting 10 here as it is shared across 3 reserved concurrency limit, and
        # 2 lambda function (transfer-manager and validation-manager)
        # Without sleep, the average of submitting jobs is about 13 jobs per second
        # https://docs.aws.amazon.com/batch/latest/userguide/service_limits.html
        if (i + 1) % 10 == 0:
            time.sleep(1)

    logger.info("Batch job has been submitted.")


################################################################
# Helper function
################################################################


def validate_event_data(event_record):
    # Check for unknown arguments
    args_known = {
        "manifest_fp",
        "filepaths",
        "output_prefix",
        "include_fns",
        "exclude_fns",
        "tasks",
        "tasks_skipped",
        "manifest_dynamodb_key_prefix",
        "exception_postfix_filename",
        "skip_update_dynamodb",
    }

    args_unknown = [arg for arg in event_record if arg not in args_known]
    if args_unknown:
        args_unknown_str = "\r\t".join(args_unknown)
        logger.critical(
            f"got {len(args_unknown)} unknown arguments:\r\t{args_unknown_str}"
        )
        raise ValueError

    # Check if 'exception_postfix_filename' is a list
    if "exception_postfix_filename" in event_record and not isinstance(
        event_record.get("exception_postfix_filename"), list
    ):
        logger.critical("'exception_postfix_filename' must be a list")
        raise ValueError

    # Only allow either manifest_fp or filepaths
    if "manifest_fp" in event_record and "filepaths" in event_record:
        logger.critical("'manifest_fp' or 'filepaths' cannot both be provided")
        raise ValueError
    if "manifest_dynamodb_key_prefix" in event_record and "filepaths" in event_record:
        logger.critical(
            "'manifest_dynamodb_key_prefix' or 'filepaths' cannot both be provided"
        )
        raise ValueError
    if not (
        "manifest_fp" in event_record
        or "filepaths" in event_record
        or "manifest_dynamodb_key_prefix" in event_record
    ):
        logger.critical("either 'manifest_fp' or 'filepaths' must be provided")
        raise ValueError

    # If given filepaths require output directory, prohibit for manifest_fp
    if "filepaths" in event_record and "output_prefix" not in event_record:
        logger.critical("'output_prefix' must be set when providing 'filepaths'")
        raise ValueError
    if "manifest_fp" in event_record and "output_prefix" in event_record:
        logger.critical("use of 'output_prefix' is prohibited with 'manifest_fp'")
        raise ValueError

    # TODO: ensure that manifest and filepaths are just S3 key prefixes
    # TODO: check that output_prefix is prefix only - no bucket name
    # TODO: require filepaths to have the same prefix i.e. be in the same directory

    # Only allow to exclude or include at one time
    if "exclude_fns" in event_record and "include_fns" in event_record:
        logger.critical("you cannot specify both 'exclude_fns' and 'include_fns'")
        raise ValueError

    # Only allow exclude_fns/include_fns for manifest_fp input
    if "filepaths" in event_record:
        if "exclude_fns" in event_record:
            logger.critical("you cannot specify 'exclude_fns' with 'filepaths'")
            raise ValueError
        elif "include_fns" in event_record:
            logger.critical("you cannot specify 'include_fps' with 'filepaths'")
            raise ValueError

        filepath_list = event_record.get("filepaths")
        if (
            len(
                [
                    filename
                    for filename in filepath_list
                    if agha.FileType.is_index_file(filename)
                ]
            )
            > 0
        ):
            logger.critical("Not expecting any indexing file to be included")
            raise ValueError

    # Check tasks are valid if provided
    tasks_list = event_record.get("tasks", list())
    tasks_list.extend(event_record.get("skipped_tasks", list()))
    tasks_unknown = [task for task in tasks_list if not batch.Tasks.is_valid(task)]
    if tasks_unknown:
        tasks_str = "\r\t".join(tasks_unknown)
        tasks_allow_str = "', '".join(batch.Tasks.tasks_to_list())
        logger.critical(
            f"expected tasks to be one of '{tasks_allow_str}' but got:\t\r{tasks_str}"
        )
        raise ValueError

    # Set remaining defaults
    if "exclude_fns" not in event_record:
        event_record["exclude_fns"] = list()
    if "include_fns" not in event_record:
        event_record["include_fns"] = list()

    # Sanitize to string of bool to bool()
    for args in ["skip_update_dynamodb"]:
        if (bool_payload := event_record.get(args)) is not None:
            if isinstance(bool_payload, str):
                try:
                    event_record[args] = json.loads(bool_payload.lower())
                except json.JSONDecodeError:
                    raise ValueError(f"'{args}' has an invalid boolean")


def handle_input_manifest(data: submission_data.SubmissionData, event, file_list):
    data.manifest_key = event["manifest_fp"]
    data.submission_prefix = os.path.dirname(data.manifest_key)

    files_included, data.files_rejected = filter_filelist(
        file_list, event["include_fns"], event["exclude_fns"]
    )

    logger.info(f"File list to process:")
    logger.info(json.dumps(files_included, indent=4, cls=util.JsonSerialEncoder))

    # Create file records
    # data.output_prefix = s3.get_output_prefix(data.submission_prefix)
    data.output_prefix = data.submission_prefix

    data.filename_accepted = files_included

    return data


def handle_input_filepaths(data: submission_data.SubmissionData, event):
    # Populate submission data from event
    data.submission_prefix = os.path.dirname(event["filepaths"][0])
    data.output_prefix = data.submission_prefix

    missing_files = list()

    for filepath in event["filepaths"]:

        if not (
            file_mdata_list := s3.get_s3_object_metadata(
                bucket_name=STAGING_BUCKET, directory_prefix=data.submission_prefix
            )
        ):
            missing_files.append(filepath)
        else:
            assert len(file_mdata_list)
            data.file_metadata.extend(file_mdata_list)

    if missing_files:
        plurality = "files" if len(missing_files) > 1 else "file"
        files_str = "\r\t".join(missing_files)
        message_base = (
            f"could not find {len(missing_files)} {plurality} in '{STAGING_BUCKET}'"
        )
        logger.critical(f"{message_base}:\r\t{files_str}")
        raise Exception

    data.filename_accepted = [
        os.path.basename(filepath) for filepath in event["filepaths"]
    ]

    return data


def filter_filelist(file_list, include_fns, exclude_fns):
    accepted = list()
    excluded = list()
    for filepath in file_list:
        # Prior checks to disallow 'exclude_fns' and 'include_fns' being set at once - processing
        # is simplified with this assumption here
        if include_fns:
            if filepath in include_fns:
                accepted.append(filepath)
            else:
                excluded.append(filepath)
        elif exclude_fns:
            if filepath in exclude_fns:
                excluded.append(filepath)
            else:
                accepted.append(filepath)
        else:
            accepted.append(filepath)
    # Log matched filename filters
    if include_fns:
        log_matched_filename_filters(accepted, include_fns, "include")
    if exclude_fns:
        log_matched_filename_filters(excluded, exclude_fns, "exclude")
    if accepted:
        if include_fns or exclude_fns:
            plurality = "files" if len(accepted) > 1 else "file"
            filenames_str = "\r\t".join(accepted)
            logger.info(
                f"{len(accepted)}/{len(file_list)} {plurality} passed filtering:\r\t{filenames_str}"
            )
        else:
            logger.info(
                f"no file include/exclude lists provided, skipping file filtering"
            )
    else:
        logger.critical(f"no files remaining after filtering")
        raise Exception
    return accepted, excluded


def log_matched_filename_filters(files_found, filter_list, filter_type):
    # Emit matched files
    filenames_str = "\r\t".join(files_found)
    logger.info(
        f"{filter_type}d {len(files_found)}/{len(filter_list)} files:\r\t{filenames_str}"
    )
    # Check for missing files
    filenames_missing = set(filter_list).difference(files_found)
    if filenames_missing:
        filenames_str = "\r\t".join(filenames_missing)
        message_base = f"did not find all files in {filter_type} list, missing"
        logger.warning(f"{message_base}:\r\t{filenames_str}")
