#!/usr/bin/env python3
import json
import logging
import os
import sys

import util
import util.dynamodb as dynamodb
import util.submission_data as submission_data
import util.notification as notification
import util.s3 as s3
import util.batch as batch
import util.agha as agha

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_RESULT_TABLE_NAME')
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')

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

    # Reset notification variable (in case value cached between lambda)
    notification.MESSAGE_STORE = list()
    notification.SUBMITTER_INFO = notification.SubmitterInfo()
    dynamodb_result_update = list()

    logger.info('Processing event at validation manager lambda:')
    logger.info(json.dumps(event))

    # Parse event data and get record
    validate_event_data(event)
    data = submission_data.SubmissionData(
        bucket_name=STAGING_BUCKET
    )

    # Prepare submitter info
    notification.initialized_submitter_information(
        name=event.get('name', str()),
        email=event.get('email_address', str()),
    )

    if 'manifest_dynamodb_key_prefix' in event:
        filename_list = dynamodb.get_field_list_from_dynamodb_record(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                                     field_name='filename',
                                                                     partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                                     sort_key_prefix=event[
                                                                         'manifest_dynamodb_key_prefix'])

        non_index_file_list = [filename for filename in filename_list if not agha.FileType.is_index_file(filename)]
        event['include_fns'] = non_index_file_list

    # Get file lists and other data. Returning and assigning to be explicit.
    # NOTE(SW): FileRecords are used so that we have single interface for record creation and job
    # submission later, and are available through data.files_accepted.
    if 'manifest_fp' in event:
        notification.SUBMITTER_INFO.submission_prefix = os.path.dirname(event['manifest_fp'])
        data = handle_input_manifest(data, event)
    elif 'filepaths' in event:
        notification.SUBMITTER_INFO.submission_prefix = os.path.dirname(event['filepaths'][0])
        data = handle_input_filepaths(data, event)
    else:
        assert False

    # Process each record and prepare Batch commands
    batch_job_data = list()

    for filename in data.filename_accepted:

        # Get partition key and existing records
        sort_key = f'{data.submission_prefix}/{filename}'

        # Search for existing record
        logger.info('Grabing existing record from partition and sort key')
        manifest_response = dynamodb.get_item_from_exact_pk_and_sk(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                                   partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                                   sort_key=sort_key)
        logger.info('Manifest Record Response')
        logger.info(json.dumps(manifest_response))

        if manifest_response['Count'] != 1:
            message = f'No or more than one manifest record found for \'{sort_key}\'. Aborting!'

            notification.log_and_store_message(message, 'error')
            notification.notify_and_exit()
            return

        manifest_record_json = manifest_response["Items"][0]
        # Check to dynamodb if staging record has been validated
        if manifest_record_json['validation_status'].upper() != "PASS".upper():
            message = f"Data at '{sort_key}' has not pass manifest check. Aborting!"
            notification.log_and_store_message(message, 'error')
            notification.notify_and_exit()

        provided_checksum = manifest_record_json['provided_checksum']

        # Replace tasks with those specified by user if available
        if event.get('tasks') == None:
            tasks_list = batch.get_tasks_list()
        else:
            tasks_list = event.get('tasks')

        if event.get('tasks_skipped') != None:
            tasks_list = list(set(tasks_list) - set(event.get('tasks_skipped')))

        # Grab file size
        logger.info('Grab existing file record from partition and sort key for filesize')
        file_record_response = dynamodb.get_item_from_exact_pk_and_sk(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                                      partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
                                                                      sort_key=sort_key)
        logger.info('File Record Response File Size:')
        logger.info(json.dumps(file_record_response, indent=4, cls=util.JsonSerialEncoder))

        if file_record_response['Count'] != 1:
            message = f'No or more than one file record found for \'{sort_key}\'. Aborting!'

            notification.log_and_store_message(message, 'error')
            notification.notify_and_exit()
            return

        file_record_json = file_record_response["Items"][0]
        filesize = int(file_record_json['size_in_bytes'])

        # Create job data
        logger.info(f'Creating batch job for, s3_key:{sort_key}')
        job_data = batch.create_job_data(s3_key=sort_key, partition_key=dynamodb.ResultPartitionKey.FILE.value,
                                         checksum=provided_checksum, tasks_list=tasks_list,
                                         output_prefix=data.output_prefix, filesize=filesize)

        batch_job_data.append(job_data)

        # Create dydb record
        for task_type in tasks_list:
            # STATUS record
            partition_key = dynamodb.ResultPartitionKey.create_partition_key_with_result_prefix(
                data_type=dynamodb.ResultPartitionKey.STATUS.value,
                check_type=task_type)
            running_status = dynamodb.ResultRecord(sort_key=sort_key, partition_key=partition_key,
                                                   value=batch.StatusBatchResult.RUNNING.value)
            dynamodb_result_update.append(running_status)

    # Submit Batch jobs
    logger.info(f'Submitting job to batch. Processing {len(batch_job_data)} number of job')
    for job_data in batch_job_data:
        logger.info('Job submitted')
        logger.info(json.dumps(job_data))

        # Submit job to batch
        batch_res = batch.submit_batch_job(job_data)

        # Update DynamoDb status to running
        logger.info(json.dumps(batch_res, indent=4, cls=util.JsonSerialEncoder))

    # # Update status of dynamodb to RUNNING
    # if not event.get('skip_update_dynamodb') == 'true':
    #     dynamodb.batch_write_records(table_name=DYNAMODB_RESULT_TABLE_NAME, records=dynamodb_result_update)
    #     dynamodb.batch_write_record_archive(table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
    #                                         records=dynamodb_result_update,
    #                                         archive_log='ObjectCreated')


def validate_event_data(event_record):
    # Check for unknown arguments
    args_known = {
        'manifest_fp',
        'filepaths',
        'output_prefix',
        'include_fns',
        'exclude_fns',
        'record_mode',
        'email_address',
        'email_name',
        'tasks',
        'tasks_skipped',
        'manifest_dynamodb_key_prefix',
        'exception_postfix_filename',
        'skip_update_dynamodb',
        'skip_checksum_validation',
    }
    args_unknown = [arg for arg in event_record if arg not in args_known]
    if args_unknown:
        args_unknown_str = '\r\t'.join(args_unknown)
        logger.critical(f'got {len(args_unknown)} unknown arguments:\r\t{args_unknown_str}')
        raise ValueError

    # Only allow either manifest_fp or filepaths
    if 'manifest_fp' in event_record and 'filepaths' in event_record:
        logger.critical('\'manifest_fp\' or \'filepaths\' cannot both be provided')
        raise ValueError
    if 'manifest_dynamodb_key_prefix' in event_record and 'filepaths' in event_record:
        logger.critical('\'manifest_dynamodb_key_prefix\' or \'filepaths\' cannot both be provided')
        raise ValueError
    if not ('manifest_fp' in event_record or
            'filepaths' in event_record or
            'manifest_dynamodb_key_prefix' in event_record):
        logger.critical('either \'manifest_fp\' or \'filepaths\' must be provided')
        raise ValueError

    # If given filepaths require output directory, prohibit for manifest_fp
    if 'filepaths' in event_record and 'output_prefix' not in event_record:
        logger.critical('\'output_prefix\' must be set when providing \'filepaths\'')
        raise ValueError
    if 'manifest_fp' in event_record and 'output_prefix' in event_record:
        logger.critical('use of \'output_prefix\' is prohibited with \'manifest_fp\'')
        raise ValueError

    # TODO: ensure that manifest and filepaths are just S3 key prefixes
    # TODO: check that output_prefix is prefix only - no bucket name
    # TODO: require filepaths to have the same prefix i.e. be in the same directory

    # Only allow exclude or include at one time
    if 'exclude_fns' in event_record and 'include_fns' in event_record:
        logger.critical('you cannot specify both \'exclude_fns\' and \'include_fns\'')
        raise ValueError

    # Only allow exclude_fns/include_fns for manifest_fp input
    if 'filepaths' in event_record:
        if 'exclude_fns' in event_record:
            logger.critical('you cannot specify \'exclude_fns\' with \'filepaths\'')
            raise ValueError
        elif 'include_fns' in event_record:
            logger.critical('you cannot specify \'include_fps\' with \'filepaths\'')
            raise ValueError

        filepath_list = event_record.get('filepaths')
        if len([filename for filename in filepath_list if agha.FileType.is_index_file(filename)]) > 0:
            logger.critical('Not expecting any indexing file to be included')
            raise ValueError

    # Check tasks are valid if provided
    tasks_list = event_record.get('tasks', list())
    tasks_list.extend(event_record.get('skipped_tasks', list()))
    tasks_unknown = [task for task in tasks_list if not batch.Tasks.is_valid(task)]
    if tasks_unknown:
        tasks_str = '\r\t'.join(tasks_unknown)
        tasks_allow_str = '\', \''.join(batch.Tasks.tasks_to_list())
        logger.critical(f'expected tasks to be one of \'{tasks_allow_str}\' but got:\t\r{tasks_str}')
        raise ValueError

    # Check record mode, if not present set default
    if rm := event_record.get('record_mode'):
        if rm not in {'create', 'update'}:
            msg = f'expected \'record_mode\' as one of \'create\' or \'update\' but got \'{rm}\''
            logger.critical(msg)
            raise ValueError
    else:
        event_record['record_mode'] = 'create'

    # Set remaining defaults
    if 'exclude_fns' not in event_record:
        event_record['exclude_fns'] = list()
    if 'include_fns' not in event_record:
        event_record['include_fns'] = list()


def handle_input_manifest(data: submission_data.SubmissionData, event):
    data.manifest_key = event['manifest_fp']
    data.submission_prefix = os.path.dirname(data.manifest_key)
    data.file_metadata = s3.get_s3_object_metadata(data.bucket_name, data.submission_prefix)
    data.manifest_data = submission_data.retrieve_manifest_data(bucket_name=data.bucket_name,
                                                                manifest_key=data.manifest_key)

    if event.get('exception_postfix_filename') != None:
        exception_filename = event.get('exception_postfix_filename')
    else:
        exception_filename = []

    if event.get('skip_checksum_validation') == 'true':
        skip_checksum_validation = True
    else:
        skip_checksum_validation = False

    try:
        file_list, data.files_extra = submission_data.validate_manifest(data, exception_filename,
                                                                        skip_checksum_check=skip_checksum_validation)
    except ValueError as e:
        logger.error(f'Incorrect/Wrong manifest file: {str(e)}')
        logger.error(f'Terminating')
        raise ValueError

    logger.info(f'File list to process:')
    logger.info(json.dumps(file_list, indent=4, cls=util.JsonSerialEncoder))

    files_included, data.files_rejected = filter_filelist(
        file_list,
        event['include_fns'],
        event['exclude_fns']
    )

    # Create file records
    # data.output_prefix = s3.get_output_prefix(data.submission_prefix)
    data.output_prefix = data.submission_prefix

    data.filename_accepted = files_included

    return data


def handle_input_filepaths(data: submission_data.SubmissionData, event):
    # Populate submission data from event
    data.submission_prefix = os.path.dirname(event['filepaths'][0])
    data.output_prefix = data.submission_prefix

    missing_files = list()

    for filepath in event['filepaths']:

        if not (file_mdata_list := s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET,
                                                             directory_prefix=data.submission_prefix)):
            missing_files.append(filepath)
        else:
            assert len(file_mdata_list)
            data.file_metadata.extend(file_mdata_list)

    if missing_files:
        plurality = 'files' if len(missing_files) > 1 else 'file'
        files_str = '\r\t'.join(missing_files)
        message_base = f'could not find {len(missing_files)} {plurality} in \'{STAGING_BUCKET}\''
        notification.log_and_store_message(f'{message_base}:\r\t{files_str}', level='critical')
        notification.notify_and_exit()

    data.filename_accepted = [os.path.basename(filepath) for filepath in event['filepaths']]

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
        log_matched_filename_filters(accepted, include_fns, 'include')
    if exclude_fns:
        log_matched_filename_filters(excluded, exclude_fns, 'exclude')
    if accepted:
        if include_fns or exclude_fns:
            plurality = 'files' if len(accepted) > 1 else 'file'
            filenames_str = '\r\t'.join(accepted)
            logger.info(f'{len(accepted)}/{len(file_list)} {plurality} passed filtering:\r\t{filenames_str}')
        else:
            logger.info(f'no file include/exclude lists provided, skipping file filtering')
    else:
        logger.critical(f'no files remaining after filtering')
        raise Exception
    return accepted, excluded


def log_matched_filename_filters(files_found, filter_list, filter_type):
    # Emit matched files
    filenames_str = '\r\t'.join(files_found)
    logger.info(f'{filter_type}d {len(files_found)}/{len(filter_list)} files:\r\t{filenames_str}')
    # Check for missing files
    filenames_missing = set(filter_list).difference(files_found)
    if filenames_missing:
        filenames_str = '\r\t'.join(filenames_missing)
        message_base = f'did not find all files in {filter_type} list, missing'
        logger.warning(f'{message_base}:\r\t{filenames_str}')
