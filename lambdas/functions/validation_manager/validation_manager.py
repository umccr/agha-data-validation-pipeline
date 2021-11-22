#!/usr/bin/env python3
import json
import logging
import os
import re

import util
import util.dynamodb as dynamodb
import util.submission_data as submission_data
import util.notification as notification
import util.agha as agha
import util.s3 as s3
import util.batch as batch

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')


#TODO: Remove the following block (for dev purpose only)
from lambdas.layers.util.util import dynamodb, submission_data
from lambdas.layers.util.util import notification
from lambdas.layers.util.util import agha
from lambdas.layers.util.util import s3
from lambdas.layers.util.util import batch
import lambdas.layers.util.util

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to invoke batch job with given event.

    There are 2 event structure to invoke this function, as follows.

    Structure #1:
    {
      "manifest_fp": "cardiac/20210711_170230/manifest.txt",
      "include_fns": [
        "19W001053.bam",
        "19W001053.individual.norm.vcf.gz",
        "19W001056.bam",
        "19W001056.individual.norm.vcf.gz"
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

    :param event: payload to process and run batchjob
    :param context: not used
    """

    # Parse event data and get record
    validate_event_data(event)
    data = submission_data.SubmissionData(
        bucket_name=STAGING_BUCKET,
    )

    # Prepare submitter info
    submitter_info = notification.SubmitterInfo()
    submitter_info.name = event.get('name', str())
    submitter_info.email = event.get('email_address', str())

    # Get file lists and other data. Returning and assigning to be explicit.
    # NOTE(SW): FileRecords are used so that we have single interface for record creation and job
    # submission later, and are available through data.files_accepted.
    if 'manifest_fp' in event:
        data = handle_input_manifest(data, event, event['strict_mode'])
    elif 'filepaths' in event:
        data = handle_input_filepaths(data)
    else:
        assert False

    # Process each record and prepare Batch commands
    batch_job_data = list()

    for filename in data.filename_accepted:
        
        dynamodb_status = "UPDATE"

        # Get partition key and existing records
        partition_key = f'{data.submission_prefix}/{filename}'

        # Search for existing record
        try:
            file_record = dynamodb.get_record_from_s3_key(DYNAMODB_STAGING_TABLE_NAME, partition_key)
            dynamodb_status = "UPDATE"

        except ValueError as e:

            logger.warn(e)
            logger.info(f'Create a new record for {partition_key}')
            file_record = dynamodb.BucketFileRecord.from_manifest_record(data)
            dynamodb_status = "CREATE"

        finally:
        
            agha_study_id = submission_data.find_study_id_from_manifest_df_and_filename(data.manifest_data, filename)
            provided_checksum = submission_data.find_checksum_from_manifest_df_and_filename(data.manifest_data, filename)

            # Get partition key and existing records, and set sort key
            message_base = f'found existing records for {filename} with key {partition_key}'
            logger.info(f'{message_base}: {json.dumps(file_record)}')

            logger.info(f'Updating record with manifest data')
            file_record.date_modified = util.get_datetimestamp()
            file_record.agha_study_id = agha_study_id
            file_record.provided_checksum = provided_checksum
            file_record.is_in_manifest = "True"
            file_record.is_validated = "True"

        # Update item at the record
        dynamodb.write_record(DYNAMODB_STAGING_TABLE_NAME, file_record)

        # Archive DB
        db_record_archive = dynamodb.create_archive_record_from_db_record(
            file_record, dynamodb_status)
        dynamodb.write_record(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)

        # Grab result dynamodb for status data
        # ...
        # For time being append all test to it


        # Replace tasks with those specified by user if available
        if 'tasks' in data.record:
            tasks_list = data.record['tasks']
        else:
            tasks_list = batch.get_tasks_list()

        # Create job data
        #TODO: Create sort_key
        sort_key = ''
        job_data = batch.create_job_data(partition_key, tasks_list, file_record)
        batch_job_data.append(job_data)

    # Submit Batch jobs
    for job_data in batch_job_data:
        batch.submit_batch_job(job_data)

def validate_event_data(event_record):
    # Check for unknown argments
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
        'strict_mode',
    }
    args_unknown = [arg for arg in event_record if arg not in args_known]
    if args_unknown:
        plurality = 'arguments' if len(args_unknown) > 1 else 'argument'
        args_unknown_str = '\r\t'.join(args_unknown)
        logger.critical(f'got {len(args_unknown)} unknown arguments:\r\t{args_unknown_str}')
        raise ValueError

    # Only allow either manifest_fp or filepaths
    if 'manifest_fp' in event_record and 'filepaths' in event_record:
        logger.critical('\'manifest_fp\' or \'filepaths\' cannot both be provided')
        raise ValueError
    if not ('manifest_fp' in event_record or 'filepaths' in event_record):
        logger.critical('either \'manifest_fp\' or \'filepaths\' must be provided')
        raise ValueError

    # If given filepaths require output directory, prohibit for manifest_fp
    if 'filepaths' in event_record and not 'output_prefix' in event_record:
        logger.critical('\'output_prefix\' must be set when providing \'filepaths\'')
        raise ValueError
    if 'manifest_fp' in event_record and 'output_prefix' in event_record:
        logger.critical('use of \'output_prefix\' is prohibited with \'manifest_fp\'')
        raise ValueError

    # TODO: ensure that manifest and filepaths are just S3 key prefices
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

    # Check tasks are valid if provided
    tasks_list = event_record.get('tasks', list())
    tasks_unknown = [task for task in tasks_list if task not in batch.TASKS_AVAILABLE]
    if tasks_unknown:
        tasks_str = '\r\t'.join(tasks_unknown)
        tasks_allow_str = '\', \''.join(batch.DEFAULT_TASKS_LIST)
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

    # Process strict mode, must be bool
    if 'strict_mode' in event_record:
        strict_mode_str = event_record.get('strict_mode')
        if strict_mode_str.lower() == 'true':
            event_record['strict_mode'] = True
        elif strict_mode_str.lower() == 'false':
            event_record['strict_mode'] = False
        else:
            msg = f'expected \'True\' or \'False\' for strict_mode but got {strict_mode_str}'
            logger.critical(msg)
            raise ValueError
    else:
        event_record['strict_mode'] = True

    # Set remaining defaults
    if not 'exclude_fns' in event_record:
        event_record['exclude_fns'] = list()
    if not 'include_fns' in event_record:
        event_record['include_fns'] = list()


def handle_input_manifest(data: submission_data.SubmissionData, event, strict_mode):

    data.manifest_key = event['manifest_fp']
    data.submission_prefix = os.path.dirname(data.manifest_key)
    data.file_metadata = s3.get_s3_object_metadata(data.bucket_name, data.submission_prefix)
    data.manifest_data = submission_data.retrieve_manifest_data(data)

    file_list, data.files_extra = submission_data.validate_manifest(
        data,
        strict_mode,
    )

    # TODO:Raise exception notify email

    files_included, data.files_rejected = filter_filelist(
        file_list,
        event['include_fns'],
        event['exclude_fns']
    )

    # Create file records
    data.output_prefix = s3.get_output_prefix(data.submission_prefix)

    data.filename_accepted = files_included

    return data


def handle_input_filepaths(data:submission_data.SubmissionData, event):

    # Populate submission data from event
    data.submission_prefix = os.path.dirname(event['filepaths'][0])

    missing_files = list()

    for filepath in event['filepaths']:

        if not (file_mdata_list := s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET, directory_prefix=data.submission_prefix)):
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
