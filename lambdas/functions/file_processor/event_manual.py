#!/usr/bin/env python3
import json
import logging
import os
import re
import sys


import util


import shared


# Logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


def handler(event_record):
    # Parse event data and get record
    validate_event_data(event_record)
    data = shared.SubmissionData(event_record)
    data.bucket_name = shared.STAGING_BUCKET

    # Prepare submitter info
    submitter_info = shared.SubmitterInfo()
    submitter_info.name = data.record.get('email_name', str())
    submitter_info.email = data.record.get('email_address', str())

    # Get file lists and other data. Returning and assigning to be explicit.
    # NOTE(SW): FileRecords are used so that we have single interface for record creation and job
    # submission later, and are available through data.files_accepted.
    if 'manifest_fp' in data.record:
        data = handle_input_manifest(data, submitter_info, data.record['strict_mode'])
    elif 'filepaths' in data.record:
        data = handle_input_filepaths(data, submitter_info)
    else:
        assert False

    # Process each record and prepare Batch commands
    batch_job_data = list()
    for file_record in data.files_accepted:
        # Get partition key and existing records
        partition_key = f'{data.submission_prefix}/{file_record.filename}'
        records_existing = shared.get_existing_records(partition_key)
        file_number_current = shared.get_file_number(records_existing)
        if records_existing:
            message_base = f'found existing records for {file_record.filename} with key {partition_key}'
            LOGGER.info(f'{message_base}: {records_existing}')
        elif data.record['record_mode'] == 'update':
            msg_p1 = f'no existing records found for {file_record.filename} with key {partition_key}'
            msg_p2 = 'in update mode, switching to create mode'
            # NOTE(SW): we should allow this to continue and exit after job submission, refactor
            shared.log_and_store_message(f'{msg_p1} {msg_p2}', level='info')
            data.record['record_mode'] = 'create'

        # Set sort key, write data to DynamoDB
        if data.record['record_mode'] == 'create':
            sort_key = file_number_current + 1
            record = shared.create_record(partition_key, sort_key, file_record)
            shared.inactivate_existing_records(records_existing)
            tasks_list = shared.get_tasks_list(record)
        elif data.record['record_mode'] == 'update':
            sort_key = file_number_current
            record = shared.update_record(partition_key, sort_key, file_record)
            tasks_list = shared.get_tasks_list(record)
        else:
            assert False

        # Replace tasks with those specified by user if available
        if 'tasks' in data.record:
            task_list = data.record['tasks']

        # Create job data
        job_data = shared.create_job_data(partition_key, sort_key, tasks_list, file_record)
        batch_job_data.append(job_data)

    # Submit Batch jobs
    for job_data in batch_job_data:
        shared.submit_batch_job(job_data)


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
        LOGGER.critical(f'got {len(args_unknown)} unknown arguments:\r\t{args_unknown_str}')
        sys.exit(1)

    # Only allow either manifest_fp or filepaths
    if 'manifest_fp' in event_record and 'filepaths' in event_record:
        LOGGER.critical('\'manifest_fp\' or \'filepaths\' cannot both be provided')
        sys.exit(1)
    if not ('manifest_fp' in event_record or 'filepaths' in event_record):
        LOGGER.critical('either \'manifest_fp\' or \'filepaths\' must be provided')
        sys.exit(1)

    # If given filepaths require output directory, prohibit for manifest_fp
    if 'filepaths' in event_record and not 'output_prefix' in event_record:
        LOGGER.critical('\'output_prefix\' must be set when providing \'filepaths\'')
        sys.exit(1)
    if 'manifest_fp' in event_record and 'output_prefix' in event_record:
        LOGGER.critical('use of \'output_prefix\' is prohibited with \'manifest_fp\'')
        sys.exit(1)

    # TODO: ensure that manifest and filepaths are just S3 key prefices
    # TODO: check that output_prefix is prefix only - no bucket name
    # TODO: require filepaths to have the same prefix i.e. be in the same directory

    # Only allow exclude or include at one time
    if 'exclude_fns' in event_record and 'include_fns' in event_record:
        LOGGER.critical('you cannot specify both \'exclude_fns\' and \'include_fns\'')
        sys.exit(1)
    # Only allow exclude_fns/include_fns for manifest_fp input
    if 'filepaths' in event_record:
        if 'exclude_fns' in event_record:
            LOGGER.critical('you cannot specify \'exclude_fns\' with \'filepaths\'')
            sys.exit(1)
        elif 'include_fns' in event_record:
            LOGGER.critical('you cannot specify \'include_fps\' with \'filepaths\'')
            sys.exit(1)

    # Check tasks are valid if provided
    tasks_list = event_record.get('tasks', list())
    tasks_unknown = [task for task in tasks_list if task not in shared.TASKS_AVAILABLE]
    if tasks_unknown:
        tasks_str = '\r\t'.join(tasks_unknown)
        tasks_allow_str = '\', \''.join(shared.DEFAULT_TASKS_LIST)
        LOGGER.critical(f'expected tasks to be one of \'{tasks_allow_str}\' but got:\t\r{tasks_str}')
        sys.exit(1)

    # Check record mode, if not present set default
    if rm := event_record.get('record_mode'):
        if rm not in {'create', 'update'}:
            msg = f'expected \'record_mode\' as one of \'create\' or \'update\' but got \'{rm}\''
            LOGGER.critical(msg)
            sys.exit(1)
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
            LOGGER.critical(msg)
            sys.exit(1)
    else:
        event_record['strict_mode'] = True

    # Set remaining defaults
    if not 'exclude_fns' in event_record:
        event_record['exclude_fns'] = list()
    if not 'include_fns' in event_record:
        event_record['include_fns'] = list()


def handle_input_manifest(data, submitter_info, strict_mode):
    data.manifest_key = data.record['manifest_fp']

    data.submission_prefix = os.path.dirname(data.manifest_key)
    data.flagship = shared.get_flagship_from_key(data.manifest_key, submitter_info, strict_mode)

    data.file_metadata = shared.get_s3_object_metadata(data, submitter_info)
    data.file_etags = shared.get_s3_etags_by_filename(data.file_metadata)

    data.manifest_data = shared.retrieve_manifest_data(data, submitter_info)

    file_list, data.files_extra = shared.validate_manifest(data, submitter_info, strict_mode)

    files_included, data.files_rejected = filter_filelist(
        file_list,
        data.record['include_fns'],
        data.record['exclude_fns']
    )

    # Create file records
    output_prefix = shared.get_output_prefix(data.submission_prefix)
    for filename in files_included:
        file_record = shared.FileRecord.from_manifest_record(filename, output_prefix, data)
        data.files_accepted.append(file_record)
    return data


def handle_input_filepaths(data, submitter_info):
    filepath = data.record['filepaths'][0]

    data.submission_prefix = os.path.dirname(filepath)
    data.flagship = shared.get_flagship_from_key(filepath, submitter_info, strict_mode=False)

    missing_files = list()
    for filepath in data.record['filepaths']:
        if not (file_mdata_list := shared.get_s3_object_metadata(data, submitter_info)):
            missing_files.append(filepath)
        else:
            assert len(file_mdata_list)
            data.file_metadata.extend(file_mdata_list)
    if missing_files:
        plurality = 'files' if len(missing_files) > 1 else 'file'
        files_str = '\r\t'.join(missing_files)
        message_base = f'could not find {len(missing_files)} {plurality} in \'{STAGING_BUCKET}\''
        log_and_store_message(f'{message_base}:\r\t{files_str}', level='critical')
        notify_and_exit(submitter_info)
    data.file_etags = shared.get_s3_etags_by_filename(data.file_metadata)

    for filepath in data.record['filepaths']:
        file_record = shared.FileRecord.from_filepath(
            filepath,
            data.record['output_prefix'],
            data,
        )
        data.files_accepted.append(file_record)
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
            LOGGER.info(f'{len(accepted)}/{len(file_list)} {plurality} passed filtering:\r\t{filenames_str}')
        else:
            LOGGER.info(f'no file include/exclude lists provided, skipping file filtering')
    else:
        LOGGER.critical(f'no files remaining after filtering')
        sys.exit(1)
    return accepted, excluded


def log_matched_filename_filters(files_found, filter_list, filter_type):
    # Emit matched files
    plurality = 'files' if len(filter_list) > 1 else 'file'
    filenames_str = '\r\t'.join(files_found)
    LOGGER.info(f'{filter_type}d {len(files_found)}/{len(filter_list)} {plurality}:\r\t{filenames_str}')
    # Check for missing files
    filenames_missing = set(filter_list).difference(files_found)
    if filenames_missing:
        filenames_str = '\r\t'.join(filenames_missing)
        message_base = f'did not find all {plurality} in {filter_type} list, missing'
        LOGGER.warning(f'{message_base}:\r\t{filenames_str}')
