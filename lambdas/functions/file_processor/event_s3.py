#!/usr/bin/env python3
import json
import logging
import os
import re
import sys


import shared


# NOTE(SW): it seems reasonable to require some structuring of uploads in the format of
# <FLAGSHIP>/<DATE_TS>/<FILES ...>. Outcomes on upload wrt directory structure:
#   1. meets prescribed structure and we automatically launch validation jobs
#   2. differs from prescribed structure and data manager is notified to fix and then launch jobs
#
# Similarly, this logic could be applied to anything that might block or interfere with validation
# jobs. e.g. prohibited file types such as CRAM


# Logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Email/name regular expressions
AWS_ID_RE = '[0-9A-Z]{21}'
EMAIL_RE = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f'AWS:({AWS_ID_RE})')
SSO_RE = re.compile(f'AWS:({AWS_ID_RE}):({EMAIL_RE})')


def handler(event_record):
    # Parse event data and get record
    validate_event_data(event_record)
    data = shared.SubmissionData(event_record)
    data.bucket_name = shared.STAGING_BUCKET

    # Update policy on S3 submission prefix to be read-only
    shared.CLIENT_LAMBDA.invoke(
        FunctionName=shared.FOLDER_LOCK_LAMBDA_ARN,
        InvocationType='Event',
        Payload=json.dumps({'Record': event_record})
    )

    # Prepare submitter info
    submitter_info = shared.SubmitterInfo()
    if 'userIdentity' in event_record and 'principalId' in event_record['userIdentity']:
        principal_id = event_record['userIdentity']['principalId']
        submitter_info.name, submitter_info.email = get_name_email_from_principalid(principal_id)
        LOGGER.info(f'Extracted name and email from record: {submitter_info.name} <{submitter_info.email}>')
    else:
        LOGGER.warning(f'Could not extract name and email: unsuitable event type/data')

    # Get manifest submission S3 key prefix
    data.bucket_name = event_record['s3']['bucket']['name']
    data.manifest_key = event_record['s3']['object']['key']
    data.submission_prefix = os.path.dirname(data.manifest_key)
    LOGGER.info(f'Submission with prefix: {data.submission_prefix}')

    # Obtain flagship from S3 key and require it to be a known value
    data.flagship = shared.get_flagship_from_key(data.manifest_key, submitter_info)

    # Pull file metadata from S3 and get ETags, file sizes by filename
    data.file_metadata = shared.get_s3_object_metadata(data, submitter_info)
    data.file_etags = shared.get_s3_etags_by_filename(data.file_metadata)
    data.file_sizes = shared.get_s3_filesizes_by_filename(data.file_metadata)

    # Collect manifest data and then validate
    data.manifest_data = shared.retrieve_manifest_data(data, submitter_info)
    file_list, data.extra_files = shared.validate_manifest(data, submitter_info)

    # Process each record and prepare Batch commands
    batch_job_data = list()
    output_prefix = shared.get_output_prefix(data.submission_prefix)
    for filename in file_list:
        # Create file record
        file_record = shared.FileRecord.from_manifest_record(filename, output_prefix, data)

        # Get partition key and existing records, and set sort key
        partition_key = f'{data.submission_prefix}/{file_record.filename}'
        records_existing = shared.get_existing_records(partition_key)
        file_number_current = shared.get_file_number(records_existing)
        if records_existing:
            message_base = f'found existing records for {filename} with key {partition_key}'
            LOGGER.info(f'{message_base}: {records_existing}')
        sort_key = file_number_current + 1

        # Create record, inactivate all others
        record = shared.create_record(partition_key, sort_key, file_record)
        shared.inactivate_existing_records(records_existing)

        # Construct command and job name
        tasks_list = shared.get_tasks_list(record)
        job_data = shared.create_job_data(partition_key, sort_key, tasks_list, file_record)
        batch_job_data.append(job_data)

    # Submit Batch jobs
    for job_data in batch_job_data:
        shared.submit_batch_job(job_data)


def validate_event_data(event_record):
    if 's3' not in event_record:
        LOGGER.critical('no \'s3\' entry found in record')
        sys.exit(1)
    record_s3 = event_record['s3']

    if 'bucket' not in record_s3:
        LOGGER.critical('S3 record missing bucket info')
        sys.exit(1)
    elif 'name' not in record_s3['bucket']:
        LOGGER.critical('S3 bucket record missing name info')
        sys.exit(1)

    if 'object' not in record_s3:
        LOGGER.critical('S3 record missing object info')
        sys.exit(1)
    elif 'key' not in record_s3['object']:
        LOGGER.critical('S3 object record missing key info')
        sys.exit(1)

    if record_s3['bucket']['name'] != shared.STAGING_BUCKET:
        LOGGER.critical(f'expected {shared.STAGING_BUCKET} bucket but got {record_s3["bucket"]["name"]}')
        sys.exit(1)


def get_name_email_from_principalid(principal_id):
    if USER_RE.fullmatch(principal_id):
        user_id = re.search(USER_RE, principal_id).group(1)
        user_list = IAM_CLIENT.list_users()
        for user in user_list['Users']:
            if user['UserId'] == user_id:
                username = user['UserName']
        user_details = IAM_CLIENT.get_user(UserName=username)
        tags = user_details['User']['Tags']
        for tag in tags:
            if tag['Key'] == 'email':
                email = tag['Value']
        return username, email
    elif SSO_RE.fullmatch(principal_id):
        email = re.search(SSO_RE, principal_id).group(2)
        username = email.split('@')[0]
        return username, email
    else:
        LOGGER.warning(f'Could not extract name and email: unsupported principalId format')
        return None, None
