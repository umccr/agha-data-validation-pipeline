#!/usr/bin/env python3
import json
import logging
import os
import re
import uuid
import textwrap

import util
import util.dynamodb as dynamodb
import util.submission_data as submission_data
import util.notification as notification
import util.agha as agha
import util.s3 as s3
import util.batch as batch

JOB_NAME_RE = re.compile(r'[.\\/]')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_NAME_ARN')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to invoke s3 file migration batch job.

    {   
        bucket_name=""
        directory_prefix=""
    }

    OR

    {
        bucket_name=""
        s3_keys = []
    }
    :param event: payload to process and run batchjob
    :param context: not used
    """

    # Parse event data and get record
    # validate_event_data(event)
    bucket_name = event["bucket_name"]
    
    if "directory_prefix" in event:
        directory_prefix = event["directory_prefix"]
    
        # Grab data s3 list data submission
        s3_object_list = s3.get_s3_object_metadata(bucket_name=bucket_name, directory_prefix=directory_prefix)

        s3_keys = [object_metadata['Key'] for object_metadata in s3_object_list]
    else:
        s3_keys = event['s3_keys']

    # TODO: Check to DynamoDb if it is ok to do the migration
    # Insert implementation here ...

    # Process each record and prepare Batch commands
    batch_job_data = list()

    for s3_key in s3_keys:


        # Create job data
        job_data = batch.create_job_data(task='copy_and_delete', source_bucket=STAGING_BUCKET, \
            source_directory=s3_key, target_bucket=STORE_BUCKET, target_directory=s3_key)
        batch_job_data.append(job_data)

    # Submit Batch jobs
    for job_data in batch_job_data:
        batch.submit_batch_job(job_data)

def create_s3_data_transfer_job(task, source_bucket, source_directory, target_bucket, target_directory):

    name_raw = f'agha_validation__{task}__{source_directory}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = textwrap.dedent(f'''
        /opt/s3_manipulation.py \
        --source_bucket_name {source_bucket} \
        --source_directory {source_directory} \
        --target_bucket_name {target_bucket} \
        --target_directory {target_directory} \
        --task {task}
    ''')
    return {'name': name, 'command': command}


def submit_s3_data_transfer_job(job_data):
    
    client_batch = util.get_client('batch')

    command = ['bash', '-o', 'pipefail', '-c', job_data['command']]

    client_batch.submit_job(
        jobName=job_data['name'],
        jobQueue=BATCH_QUEUE_NAME,
        jobDefinition=S3_JOB_DEFINITION_ARN,
        containerOverrides={
            'memory': 4000,
            'command': command
        }
    )


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

    # PASS for now

