#!/usr/bin/env python3
import json
import logging
import os
import re
import textwrap
import uuid

import util
from util import batch, dynamodb

JOB_NAME_RE = re.compile(r'[.\\/]')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_ARN')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event, context):
    """
    The lambda is to invoke s3 file migration batch job
    {
        submission: "13023_3432423"
        flagship_code: "ACG"
    }
    :param event: payload to process and run batchjob
    :param context: not used
    """

    logger.info('Processing event:')
    logger.info(json.dumps(event))


    # Parse event data and get record
    # validate_event_data(event)
    submission = event["submission"]
    flagship = event["flagship_code"]

    s3_key = construct_directory_from_flagship_and_submission(flagship,submission)
    logger.info(f's3_key to be moved: {s3_key}')

    # Process each record and prepare Batch commands
    batch_job_data = list()

    # TODO: Check to DynamoDb if it is ok to do the migration
    # Insert implementation here ...


    source_s3_uri = create_s3_uri_from_bucket_name_and_key(STAGING_BUCKET, s3_key)
    target_s3_uri = create_s3_uri_from_bucket_name_and_key(STORE_BUCKET, s3_key)
    logger.info(f'Generating source and target s3 URI')
    logger.info(f'source: {source_s3_uri}')
    logger.info(f'target: {target_s3_uri}')


    create_batch_job = create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri)
    logger.info(f'Batch Job:')
    logger.info(json.dumps(create_batch_job))
    batch_job_data.append(create_batch_job)

    # Submit Batch jobs
    for job_data in batch_job_data:
        submit_data_transfer_job(job_data)

    logger.info(f'Batch job has executed.')

def construct_directory_from_flagship_and_submission(flagship,submission):
    return f'{flagship}/{submission}/'

def create_s3_uri_from_bucket_name_and_key(bucket_name, s3_key):
    return f"s3://{bucket_name}/{s3_key}"

def create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri):

    name_raw = f'agha_data_transfer'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = textwrap.dedent(f'''
        aws s3 mv {source_s3_uri} {target_s3_uri} --recursive
    ''')

    return { "name":name, "command" :command }

def submit_data_transfer_job(job_data):
    
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
        'strict_mode',
    }

    # PASS for now

