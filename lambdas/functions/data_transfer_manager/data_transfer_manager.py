#!/usr/bin/env python3
import json
import logging
import os
import re
import textwrap

import util
from util import batch, dynamodb

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
    The lambda is to invoke s3 file migration batch job
    {
        s3_keys = []
    }
    :param event: payload to process and run batchjob
    :param context: not used
    """

    logger.info('Processing event:')
    logger.info(json.dumps(event))


    # Parse event data and get record
    # validate_event_data(event)
    s3_keys = event["s3_keys"]


    # Process each record and prepare Batch commands
    batch_job_data = list()

    for s3_key in s3_keys:

        # TODO: Check to DynamoDb if it is ok to do the migration
        # Insert implementation here ...
        dynamodb.get_item_from_pk_and_sk()


        source_s3_uri = create_s3_uri_from_bucket_name_and_key(STAGING_BUCKET, s3_key)
        target_s3_uri = create_s3_uri_from_bucket_name_and_key(STORE_BUCKET, s3_key)

        batch_job_data.extend(create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri))

    # Submit Batch jobs
    for job_data in batch_job_data:
        batch.submit_batch_job(job_data)

def create_s3_uri_from_bucket_name_and_key(bucket_name, s3_key):
    return f"s3://{bucket_name}/{s3_key}"

def create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri):
    command = textwrap.dedent(f'''
        aws s3 mv {source_s3_uri} {target_s3_uri}
    ''')
    return command

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

