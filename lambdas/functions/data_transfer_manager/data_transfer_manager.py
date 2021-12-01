#!/usr/bin/env python3
import json
import logging
import os
import re
import textwrap
import uuid
import boto3

import util
from util import batch, dynamodb, s3

JOB_NAME_RE = re.compile(r'[.\\/]')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_ARN')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client('s3')


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
    if validate_event_data(event) == False:
        return "Incorrect event format"


    submission = event["submission"]
    flagship = event["flagship_code"]

    s3_key = construct_directory_from_flagship_and_submission(flagship, submission)
    logger.info(f's3_key to be moved: {s3_key}')

    # Process each record and prepare Batch commands
    batch_job_data = list()

    # Get object list'
    logger.info('Check if data is have passed checks before moving')
    object_list = s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET,directory_prefix=s3_key)
    logger.info('Object list response:')
    print(object_list)

    for object in object_list:
        key = object['Key']
        logger.info(f'Get status for {key} metadata')
        status_response = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                                           partition_key=key,
                                                           sort_key_prefix=dynamodb.ResultSortKeyPrefix.STATUS.value)
        logger.info(f'Metadata response {status_response}:')
        logger.info(json.dumps(status_response))

        status_list = status_response['Items']
        for status in status_list:
            logger.info(f'Checking value status for:')
            logger.info(json.dumps(status))
            if status["value"].upper() != "PASS".upper():
                logger.error('Data has invalid check status')
                logger.error('Aborting!')
                return f'{status["value"]} have not pass validation'
    logger.info('Checking fail data has passed')

    # Unlock bucket
    try:
        get_bucket_policy_response = S3_CLIENT.get_bucket_policy(Bucket=STAGING_BUCKET)
        logger.info("Received policy response:")
        logger.info(json.dumps(get_bucket_policy_response))
        bucket_policy = json.loads(get_bucket_policy_response['Policy'])

        logger.info("Grab folder lock statement")
        folder_lock_statement = s3.find_folder_lock_statement(bucket_policy)

        folder_lock_resource = folder_lock_statement.get('Resource')
        logger.info('Folder Lock statement')
        logger.info(json.dumps(folder_lock_resource))

        if isinstance(folder_lock_resource, list):
            resource = construct_resource_from_s3_key_and_bucket(STAGING_BUCKET, s3_key)

            logger.info(f'Construct resource: {resource}')

            folder_lock_resource.remove(resource)

        bucket_policy_json = json.dumps(bucket_policy)
        logger.info("New bucket policy:")
        logger.info(bucket_policy_json)

        response = S3_CLIENT.put_bucket_policy(Bucket=STAGING_BUCKET, Policy=bucket_policy_json)
        logger.info(f"BucketPolicy update response: {response}")

    except Exception as e:
        logger.warning(e)

    source_s3_uri = create_s3_uri_from_bucket_name_and_key(STAGING_BUCKET, s3_key)
    target_s3_uri = create_s3_uri_from_bucket_name_and_key(STORE_BUCKET, s3_key)
    logger.info(f'Generating source and target s3 URI')
    logger.info(f'source: {source_s3_uri}')
    logger.info(f'target: {target_s3_uri}')

    create_batch_job = create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri, s3_key)
    logger.info(f'Batch Job:')
    logger.info(json.dumps(create_batch_job))
    batch_job_data.append(create_batch_job)

    # Submit Batch jobs
    for job_data in batch_job_data:
        submit_data_transfer_job(job_data)

    logger.info(f'Batch job has executed.')
    return "Data Transfer Job has started"


def construct_directory_from_flagship_and_submission(flagship, submission):
    return f'{flagship}/{submission}/'


def create_s3_uri_from_bucket_name_and_key(bucket_name, s3_key):
    return f"s3://{bucket_name}/{s3_key}"

def create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri, name):
    name_raw = f'agha_data_transfer_{name}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = ['s3', 'mv', source_s3_uri, target_s3_uri, '--recursive']

    return {"name": name, "command": command}


def submit_data_transfer_job(job_data):
    client_batch = util.get_client('batch')

    command = job_data['command']

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
        'submission',
        'flagship_code',
    }

    for arg  in args_known:
        if arg not in event_record:
            logger.error(f'must contain {arg} of the payload')
            return False
    return True

def construct_resource_from_s3_key_and_bucket(bucket_name, s3_key):
    return f'arn:aws:s3:::{bucket_name}/{s3_key}*'