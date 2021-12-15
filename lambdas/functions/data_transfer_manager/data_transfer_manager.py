#!/usr/bin/env python3
import json
import logging
import os
import re
import uuid
import boto3

import util
from util import dynamodb, s3, batch, agha

JOB_NAME_RE = re.compile(r'[.\\/]')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_ARN')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')
DYNAMODB_STORE_ARCHIVE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_ARCHIVE_TABLE_NAME')
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
    if not validate_event_data(event):
        return {"StatusCode": 406, "body": "Invalid event payload"}

    submission = event["submission"]
    flagship = event["flagship_code"]

    s3_key = construct_directory_from_flagship_and_submission(flagship, submission)
    logger.info(f's3_key to be moved: {s3_key}')

    # Process each record and prepare Batch commands
    batch_job_data = list()

    # Get object list'
    logger.info('Check if data is have passed checks before moving')
    object_list = s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET, directory_prefix=s3_key)
    logger.info('Object list response:')
    print(object_list)

    # Checking for all status has passed
    for object_ in object_list:
        key = object_['Key']
        logger.info(f'Get status for {key} metadata')
        status_response = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                                           partition_key=dynamodb.ResultPartitionKey.STATUS.value,
                                                           sort_key_prefix=key)
        logger.info(f'Metadata response {status_response}:')
        logger.info(json.dumps(status_response))

        status_list = status_response['Items']
        for status in status_list:
            logger.info(f'Checking value status for:')
            logger.info(json.dumps(status))
            if status["value"].upper() != "PASS".upper():
                logger.error('Data has invalid check status')
                logger.error('Aborting!')
                return {"StatusCode": 406, "body": f"{status['value']} have not pass validation"}

    logger.info('Checking status validation has passed')

    # Processing s3 key (moving manifest record, and create move job)
    try:
        for object_ in object_list:
            key = object_['Key']
            logger.info(f'Processing s3_key:{key}')
            # Check if the following file has a compressed or index file
            result_data_record = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                                                  partition_key=dynamodb.ResultPartitionKey.DATA.value,
                                                                  sort_key_prefix=key)
            logger.info(f'Result data response:')
            logger.info(json.dumps(result_data_record))

            # Find out source file to transfer
            output_file_record = find_output_file(result_data_record['Items'])
            if output_file_record:
                source_bucket_name = output_file_record["value"][0]["bucket_name"]
                source_s3_key = output_file_record["value"][0]["s3_key"]
            else:
                source_bucket_name = STAGING_BUCKET
                source_s3_key = key

            # Create s3 uri
            source_s3_uri = create_s3_uri_from_bucket_name_and_key(source_bucket_name, source_s3_key)
            target_s3_uri = create_s3_uri_from_bucket_name_and_key(STORE_BUCKET, source_s3_key)
            logger.info(f'Generating source and target s3 URI')
            logger.info(f'source: {source_s3_uri}')
            logger.info(f'target: {target_s3_uri}')

            # Create and append job to job list
            create_batch_job = create_mv_s3_object_batch_job(source_s3_uri, target_s3_uri, s3_key)
            logger.info(f'Batch Job:')
            logger.info(json.dumps(create_batch_job))
            batch_job_data.append(create_batch_job)

            logger.info(f'Searching manifest record for: {s3_key}')
            # Dynamodb manifest record construct (manifest exclusion)
            manifest_response = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                                 partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                                 sort_key_prefix=key)
            # Should only have one response
            logger.info(f'Manifest response:')
            logger.info(json.dumps(manifest_response))
            if manifest_response['Count'] > 0:
                logger.info(f'Manifest record found on staging table')
                manifest_file_list = manifest_response['Items']

                logger.info(f'Moving manifest record from staging to store')
                store_manifest_record = dynamodb.ManifestFileRecord(**manifest_file_list[0])
                store_manifest_record.partition_key = source_s3_key
                store_manifest_record.filename = s3.get_s3_filename_from_s3_key(source_s3_key)
                store_manifest_record.filetype = agha.FileType.from_name(store_manifest_record.filename).get_name()

                res_write_obj = dynamodb.write_record_from_class(DYNAMODB_STORE_TABLE_NAME, store_manifest_record)
                logger.info('Write manifest record')
                print(res_write_obj)

                archive_record = dynamodb.ArchiveManifestFileRecord.create_archive_manifest_record_from_manifest_record(
                    store_manifest_record, s3.S3EventType.EVENT_OBJECT_CREATED.value)
                res_write_archive_obj = dynamodb.write_record_from_class(DYNAMODB_STORE_ARCHIVE_TABLE_NAME,
                                                                         archive_record)
                logger.info('Archive write manifest response')
                print(res_write_archive_obj)
            else:
                logger.info(f'{s3_key} does not have manifest record. Skipping ...')

    except Exception as e:
        logger.error(e)
        logger.error('Aborting!')
        return {"StatusCode": 406, "body": f"Something went wrong on moving manifest records from staging to store.\n"
                                           f",Error: {e}"}

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
        elif isinstance(folder_lock_resource, str):

            logger.info(f"Only one bucket policy found. Removing it...")
            response = S3_CLIENT.delete_bucket_policy(Bucket=STAGING_BUCKET)
            logger.info(f"Delete bucket policy response: {response}")
        else:
            logger.info("Unknown bucket policy. Raising an error")
            raise ValueError('Unknown Bucket policy')

    except Exception as e:
        logger.error(e)
        logger.error('Aborting!')
        return {"StatusCode": 406, "body": f"Something went wrong on lifting bucket policy.\n Error: {e}"}

    # Submit Batch jobs
    for job_data in batch_job_data:
        logger.info(f'Executing job:{json.dumps(job_data)}')
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

    command = ['s3', 'mv', source_s3_uri, target_s3_uri]

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

    for arg in args_known:
        if arg not in event_record:
            logger.error(f'must contain {arg} of the payload')
            return False
    return True


def construct_resource_from_s3_key_and_bucket(bucket_name, s3_key):
    return f'arn:aws:s3:::{bucket_name}/{s3_key}'


def find_output_file(result_data_record: list):
    """ Return a record with output file contain if any. Return void when none"""
    for data_record in result_data_record:
        if data_record['sort_key'].endswith(batch.Tasks.INDEX.value) or data_record['sort_key'].endswith(
                batch.Tasks.COMPRESS.value):
            return data_record
