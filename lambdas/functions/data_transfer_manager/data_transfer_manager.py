#!/usr/bin/env python3
import json
import logging
import os
import re
import sys
import uuid
import boto3
from boto3.dynamodb.conditions import Attr

import util
from util import dynamodb, s3, batch, agha

JOB_NAME_RE = re.compile(r'[.\\/]')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
BATCH_QUEUE_NAME = json.loads(os.environ.get('BATCH_QUEUE_NAME'))
S3_JOB_DEFINITION_ARN = os.environ.get('S3_JOB_DEFINITION_ARN')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_STORE_TABLE_NAME')
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_ARCHIVE_RESULT_TABLE_NAME')
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

    submission_directory = construct_directory_from_flagship_and_submission(flagship, submission)
    logger.info(f'Submission directory: {submission_directory}')

    # Process each record and prepare Batch commands
    batch_job_data = []
    dynamodb_job = []
    dynamodb_result_update_job = []

    # Checking if all type of test exist for the submission pass
    for each_test in batch.Tasks.tasks_to_list():

        partition_key_to_search = dynamodb.ResultPartitionKey.STATUS.value + ':' + each_test
        filter_key = Attr('value').ne('PASS')  # Not equal to PASS

        res = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                               partition_key=partition_key_to_search,
                                               sort_key_prefix=submission_directory,
                                               filter=filter_key)

        if res['Count'] > 0:
            logger.error('Data has FAIL check status:')
            logger.error(json.dumps(res['Items'], indent=4))
            logger.error('Aborting!')
            return {"StatusCode": 406, "body": f"{json.dumps(res['Items'], indent=4)}"}

    # Creating s3 move job
    try:

        # Grab object list
        manifest_list_res = dynamodb.get_item_from_pk_and_sk(table_name=DYNAMODB_STAGING_TABLE_NAME,
                                                             partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                             sort_key_prefix=submission_directory)
        manifest_list = manifest_list_res['Items']

        for manifest_record in manifest_list:
            s3_key = manifest_record['sort_key']
            logger.info(f'Processing s3_key:{s3_key}')

            list_to_process = []  # Temp list for creating job

            # Job to identify to move from staging (with no transformation) to store
            is_move_original_file = True

            for each_tasks in batch.Tasks.tasks_create_file():

                data_partition_key = dynamodb.ResultPartitionKey.DATA.value + ':' + each_tasks
                data_res = dynamodb.get_item_from_exact_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                                                  partition_key=data_partition_key,
                                                                  sort_key=s3_key)

                logger.info(f'Response of sort_key:\'{s3_key}\', partition_key:\'{data_partition_key}\':')
                logger.info(json.dumps(data_res, indent=4))

                if data_res['Count'] > 0:
                    item = data_res['Items'][0]
                    list_to_process.append(item['value'][0])

                    # For time being if file being compress. Original file is no longer needed
                    if each_tasks == batch.Tasks.COMPRESS.value:
                        is_move_original_file = False

                    # Dynamodb Record result update
                    updated_result_dynamodb = dynamodb.ResultRecord(**item)
                    update_value = item['value'][0].copy()
                    update_value['bucket_name'] = STORE_BUCKET
                    updated_result_dynamodb.value = update_value
                    dynamodb_result_update_job.append(updated_result_dynamodb)

            if is_move_original_file:
                logger.info(f'Do not have data generated in pipeline. Moving file from original state')
                list_to_process.append({'s3_key': s3_key,
                                        'checksum': manifest_record['provided_checksum'],
                                        'bucket_name': STAGING_BUCKET})

            # Process and crate move job
            for job_info in list_to_process:
                source_bucket = job_info['bucket_name']
                source_s3 = job_info['s3_key']
                checksum = job_info['checksum']

                cli_op = 'mv' # Move Operation for default value

                batch_job = create_cli_s3_object_batch_job(source_bucket_name=source_bucket,
                                                           source_s3_key=source_s3,
                                                           cli_op=cli_op)

                batch_job_data.append(batch_job)
                logger.info(f'Creating job for index:')
                logger.info(json.dumps(batch_job, indent=4))

                # Create Dynamodb from existing and override some value
                logger.info(f'Create dynamodb manifest record')
                record = dynamodb.ManifestFileRecord(**manifest_record)
                record.filename = s3.get_s3_filename_from_s3_key(source_s3)
                record.date_modified = util.get_datetimestamp()
                record.filetype = agha.FileType.from_name(record.filename).get_name()
                record.provided_checksum = checksum
                record.sort_key = source_s3
                dynamodb_job.append(record)

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

        resource = construct_resource_from_s3_key_and_bucket(STAGING_BUCKET, submission_directory) + '*'
        logger.info(f'Construct resource: {resource}')

        if isinstance(folder_lock_resource, list):

            try:
                folder_lock_resource.remove(resource)
            except Exception:
                raise ValueError('Folder lock resource not found')

            bucket_policy_json = json.dumps(bucket_policy)
            logger.info("New bucket policy:")
            logger.info(bucket_policy_json)

            response = S3_CLIENT.put_bucket_policy(Bucket=STAGING_BUCKET, Policy=bucket_policy_json)
            logger.info(f"BucketPolicy update response: {response}")

        elif isinstance(folder_lock_resource, str) and folder_lock_resource == resource:

            logger.info(f"Only one resource found in the folder lock policy. Removing it...")
            bucket_policy['Statement'].remove(folder_lock_statement)
            bucket_policy_json = json.dumps(bucket_policy)
            response = S3_CLIENT.put_bucket_policy(Bucket=STAGING_BUCKET, Policy=bucket_policy_json)
            logger.info(f"BucketPolicy update response: {response}")

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

    # Create DynamoDb in store table
    logger.info(f'Writing data to dynamodb')
    dynamodb.batch_write_records(table_name=DYNAMODB_STORE_TABLE_NAME,
                                 records=dynamodb_job)
    dynamodb.batch_write_record_archive(table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
                                        records=dynamodb_job,
                                        archive_log=s3.S3EventType.EVENT_OBJECT_CREATED.value)

    # Update Dynamodb for changing storage location at result table
    dynamodb.batch_write_records(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                 records=dynamodb_result_update_job)
    dynamodb.batch_write_record_archive(table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
                                        records=dynamodb_result_update_job,
                                        archive_log=s3.S3EventType.EVENT_OBJECT_CREATED.value)

    return "Data Transfer Job has started"


def construct_directory_from_flagship_and_submission(flagship, submission):
    return f'{flagship}/{submission}/'


def create_s3_uri_from_bucket_name_and_key(bucket_name, s3_key):
    return f"s3://{bucket_name}/{s3_key}"


def create_cli_s3_object_batch_job(source_bucket_name, source_s3_key, cli_op:str='mv'):
    name = source_s3_key

    source_s3_uri = create_s3_uri_from_bucket_name_and_key(bucket_name=source_bucket_name, s3_key=source_s3_key)
    target_s3_uri = create_s3_uri_from_bucket_name_and_key(bucket_name=STORE_BUCKET, s3_key=source_s3_key)

    name_raw = f'agha_data_transfer_{name}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = ['s3', cli_op, source_s3_uri, target_s3_uri]

    return {"name": name, "command": command}

def submit_data_transfer_job(job_data):
    client_batch = util.get_client('batch')

    command = job_data['command']

    client_batch.submit_job(
        jobName=job_data['name'],
        jobQueue=BATCH_QUEUE_NAME['small'],
        jobDefinition=S3_JOB_DEFINITION_ARN,
        containerOverrides={
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
