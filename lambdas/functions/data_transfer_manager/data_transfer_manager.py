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
from util import dynamodb, s3, batch, agha, submission_data

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
        "submission": "13023_3432423",
        "flagship_code": "ACG"
    }

    Additional optional payload that can be set on the lambda
    {
        "skip_unlock_bucket": "true",
        "skip_submit_batch_job": "true",
        "skip_update_dynamodb": "true",
        "validation_check_only": "true",
        "exception_postfix_filename": ["metadata.txt", ".md5", etc.],
        "run_all":"true"
    }

    :param event: payload to process and run batchjob
    :param context: not used
    """

    logger.info('Processing event:')
    logger.info(json.dumps(event))

    # Parse event data and get record
    try:
        validate_event_data(event)
    except ValueError as e:
        return {"StatusCode": 406, "body": f"Invalid event payload.\n ERROR: {e}"}

    submission = event["submission"]
    flagship = event["flagship_code"]

    submission_directory = construct_directory_from_flagship_and_submission(flagship, submission)
    logger.info(f'Submission directory: {submission_directory}')

    # Process each record and prepare Batch commands
    batch_job_data = []
    dynamodb_job = []
    dynamodb_result_update_job = []

    # Check if batch job has completed
    fail_batch_key = run_batch_check(submission_directory, event.get("exception_postfix_filename"))

    # Checking if all type of test exist for the submission pass
    fail_status_result_key = run_status_result_check(submission_directory)

    if len(fail_batch_key) > 0 or len(fail_status_result_key) > 0 or event.get('validation_check_only'):
        reason = 'Validation report requested'

        if event.get('validation_check_only') is None:
            logger.error('Status check has failed')
            reason = 'The following fail check should not exist'

        message = json.dumps({
            "reason": reason,
            "submission": submission_directory,
            "fail_batch_job_s3_key": fail_batch_key,
            "fail_validation_result_s3_key": fail_status_result_key
        }, indent=4)

        logger.error(message)
        return message

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

            if submission_data.is_file_skipped(s3_key, event.get("exception_postfix_filename")):
                logger.info(f'Skipping index file as validation manager produce its own')
                # Not processing index file from staging bucket
                # Validation manager produce its own indexing file
                continue

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
                    updated_result_dynamodb.value = [update_value]  # Putting back as an array following original output
                    dynamodb_result_update_job.append(updated_result_dynamodb)

            if is_move_original_file:
                logger.info(f'Do not have data generated in pipeline. Moving file from original state')

                # Grab checksum value from result
                data_partition_key = dynamodb.ResultPartitionKey.DATA.value + ':' + batch.Tasks.CHECKSUM_VALIDATION.value
                data_res = dynamodb.get_item_from_exact_pk_and_sk(DYNAMODB_RESULT_TABLE_NAME,
                                                                  data_partition_key,
                                                                  s3_key)
                if data_res['Count'] > 0:
                    item = data_res['Items'][0]
                    calculated_checksum = item['value']

                else:
                    logger.warning(f'No checksum found from batch. Using submitted checksum ...')
                    calculated_checksum = manifest_record['provided_checksum']

                list_to_process.append({'s3_key': s3_key,
                                        'checksum': calculated_checksum,
                                        'bucket_name': STAGING_BUCKET})

            # Process and crate move job
            for job_info in list_to_process:
                source_bucket = job_info['bucket_name']
                source_s3 = job_info['s3_key']
                checksum = job_info['checksum']

                cli_op = 'mv'  # Move Operation for default value

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
    if event.get("skip_unlock_bucket"):
        logger.info('Skip unlock bucket flag is raised. Skipping...')
    else:
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
                logger.critical("Unknown bucket policy. Raising an error")
                raise ValueError('Unknown Bucket policy')

        except Exception as e:
            logger.critical(e)
            logger.critical('Continue with the risk of fail to delete files in the staging bucket.')

    # Submit Batch jobs
    if event.get("skip_submit_batch_job"):
        logger.info('Skip submit batch job flag is raised. Skipping ...')
    else:
        for job_data in batch_job_data:
            logger.info(f'Executing job:{json.dumps(job_data)}')
            submit_data_transfer_job(job_data)
        logger.info(f'Batch job has executed. Submit {len(batch_job_data)} number of job')

    # Create DynamoDb in store table
    if event.get("skip_update_dynamodb"):
        logger.info('Skip update dynamodb flag is raised. Skipping ...')
    else:
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

    if event.get("skip_generate_manifest_file"):
        logger.info('Skip generate manifest file flag is in event payload. Skipping ...')
    else:
        # Create new manifest file from dynamodb given
        logger.info(f'Generate manifest file from dynamodb')
        manifest_item = dynamodb.get_batch_item_from_pk_and_sk(DYNAMODB_STORE_TABLE_NAME,
                                                               dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                               submission_directory)
        # Define manifest file
        new_manifest_file = 'checksum\tfilename\tagha_study_id\n'  # First line is header

        # Iterate to manifest list
        for item in manifest_item:
            checksum = item['provided_checksum']
            filename = item['filename']
            agha_study_id = item['agha_study_id']

            new_manifest_file += f'{checksum}\t{filename}\t{agha_study_id}\n'
        manifest_destination_key = submission_directory + 'manifest.txt'
        upload_res = s3.upload_s3_object_from_string(bucket_name=STORE_BUCKET,
                                                     byte_of_string=new_manifest_file,
                                                     s3_key_destination=manifest_destination_key)

        logger.info(f'Uploaded dynamodb manifest.txt. Response:')
        print(upload_res)

    return "Data Transfer Job has started"


def construct_directory_from_flagship_and_submission(flagship, submission):
    return f'{flagship}/{submission}/'


def create_cli_s3_object_batch_job(source_bucket_name, source_s3_key, cli_op: str = 'mv'):
    name = source_s3_key

    source_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(bucket_name=source_bucket_name, s3_key=source_s3_key)
    target_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(bucket_name=STORE_BUCKET, s3_key=source_s3_key)

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


def validate_event_data(event_payload: dict):
    # Check for unknown arguments
    args_known = {
        'submission',
        'flagship_code',
        'skip_update_dynamodb',
        'skip_submit_batch_job',
        'skip_unlock_bucket',
        'validation_check_only',
        'exception_postfix_filename',
        'run_all',
        'skip_generate_manifest_file'
    }

    # Check for required payload
    for args in ['submission', 'flagship_code']:
        if args not in event_payload:
            raise ValueError(f'\'{args}\' does not exist. It is required.')

    # Check for foreign keyword
    payload_supplied = set(event_payload.keys())
    foreign_payload = set(payload_supplied - args_known)
    if len(foreign_payload) > 0:
        logger.error(f'\'{json.dumps(foreign_payload)}\' is not in the allowed payload.')
        raise ValueError(f'\'{json.dumps(foreign_payload)}\' is not in the allowed payload.')

    # Sanitize to string of bool to bool()
    for args in ['skip_update_dynamodb', 'skip_submit_batch_job', 'skip_unlock_bucket', 'validation_check_only',
                 "run_all", 'skip_generate_manifest_file']:
        if (bool_payload := event_payload.get(args)) is not None:
            if isinstance(bool_payload, str):
                try:
                    event_payload[args] = json.loads(bool_payload.lower())
                except json.JSONDecodeError:
                    raise ValueError(f'\'{args}\' has an invalid boolean')

    # Check if exception_postfix_filename is a list
    if (list_args := event_payload.get('exception_postfix_filename')) is not None:
        if not isinstance(list_args, list):
            raise ValueError(f'\'exception_postfix_filename\' is not a list')

    # Check if submission and flagship_code is a string
    for args in ['submission', 'flagship_code']:
        if (payload := event_payload.get(args)) is not None:
            if not isinstance(payload, str):
                raise ValueError(f'\'{args}\' is not a string')

    # If run all, remove all skipping steps
    if event_payload.get('run_all') is not None and event_payload['run_all']:
        for args in ['skip_update_dynamodb', 'skip_submit_batch_job', 'skip_unlock_bucket', 'validation_check_only']:
            event_payload.pop(args, None)
    else:
        # These args must be in the payload if not run_all
        skip_args = ['skip_update_dynamodb', 'skip_submit_batch_job', 'skip_unlock_bucket', 'validation_check_only']
        difference = set(skip_args) - set(event_payload.keys())

        if len(difference) >= len(skip_args):
            raise ValueError(
                f'If run_all is not specified or equals to False, one of the skip arguments must be present. \n'
                f'Skipped arguments: {json.dumps(skip_args)}')


def construct_resource_from_s3_key_and_bucket(bucket_name, s3_key):
    return f'arn:aws:s3:::{bucket_name}/{s3_key}'


def run_batch_check(staging_directory_prefix: str, exception_list=None) -> list:
    """
    This function is to check if all batch job has been run successfully. The function will check if non-index file
    in s3 staging bucket has a result generated by batch job in the result bucket.

    :param directory_prefix: s3 key to the staging bucket
    :return:
    """

    s3_list = s3.get_s3_object_metadata(bucket_name=STAGING_BUCKET, directory_prefix=staging_directory_prefix)

    if exception_list is None:
        exception_list = []

    fail_batch_job_key = []

    for s3_item in s3_list:
        s3_key = s3_item['Key']

        # Skip for index file
        if agha.FileType.is_index_file(s3_key) or agha.FileType.from_name(s3_key) == agha.FileType.MANIFEST \
                or agha.FileType.is_md5_file(s3_key) or len(
            [s3_key for exception_postfix in exception_list if s3_key.endswith(exception_postfix)]) > 0:
            continue

        # Check if validation had succeed via dynamodb
        sort_key = s3_key + '__results.json'
        dy_res = dynamodb.get_item_from_exact_pk_and_sk(table_name=DYNAMODB_RESULT_TABLE_NAME,
                                                        partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
                                                        sort_key=sort_key)

        if dy_res['Count'] < 1:
            # Means no data generated for s3 key
            logger.warning(f'Batch job for \'{s3_key}\' key has not succeed.')
            fail_batch_job_key.append(s3_key)

    if len(fail_batch_job_key) > 0:
        logger.error('Batch Job list tha has not succeed:')
        logger.error(json.dumps(fail_batch_job_key, indent=4))

    return fail_batch_job_key


def run_status_result_check(submission_directory: str) -> list:
    """
    This will check if all checks generated from the batch job is valid with a PASS status. Any other value than 'PASS'
    will be returned from this function.
    :param submission_directory:
    :return:
    """

    fail_s3_key = []

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
            fail_s3_key.extend(res['Items'])

    return fail_s3_key
