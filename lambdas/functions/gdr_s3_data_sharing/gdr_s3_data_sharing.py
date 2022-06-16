#!/usr/bin/env python3
import json
import logging
import os
import uuid
import re
import sys

import util
from util import batch, dynamodb

S3_CLIENT = util.get_client('s3')
IAM_CLIENT = util.get_client('iam')

JOB_NAME_RE = re.compile(r'[.\\/]')

# Environment Variables
S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME = os.environ.get('S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME')
S3_DATA_SHARING_BATCH_QUEUE_NAME = os.environ.get('S3_DATA_SHARING_BATCH_QUEUE_NAME')
S3_DATA_SHARING_JOB_DEFINITION_ARN = os.environ.get('S3_DATA_SHARING_JOB_DEFINITION_ARN')
STORE_BUCKET = os.environ.get('STORE_BUCKET')
DYNAMODB_STORE_TABLE_NAME = os.environ.get('DYNAMODB_STORE_TABLE_NAME')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

from util import s3


def handler(event, context):
    """
    This function will check and notify if check has been done and the result of it.

    event payload expected:
    {
        destination_s3_arn: '',
        destination_s3_key_prefix: '',
        source_s3_key_list :[ 'ABCDE/121212/filename.fastq.gz']
    }
    """

    logger.info("Processing event:")
    logger.info(json.dumps(event, indent=4))

    # Parsing
    destination_s3_arn = event['destination_s3_arn'].strip('/*')
    source_s3_key_list = event['source_s3_key_list']
    destination_s3_key_prefix = event['destination_s3_key_prefix']

    ################################################
    # Check bucket location
    # The reason that we transfer via S3 is to prevent any egress cost.
    # If bucket location is not at the same region. Egress cost will still occur.

    destination_bucket_name = destination_s3_arn.split(':::')[-1]
    bucket_location = get_s3_location(destination_bucket_name)
    logger.info(f'Destination bucket location: {bucket_location}')

    if bucket_location != 'ap-southeast-2':
        message = f"Expected in 'ap-southeast-2'"
        logger.error(message)
        return message

    ################################################
    # Add this S3 ARN to instance role to allow batch push data to destination
    logger.info('ARN added to IAM role')

    new_iam_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:PutObjectAcl"
                ],
                "Resource": f"{destination_s3_arn}/*"

            }
        ]
    }

    # Create new policy
    create_policy_response = IAM_CLIENT.create_policy(
        PolicyName=f"{destination_s3_arn}-bucket-policy",
        PolicyDocument=json.dumps(new_iam_policy),
        Description='The policy to allow push object to s3',
        Tags=[
            {
                'Key': 'Creator',
                'Value': 'agha-gdr-validation-pipeline-s3-data-sharing-lambda'
            },
        ]
    )
    logger.debug(
        f'New policy created. Response: {json.dumps(create_policy_response, indent=4, cls=util.JsonSerialEncoder)}')
    policy_arn = create_policy_response["Policy"]["Arn"]

    # Attach policy to the instance role
    response_attach_policy = IAM_CLIENT.attach_role_policy(
        RoleName=S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME,
        PolicyArn=policy_arn
    )

    logger.info(f"Successfully add new policy to the '{S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME}' role.")

    ################################################
    # Find what needs to be transfer and create batch job
    logger.info('List of jobs need to be copied')

    batch_job_list = []
    for source_s3_key in source_s3_key_list:
        filename = s3.get_s3_filename_from_s3_key(source_s3_key)

        destination_s3_key = f"destination_s3_key_prefix/{filename}"

        batch_job = create_cli_s3_object_batch_job(source_bucket_name=STORE_BUCKET, source_s3_key=source_s3_key,
                                                   destination_bucket_name=destination_bucket_name,
                                                   destination_s3_key=destination_s3_key, cli_op='cp')
        batch_job_list.append(batch_job)

    ################################################
    # Submitting job

    logger.info(f'Batch Job list: {json.dumps(batch_job_list, indent=4)}')
    for job_data in batch_job_list:
        submit_s3_data_sharing_batch_job(job_data)
    logger.info(f'Batch job has executed. Submit {len(batch_job_list)} number of job')

    ################################################
    # Notify if any


def get_s3_location(bucket_name):
    response = S3_CLIENT.get_bucket_location(Bucket=bucket_name)
    return response['LocationConstraint']


def create_cli_s3_object_batch_job(source_bucket_name: str, source_s3_key: str, destination_bucket_name: str,
                                   destination_s3_key: str, cli_op: str = 'cp'):
    name = source_s3_key

    source_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(bucket_name=source_bucket_name, s3_key=source_s3_key)
    destination_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(bucket_name=destination_bucket_name,
                                                                   s3_key=destination_s3_key)

    name_raw = f'agha_s3_data_sharing_{name}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = ['s3', cli_op, source_s3_uri, destination_s3_uri]

    return {"name": name, "command": command}


def submit_s3_data_sharing_batch_job(job_data):
    client_batch = util.get_client('batch')

    command = job_data['command']

    client_batch.submit_job(
        jobName=job_data['name'],
        jobQueue=S3_DATA_SHARING_BATCH_QUEUE_NAME,
        jobDefinition=S3_DATA_SHARING_JOB_DEFINITION_ARN,
        containerOverrides={
            'command': command
        }
    )
