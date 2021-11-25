
#!/usr/bin/env python3
import json
import logging
import os
import re

import pandas as pd

from lambdas.layers.util.util import notification
# From layers
import util
import util.dynamodb as dynamodb
import util.submission_data as submission_data
import util.notification as notification
import util.s3 as s3

DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME')
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')

# NOTE(SW): it seems reasonable to require some structuring of uploads in the format of
# <FLAGSHIP>/<DATE_TS>/<FILES ...>. Outcomes on upload wrt directory structure:
#   1. meets prescribed structure and we automatically launch validation jobs
#   2. differs from prescribed structure and data manager is notified to fix and then launch jobs
#
# Similarly, this logic could be applied to anything that might block or interfere with validation
# jobs. e.g. prohibited file types such as CRAM


# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Email/name regular expressions
AWS_ID_RE = '[0-9A-Z]{21}'
EMAIL_RE = '[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
USER_RE = re.compile(f'AWS:({AWS_ID_RE})')
SSO_RE = re.compile(f'AWS:({AWS_ID_RE}):({EMAIL_RE})')


def handler(event_record, context):
    """
    The lambda is todo a quick validation upon manifest file upload event and record to the database.

    What this lambda do:
    - check validity of the manifest
    - add Checksum from manifest to dynamodb
    - add StudyID from manifest to dynamodb
    - check and warn if file with the same etag has exist

    Entry point for S3 event processing. An S3 event is essentially a dict with a list of S3 Records:
    {
        "eventSource": "aws:s3",
        "eventTime": "2021-06-07T00:33:42.818Z",
        "eventName": "ObjectCreated:Put",
        ...
        "s3": {
            "bucket": {
                "name": "bucket-name",
                ...
            },
            "object": {
                "key": "UMCCR-COUMN/SBJ00805/WGS/2021-06-03/umccrised/work/SBJ00805__SBJ00805_MDX210095_L2100459/oncoviruses/work/detect_viral_reference/host_unmapped_or_mate_unmapped_to_gdc.bam.bai",
                "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                ...
            }
        }
    }

    :param event: S3 event
    :param context: not used
    """

    logger.info(f"Start processing S3 event:")
    logger.info(json.dumps(event_record))


    # Validate the event structure
    validate_event_data(event_record)

    # Store submission data into a class
    data = submission_data.SubmissionData(event_record)
  
    # Prepare submitter info
    submitter_info = notification.SubmitterInfo()
    if 'userIdentity' in event_record and 'principalId' in event_record['userIdentity']:
        principal_id = event_record['userIdentity']['principalId']
        submitter_info.name, submitter_info.email = get_name_email_from_principalid(principal_id)
        logger.info(f'Extracted name and email from record: {submitter_info.name} <{submitter_info.email}>')
    else:
        logger.warning(f'Could not extract name and email: unsuitable event type/data')
    

    # Pull file metadata from S3
    data.file_metadata = s3.get_s3_object_metadata(data.bucket_name, data.submission_prefix)

    # Collect manifest data and then validate
    data.manifest_data = submission_data.retrieve_manifest_data(data.bucket_name, data.manifest_key)
    file_list, data.files_extra = submission_data.validate_manifest(data)
  
    for filename in file_list:

        partition_key = f'{data.submission_prefix}/{filename}'

        # Variables from manifest data
        agha_study_id = submission_data.find_study_id_from_manifest_df_and_filename(data.manifest_data, filename)
        provided_checksum = submission_data.find_checksum_from_manifest_df_and_filename(data.manifest_data, filename)

        # Search for existing record
        try:
            file_record = dynamodb.get_record_from_s3_key(DYNAMODB_STAGING_TABLE_NAME, partition_key)
        except ValueError as e:

            logger.warn(e)
            logger.info(f'Create a new record for {partition_key}')
            file_record = dynamodb.BucketFileRecord.from_manifest_record(filename,data)

        finally:
        
            # Get partition key and existing records, and set sort key
            message_base = f'found existing records for {filename} with key {partition_key}'
            logger.info(f'{message_base}: {json.dumps(file_record)}')

            logger.info(f'Updating record with manifest data')
            file_record.date_modified = util.get_datetimestamp()
            file_record.agha_study_id = agha_study_id
            file_record.provided_checksum = provided_checksum
            file_record.is_in_manifest = "True"
            file_record.is_validated = "True"

                
        # Check if etag has exist
        # IGNORE on the staging bucket
        etag_response = dynamodb.grab_etag_record(file_record.etag)

        if etag_response['Count']>0:

            for each_etag_appearance in etag_response['Items']:
                s3_key = each_etag_appearance['s3_key']['S']
                bucket_name = each_etag_appearance['bucket_name']['S']


                message = f"File with the same ETag exist at the bucket.\n \
                    File location - bucket_name: {bucket_name}, s3_key: {s3_key}"

                notification.MESSAGE_STORE.append(message)
                logger.warning(message)

        # Update item at the record
        dynamodb.write_record(DYNAMODB_STAGING_TABLE_NAME, file_record)

        db_record_archive = dynamodb.create_archive_record_from_db_record(
            file_record, "CREATE or UPDATE")

        dynamodb.write_record(DYNAMODB_ARCHIVE_STAGING_TABLE_NAME, db_record_archive)



def validate_event_data(event_record):
    if 's3' not in event_record:
        logger.critical('no \'s3\' entry found in record')
        raise ValueError
    record_s3 = event_record['s3']

    if 'bucket' not in record_s3:
        logger.critical('S3 record missing bucket info')
        raise ValueError
    elif 'name' not in record_s3['bucket']:
        logger.critical('S3 bucket record missing name info')
        raise ValueError

    if 'object' not in record_s3:
        logger.critical('S3 record missing object info')
        raise ValueError
    elif 'key' not in record_s3['object']:
        logger.critical('S3 object record missing key info')
        raise ValueError

    if record_s3['bucket']['name'] != STAGING_BUCKET:
        logger.critical(f'expected {STAGING_BUCKET} bucket but got {record_s3["bucket"]["name"]}')
        raise ValueError


def get_name_email_from_principalid(principal_id):
    if USER_RE.fullmatch(principal_id):
        user_id = re.search(USER_RE, principal_id).group(1)
        user_list = notification.CLIENT_IAM.list_users()
        for user in user_list['Users']:
            if user['UserId'] == user_id:
                username = user['UserName']
        user_details = notification.CLIENT_IAM.get_user(UserName=username)
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
        logger.warning(f'Could not extract name and email: unsupported principalId format')
        return None, None
