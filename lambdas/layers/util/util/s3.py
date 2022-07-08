import logging
import json
from enum import Enum
from typing import List
import os
import uuid

import util

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = util.get_client('s3')

class S3EventType(Enum):
    """
    See S3 Supported Event Types
    https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html#supported-notification-event-types
    """
    EVENT_OBJECT_CREATED = 'ObjectCreated'
    EVENT_OBJECT_REMOVED = 'ObjectRemoved'
    EVENT_UNSUPPORTED = 'Unsupported'

    def __str__(self):
        return self.value


class S3EventRecord:
    """
    A helper class for S3 event data passing and retrieval
    """

    def __init__(self, event_type, event_time, bucket_name, object_key, etag, size_in_bytes) -> None:
        self.event_type = event_type
        self.event_time = event_time
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.etag = etag
        self.size_in_bytes = size_in_bytes


def parse_s3_event(s3_event: dict) -> List[S3EventRecord]:
    """
    Parse raw SQS message bodies into S3EventRecord objects
    :param s3_event: the S3 event to be processed
    :return: list of S3EventRecord objects
    """
    s3_event_records = []

    if 'Records' not in s3_event.keys():
        logger.warning("No Records in message body!")
        logger.warning(json.dumps(s3_event, cls=util.DecimalEncoder))
        return

    records = s3_event['Records']

    for record in records:
        event_name = record['eventName']
        event_time = record['eventTime']
        s3 = record['s3']
        s3_bucket_name = s3['bucket']['name']
        s3_object_key = s3['object']['key']

        # eTag and Size is not included at object deletion event
        try:
            s3_object_etag = s3['object']['eTag']
            s3_object_size = s3['object']['size']
        except KeyError:
            s3_object_etag = ""
            s3_object_size = ""

        # Check event type
        if S3EventType.EVENT_OBJECT_CREATED.value in event_name:
            event_type = S3EventType.EVENT_OBJECT_CREATED
        elif S3EventType.EVENT_OBJECT_REMOVED.value in event_name:
            event_type = S3EventType.EVENT_OBJECT_REMOVED
        else:
            event_type = S3EventType.EVENT_UNSUPPORTED

        logger.debug(f"Found new event of type {event_type}")

        s3_event_records.append(S3EventRecord(event_type=event_type,
                                              event_time=event_time,
                                              bucket_name=s3_bucket_name,
                                              object_key=s3_object_key,
                                              etag=s3_object_etag,
                                              size_in_bytes=s3_object_size))

    return s3_event_records


def get_s3_object_metadata(bucket_name: str, directory_prefix: str):
    """
    Expected Output:
    [
        {
            'Key': 'string',
            'LastModified': datetime(2015, 1, 1),
            'ETag': 'string',
            'Size': 123,
            'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|'OUTPOSTS',
            'Owner': {
                'DisplayName': 'string',
                'ID': 'string'
            }
        },
        ...,
    ]
    """
    client_s3 = util.get_client('s3')

    results = list()
    response = client_s3.list_objects_v2(
        Bucket=bucket_name,
        Prefix=directory_prefix
    )

    if not (object_metadata := response.get('Contents')):
        raise ValueError(
            f'Something went wrong. List response: {json.dumps(response, indent=4, cls=util.JsonSerialEncoder)}')
    else:
        results.extend(object_metadata)

    while response['IsTruncated']:
        token = response['NextContinuationToken']
        response = client_s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=directory_prefix,
            ContinuationToken=token
        )
        results.extend(response.get('Contents'))

    return results


def aws_s3_ls(bucket_name: str, prefix: str) -> list:
    '''
    The same method of 'aws s3 ls' without --recursive flag

    :param bucket_name:
    :param prefix: Prefix to search in the bucket
    :return: A list of prefix in the directory
    '''

    s3 = util.get_client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    ls_list = []

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, Delimiter="/"):
        # CommonPrefixes and Contents might not be included in a page if there
        # are no items, so use .get() to return an empty list in that case
        for cur in page.get("CommonPrefixes", []):
            ls_list.append(cur["Prefix"])

    return ls_list


def get_object_from_bucket_name_and_s3_key(bucket_name, s3_key):
    client_s3 = util.get_client('s3')

    get_object_response = client_s3.get_object(
        Bucket=bucket_name,
        Key=s3_key,
    )

    data = get_object_response['Body'].read()

    return json.loads(data)


def get_s3_filename(metadata_record):
    filepath = metadata_record.get('Key')
    return get_s3_filename_from_s3_key(filepath)


def get_output_prefix(submission_prefix):
    output_dn = f'{util.get_datetimestamp()}_{uuid.uuid1().hex[:7]}'
    return os.path.join(submission_prefix, output_dn)


def get_s3_filename_from_s3_key(filepath):
    return os.path.basename(filepath)


def find_folder_lock_statement(policy: dict):
    for policy_statement in policy.get('Statement'):
        if policy_statement.get('Sid') == "FolderLock":
            return policy_statement

    raise ValueError


def create_s3_uri_from_bucket_name_and_key(bucket_name, s3_key):
    return f"s3://{bucket_name}/{s3_key}"


def delete_s3_object_from_key(bucket_name: str, key_list: list):
    objects_of_key = [{"Key": key} for key in key_list]

    # Delete the key in s3
    s3_client = util.get_client('s3')
    res = s3_client.delete_objects(
        Bucket=bucket_name,
        Delete={
            'Objects': objects_of_key,
        }
    )

    return res


def upload_s3_object_local_file(bucket_name: str, local_file: str, s3_key_destination: str):
    s3_client = util.get_client('s3')

    byte_file = open(local_file, 'rb')

    res = s3_client.put_object(
        Body=byte_file,
        Bucket=bucket_name,
        Key=s3_key_destination,
    )
    return res


def upload_s3_object_from_string(bucket_name: str, byte_of_string: str, s3_key_destination: str):
    s3_client = util.get_client('s3')
    res = s3_client.put_object(Body=byte_of_string, Bucket=bucket_name, Key=s3_key_destination)

    return res