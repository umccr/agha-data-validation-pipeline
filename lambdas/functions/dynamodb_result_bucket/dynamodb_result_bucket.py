import json
import logging
import os
import enum

import util
import util.dynamodb as dynamodb

RESULT_BUCKET = os.environ.get('RESULT_BUCKET')
DYNAMODB_RESULT_TABLE_NAME = os.environ.get('DYNAMODB_RESULT_TABLE_NAME')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SortKeyPrefix(enum.Enum):

    FILE = "FILE"
    STATUS = "STATUS"
    DATA = "DATA"

    def __str__(self):
        return self.value

class ResultFileRecord:
    """
    - partition_key: Most probably the s3 of the original file
    - sort_key: Prefix will be 'FILE' followed by check_type and the filename seperated by a colon
            (e.g. Result of index check file would be stored as 'FILE:INDEX:some_file.bai')
    - s3_key: The s3_key for the result file
    
    Other field are self explanotary taken from S3 metadata
    """
    def __init__(self):
        self.partition_key=""
        self.sort_key=""
        self.etag=""
        self.size_in_bytes=0
        self.filename=""
        self.s3_key=""
        self.flagship=""
        self.date_modified=util.get_datetimestamp()

class ResultDataRecord:
    """
    partition_key: Most probably the s3 of the original file
    sort_key: Prefix will be 'DATA' followed by check_type seperated by colon
            (e.g. Result of checksum check would be stored as 'DATA:CHECKSUM')
    value: The value of the test run (e.g checksum result will be '058116692da5f14d74a32f1db3195c30')

    """
    def __init__(self):
        self.partition_key=""
        self.sort_key=""
        self.date_modified=util.get_datetimestamp()
        self.value=""

class ResultStatusRecord:
    """
    partition_key: Most probably the s3 of the original file
    sort_key: Prefix will be 'DATA' followed by check_type seperated by colon
            (e.g. Result of checksum check would be stored as 'DATA:CHECKSUM')
    value: The status result for the tests (e.g 'SUCCESS', 'FAILURE')

    """
    def __init__(self, partition_key='', sort_key='', value=''):
        self.partition_key=partition_key
        self.sort_key=sort_key
        self.date_modified= util.get_datetimestamp()
        self.value=value


def handler(event,context):
    """
    Entry point for S3 event processing. An S3 event is essentially a dict with a list of S3 Records:
    {
        "Records": [
            {
                 "eventVersion":"2.2",
                 "eventSource":"aws:s3",
                 "awsRegion":"us-west-2",
                 "eventTime":"1970-01-01T00:00:00.000Z",
                 "eventName":"event-type",
                 "userIdentity":{
                    "principalId":"Amazon-customer-ID-of-the-user-who-caused-the-event"
                 },
                 "requestParameters":{
                    "sourceIPAddress":"ip-address-where-request-came-from"
                 },
                 "responseElements":{
                    "x-amz-request-id":"Amazon S3 generated request ID",
                    "x-amz-id-2":"Amazon S3 host that processed the request"
                 },
                 "s3":{
                    "s3SchemaVersion":"1.0",
                    "configurationId":"ID found in the bucket notification configuration",
                    "bucket":{
                       "name":"bucket-name",
                       "ownerIdentity":{
                          "principalId":"Amazon-customer-ID-of-the-bucket-owner"
                       },
                       "arn":"bucket-ARN"
                    },
                    "object":{
                       "key":"object-key",
                       "size":"object-size",
                       "eTag":"object eTag",
                       "versionId":"object version if bucket is versioning-enabled, otherwise null",
                       "sequencer": "a string representation of a hexadecimal value used to determine event sequence"
                    }
                 },
                 "glacierEventData": {
                    "restoreEventData": {
                       "lifecycleRestorationExpiryTime": "1970-01-01T00:00:00.000Z",
                       "lifecycleRestoreStorageClass": "Source storage class for restore"
                    }
                 }
            },
            ...
        ]
    }

    :param event: S3 event
    :param context: not used
    """
    logger.info(f"Start processing S3 event from result bucket:")
    logger.info(json.dumps(event))

    s3_records = event.get('Records')
    if not s3_records:
        logger.warning("Unexpected S3 event format, no Records! Aborting.")
        return

    for s3_record in s3_records:
        # routing logic goes here
        event_name = s3_record['eventName']
        s3key: str = s3_record['s3']['object']['key']
        bucket: str = s3_record['s3']['bucket']['name']

        if bucket != RESULT_BUCKET:
            logger.warn(f"Non staging bucket event received. Recived event from '{bucket}' bucket. Skipping... ")
            continue

        result_list = get_results_data(s3_key_result=s3key,bucket_name=bucket)

        dynamodb_put_item_list = []

        # Iterate and create file record accordingly
        for batch_result in result_list:

            s3_key = batch_result['staging_s3_key']
            type = batch_result['type']
            status = batch_result['status']
            value = batch_result['value']

            # STATUS record
            status_sort_key = SortKeyPrefix.STATUS + ':' + type.upper()
            status_record = ResultStatusRecord(partition_key=s3_key, sort_key=status_sort_key, value=status)
            dynamodb_put_item_list.append(status_record)

            # DATA record
            data_sort_key = SortKeyPrefix.DATA + ':' + type.upper()
            data_record = ResultDataRecord(partition_key=s3_key, sort_key=data_sort_key, value=value)
            dynamodb_put_item_list.append(data_record)

        # Write record to db
        dynamodb.batch_write_records(DYNAMODB_RESULT_TABLE_NAME, dynamodb_put_item_list)


        


def get_results_data(s3_key_result, bucket_name):
    """
    Expected result:
    [
        {
            staging_s3_key= staging_s3_key
            type = type
            value = value
            status = status
            filename = filename
        },
        ...
    ]
    """

    client_s3 = util.get_resource('s3')

    get_object_response = client_s3.get_object(
        Bucket=bucket_name,
        Key=s3_key_result,
    )

    data = get_object_response['Body'].read()

    return json.loads(data)
        