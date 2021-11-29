import os
import logging
import json
import boto3

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
MANIFEST_PROCESSOR_LAMBDA_ARN = os.environ.get('MANIFEST_PROCESSOR_LAMBDA_ARN')
FOLDER_LOCK_LAMBDA_ARN = os.environ.get('FOLDER_LOCK_LAMBDA_ARN')
S3_RECORDER_LAMBDA_ARN = os.environ.get('S3_RECORDER_LAMBDA_ARN')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda')


def call_lambda(lambda_arn: str, payload: dict):
    response = lambda_client.invoke(
        FunctionName=lambda_arn,
        InvocationType='Event',
        Payload=json.dumps(payload)
    )
    return response


def handler(event, context):
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
    logger.info(f"Start processing S3 event:")
    logger.info(json.dumps(event))

    s3_records = event.get('Records')
    if not s3_records:
        logger.warning("Unexpected S3 event format, no Records! Aborting.")
        return

    # split event records into manifest and others
    # manifest events will be acted on by the validation and folder lock lambdas
    # non manifest events will be passed on to the recorder lambda for persisting into DynamoDB
    manifest_records = list()
    non_manifest_records = list()
    for s3_record in s3_records:
        # routing logic goes here
        event_name = s3_record['eventName']
        s3key: str = s3_record['s3']['object']['key']
        bucket: str = s3_record['s3']['bucket']['name']

        if bucket != STAGING_BUCKET:
            logger.warning(f"Non staging bucket event received. Received event from '{bucket}' bucket. Skipping... ")
            continue


        if s3key.endswith('manifest.txt') and 'ObjectCreated' in event_name:
            manifest_records.append(s3_record)
        else:
            non_manifest_records.append(s3_record)

    logger.info(f"Processing {len(manifest_records)}/{len(non_manifest_records)} manifest/non-manifest events.")

    # Call S3 recorder lambda for every file uplaoded
    ser_res = call_lambda(S3_RECORDER_LAMBDA_ARN, {"Records": s3_records})
    logger.info(f"S3 Event Recorder Lambda call response: {ser_res}")

    # call corresponding lambda functions
    # for manifest related events and others
    if len(manifest_records) > 0:

        # Call folder lock lambda
        folder_lock_response = call_lambda(FOLDER_LOCK_LAMBDA_ARN, {"Records": manifest_records})
        logger.info(f"Folder Lock Lambda call response: {folder_lock_response}")

        # Call validation lambda
        validation_response = call_lambda(MANIFEST_PROCESSOR_LAMBDA_ARN, {"Records": manifest_records})
        logger.info(f"Validation Lambda call response: {validation_response}")

