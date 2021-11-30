import json
import boto3
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

STAGING_BUCKET = os.environ.get('STAGING_BUCKET')

BUCKET_POLICY_TEMPLATE = {
    "Version": "2012-10-17",
    "Statement": []
}

POLICY_STATEMENT_TEMPLATE = {
    "Sid": "FolderLock",
    "Effect": "Deny",
    "Principal": "*",
    "Action": [
        "s3:PutObject",
        "s3:DeleteObject"
    ],
    "Resource": []
}

def find_folder_lock_statement(policy: dict):
    for policy_statement in policy.get('Statement'):
        if policy_statement.get('Sid') == "FolderLock":
            return policy_statement

    raise ValueError

def handler(event, context):
    """
    Locking event when manifest file has been uploaded.
    An example of the S3 event expected for this lambda as follows.
    {
        "Records": [
            {
                ...,
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
                ...
            },
            ...
        ]
    }

    :param event: S3 event
    :param context: not used
    """

    logger.info(f"Start processing S3 event:")
    logger.info(json.dumps(event))

    resource_arns = list()
    s3_records = event.get('Records')

    # Loop on S3 event records
    for s3_record in s3_records:
        
        # Only manipulating bucket policy of the staging bucket
        if s3_record['s3']['bucket']['name'] != STAGING_BUCKET:
            logger.warning(f"S3 record for unexpected bucket {s3_record['s3']['bucket']['name']}. Skipping.")
            continue
        
        s3key: str = s3_record['s3']['object']['key']
        obj_prefix = os.path.dirname(s3key)
        resource_arns.append(f"arn:aws:s3:::{STAGING_BUCKET}/{obj_prefix}/*")

    logger.info(f"Updating folder lock with {len(resource_arns)} resources: {resource_arns}")
    
    # Get Bucket Policy
    try:
        get_bucket_policy_response = s3.get_bucket_policy(Bucket=STAGING_BUCKET)
        logger.info("Received policy response:\n", get_bucket_policy_response)
        bucket_policy = json.loads(get_bucket_policy_response['Policy'])

    except:
        logger.warning("No Bucket policy found. Creating a brand new policy")
        bucket_policy = BUCKET_POLICY_TEMPLATE

    logger.info("Existing bucket policy:")
    logger.info(json.dumps(bucket_policy))

    # Find Lock resource
    try:
        logger.info("Grab folder lock statement")
        folder_lock_statement = find_folder_lock_statement(bucket_policy)

        folder_lock_resource = folder_lock_statement.get('Resource')
        logger.info('Folder Lock statement')
        logger.info(json.dumps(folder_lock_resource))

        # The resource could either be a list of strings or a single resource string
        # TODO: improvements: make sure there are no duplicates and sort ARNs

        # Appending existing bucket policy to the new policy
        if isinstance(folder_lock_resource, list):
            resource_arns.extend(folder_lock_resource)
        else:
            resource_arns.append(folder_lock_resource)

        # Update statement with new resource ARNs
        folder_lock_statement['Resource'] = resource_arns

    except ValueError:
        logger.info("No 'FolderLock' policy is found. Creating a new statement")
        folder_lock_statement = POLICY_STATEMENT_TEMPLATE
        folder_lock_statement['Resource'] = resource_arns
        bucket_policy['Statement'].append(folder_lock_statement)


    bucket_policy_json = json.dumps(bucket_policy)
    logger.info("New bucket policy:")
    logger.info(bucket_policy_json)

    response = s3.put_bucket_policy(Bucket=STAGING_BUCKET, Policy=bucket_policy_json)
    logger.info(f"BucketPolicy update response: {response}")

    return response
