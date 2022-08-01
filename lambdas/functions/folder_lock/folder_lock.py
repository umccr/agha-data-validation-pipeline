import json
import boto3
import os
import logging
import enum

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
FOLDER_LOCK_EXCEPTION_ROLE_ID = os.environ.get("FOLDER_LOCK_EXCEPTION_ROLE_ID")
AWS_ACCOUNT_NUMBER = os.environ.get("AWS_ACCOUNT_NUMBER")

BUCKET_POLICY_TEMPLATE = {"Version": "2012-10-17", "Statement": []}

POLICY_STATEMENT_TEMPLATE = {
    "Sid": "FolderLock",
    "Effect": "Deny",
    "Principal": "*",
    "Action": ["s3:PutObject", "s3:DeleteObject"],
    "Resource": [],
}


class TaskType(enum.Enum):
    FOLDER_LOCK = "FOLDER_LOCK"
    FOLDER_UNLOCK = "FOLDER_UNLOCK"


def find_folder_lock_statement(policy: dict):
    for policy_statement in policy.get("Statement"):
        if policy_statement.get("Sid") == "FolderLock":
            return policy_statement
    raise ValueError


def create_resource_arn_from_submission_prefix(submission_prefix: str) -> str:
    obj_prefix = os.path.dirname(submission_prefix)
    return f"arn:aws:s3:::{STAGING_BUCKET}/{obj_prefix}/*"


def handler(event, context):
    """
    Locking event when manifest file has been uploaded.
    *This will only work in STAGING bucket

    Payload
    {
        "submission_prefix": "FlagShip/SubmissionDate/Filename.txt",
        "task": "FOLDER_LOCK" or "FOLDER_UNLOCK"
    }
    """

    logger.info(f"Start processing event:")
    logger.info(json.dumps(event, indent=4))

    ################################################################################
    # Validate the event payload

    # Checking submission_prefix of payloads
    if "submission_prefix" not in event and "task" not in event:
        logger.error(
            'Expected JSON keys does not exist in the payload. Expected: ["submission_prefix", "task"]'
        )
        return

    # Checking the correct values of tasks
    task = event.get("task")
    task_list = [e.value for e in TaskType]
    if task not in task_list:
        logger.error(f"Unexpected '{task}' in payload. Expected : {task_list}")
        return

    # Construct resource arns
    submission_prefix = event.get("submission_prefix")
    resource_arn = create_resource_arn_from_submission_prefix(submission_prefix)

    ################################################################################
    # Generating new policy statement

    # Get Bucket Policy
    try:
        get_bucket_policy_response = s3.get_bucket_policy(Bucket=STAGING_BUCKET)
        logger.debug("Received policy response:\n", get_bucket_policy_response)
        bucket_policy = json.loads(get_bucket_policy_response["Policy"])
    except:
        logger.warning("No Bucket policy found. Creating a brand new policy")
        bucket_policy = BUCKET_POLICY_TEMPLATE

    logger.info(f"Existing bucket policy: {json.dumps(bucket_policy, indent=4)}")

    # Find Lock Policy
    try:
        logger.info("Grab folder lock statement")
        folder_lock_statement = find_folder_lock_statement(bucket_policy)

    except ValueError:
        logger.info("No 'FolderLock' policy is found. Creating a new statement")
        folder_lock_statement = POLICY_STATEMENT_TEMPLATE
        bucket_policy["Statement"].append(folder_lock_statement)

    folder_lock_resource = folder_lock_statement.get("Resource")

    # Will add/remove resource depending on task given to the event
    new_lock_resource = []
    if task == TaskType.FOLDER_LOCK.value:
        new_lock_resource.append(resource_arn)
        # Appending existing bucket policy to the new policy
        if isinstance(folder_lock_resource, list):
            new_lock_resource.extend(folder_lock_resource)
        else:
            new_lock_resource.append(folder_lock_resource)

    elif task == TaskType.FOLDER_UNLOCK.value:

        # Find and delete given resource from policy statement
        if isinstance(folder_lock_resource, list):
            new_lock_resource = list(set(folder_lock_resource) - {resource_arn})
        else:
            if folder_lock_resource == resource_arn:
                new_lock_resource = ""
            else:
                new_lock_resource = resource_arn

    # Update resource statement
    # Sanitize for any duplication
    new_lock_resource = list(set(new_lock_resource))

    # Sort ARNs for readability
    new_lock_resource.sort()

    # Updating resource back
    folder_lock_statement["Resource"] = new_lock_resource

    ################################################################################
    # Whitelist roles to allow modification even when it is locked at the statement

    logger.info("Update bucket policy to include condition")
    user_id = []
    for role_id in json.loads(
        FOLDER_LOCK_EXCEPTION_ROLE_ID
    ):  # Allowing batch role and cleanup lambda to delete files
        user_id.append(f"{role_id}:*")
    user_id.append(
        AWS_ACCOUNT_NUMBER
    )  # Allow the account number included in the role_id

    # Including condition object ['Condition']["StringNotLike"]["aws:userId"] from the statement
    # Ref: https://aws.amazon.com/blogs/security/how-to-restrict-amazon-s3-bucket-access-to-a-specific-iam-role/

    folder_lock_statement["Condition"] = {"StringNotLike": {"aws:userId": user_id}}

    ################################################################################
    # Update bucket policy

    bucket_policy_json = json.dumps(bucket_policy, indent=4)
    logger.info(f"New bucket policy: {bucket_policy_json}")
    response = s3.put_bucket_policy(Bucket=STAGING_BUCKET, Policy=bucket_policy_json)
    logger.info(f"BucketPolicy updated.")
    logger.debug(f"Response: {response}")

    return response
