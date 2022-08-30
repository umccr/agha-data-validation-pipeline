#!/usr/bin/env python3
import json
import logging
import os
import uuid
import re
import sys

import util
from util import s3

S3_CLIENT = util.get_client("s3")
IAM_CLIENT = util.get_client("iam")

JOB_NAME_RE = re.compile(r"[.\\/]")

# Environment Variables
S3_DATA_SHARING_BATCH_QUEUE_NAME = os.environ.get("S3_DATA_SHARING_BATCH_QUEUE_NAME")
S3_DATA_SHARING_JOB_DEFINITION_ARN = os.environ.get(
    "S3_DATA_SHARING_JOB_DEFINITION_ARN"
)
STORE_BUCKET = os.environ.get("STORE_BUCKET")
DYNAMODB_STORE_TABLE_NAME = os.environ.get("DYNAMODB_STORE_TABLE_NAME")

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


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

    validate_event(event)

    # Parsing Input
    destination_s3_arn = event["destination_s3_arn"].strip("*").strip("/")
    source_s3_key_list = event["source_s3_key_list"]
    destination_s3_key_prefix = event["destination_s3_key_prefix"].strip("/")
    destination_bucket_name = destination_s3_arn.split(":::")[-1]

    ################################################
    # Check bucket location
    # The reason that we transfer via S3 is to prevent any egress cost.
    # If bucket location is not at the same region. Egress cost will still occur.

    bucket_location = s3.get_s3_location(destination_bucket_name)
    logger.info(f"Destination bucket location: {bucket_location}")

    if bucket_location != "ap-southeast-2":
        message = f"Expected in 'ap-southeast-2'"
        logger.error(message)
        return message

    ################################################
    # Prepare batch jobs
    logger.info("List of jobs need to be copied")

    batch_job_list = []
    curr_datetimestamp = util.get_datetimestamp()
    for source_s3_key in source_s3_key_list:
        filename = s3.get_s3_filename_from_s3_key(source_s3_key)

        # Construct destination S3 key
        destination_s3_key_prefix = (
            f"{destination_s3_key_prefix}/" if destination_s3_key_prefix else ""
        )
        destination_timestamp_prefix = f"{curr_datetimestamp}/"  # Adding trailing / for
        destination_s3_key = (
            f"{destination_s3_key_prefix}{destination_timestamp_prefix}{filename}"
        )

        source_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(
            bucket_name=STORE_BUCKET, s3_key=source_s3_key
        )
        destination_s3_uri = s3.create_s3_uri_from_bucket_name_and_key(
            bucket_name=destination_bucket_name, s3_key=destination_s3_key
        )

        s3_sync_command = s3.create_s3_cp_command_from_s3_uri(
            source_s3_uri, destination_s3_uri
        )

        batch_job = create_s3_data_sharing_batch_job_from_s3_cli_command(
            source_s3_key=source_s3_key, s3_cli_command=s3_sync_command
        )

        batch_job_list.append(batch_job)

    ################################################
    # Submitting job
    logger.info(f"Batch Job list: {json.dumps(batch_job_list, indent=4)}")
    for job_data in batch_job_list:
        submit_s3_data_sharing_batch_job(job_data)
    logger.info(f"Batch job has executed. Submit {len(batch_job_list)} number of job")


def create_s3_data_sharing_batch_job_from_s3_cli_command(
    source_s3_key: str, s3_cli_command: str
):
    name_raw = f"agha_s3_data_sharing_{source_s3_key}"
    name = JOB_NAME_RE.sub("_", name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f"{name[:120]}_{uuid.uuid1().hex[:7]}"

    command = s3_cli_command.split()

    return {"name": name, "command": command}


def submit_s3_data_sharing_batch_job(job_data):
    client_batch = util.get_client("batch")

    command = job_data["command"]

    client_batch.submit_job(
        jobName=job_data["name"],
        jobQueue=S3_DATA_SHARING_BATCH_QUEUE_NAME,
        jobDefinition=S3_DATA_SHARING_JOB_DEFINITION_ARN,
        containerOverrides={"command": command},
    )


def validate_event(event):
    # Check payload existence
    if "destination_s3_arn" not in event:
        logger.error("Invalid event. `destination_s3_arn` is expected")
        sys.exit(0)
    if "destination_s3_key_prefix" not in event:
        logger.error("Invalid event. `destination_s3_key_prefix` is expected")
        sys.exit(0)
    if "source_s3_key_list" not in event:
        logger.error("Invalid event. `source_s3_key_list` is expected")
        sys.exit(0)

    # Check payload content
    if not isinstance(event["destination_s3_arn"], str) and not event[
        "destination_s3_arn"
    ].startswith("arn:aws:s3:::"):
        logger.error("Invalid `destination_s3_arn` payload.")
        sys.exit(0)

    if not isinstance(event["destination_s3_key_prefix"], str):
        logger.error("Invalid `destination_s3_key_prefix` payload.")
        sys.exit(0)

    if not isinstance(event["source_s3_key_list"], list):
        logger.error("Invalid `source_s3_key_list` payload.")
        sys.exit(0)
