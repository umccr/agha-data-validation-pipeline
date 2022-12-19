#!/usr/bin/env python3
import json
import logging
import os
import boto3

import util
from util import s3, agha, batch

STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
STORE_BUCKET = os.environ.get("STORE_BUCKET")

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_CLIENT = boto3.client("s3")


def handler(event, context):
    """
    This function will delete object from staging bucket.
    Object deleted will ony be index file and uncompressed file.

    {
        "submission_directory": "AC/2022-02-02/",
        "skip_manifest_check": True
    }
    """
    logger.info("event:", event)
    submission_directory = event.get("submission_directory")
    skip_manifest_check = event.get("skip_manifest_check")

    if submission_directory is None:
        return "Invalid payload"
    if skip_manifest_check is None:
        skip_manifest_check = False

    ################################################################################
    # First stage - Check if content of bucket is ok to delete
    ################################################################################
    # List of all files in the bucket

    # Check if store bucket contain all files in manifest.orig
    if not skip_manifest_check:
        missing_store_file = batch.run_manifest_orig_store_check(submission_directory)
        logger.info(
            f"Missing file from manifest.orig in store bucket: {json.dumps(missing_store_file, indent=4)}"
        )
        if len(missing_store_file) > 0:
            logger.error("Missing file from in store bucket from manifest.orig")
    else:
        logger.info("Skipping original manifest check.")

    try:
        metadata_list = s3.get_s3_object_metadata(
            bucket_name=STAGING_BUCKET, directory_prefix=submission_directory
        )
        s3_key_list = [metadata["Key"] for metadata in metadata_list]
    except ValueError:

        # All files submission might all be moved to store bucket
        # Checking submission in store bucket
        try:
            store_file_list = s3.get_s3_object_metadata(
                STORE_BUCKET, submission_directory
            )
            if len(store_file_list) > 0:
                logger.info("All file has been transferred to store bucket")
                s3_key_list = []
            else:
                raise ValueError("No data found")
        except ValueError:

            logger.error(f"No '{submission_directory}' found in staging bucket")
            return {"reason": f"No '{submission_directory}' found in staging bucket"}

    deletion_key_list = []
    for s3_key in s3_key_list:

        # Allowed index and non-compressed but compressible file to be in the deletion list
        if agha.FileType.is_index_file(s3_key) or (
            agha.FileType.is_compressable_file(s3_key)
            and not agha.FileType.is_compress_file(s3_key)
        ):
            deletion_key_list.append(s3_key)
    ################################################################################
    # Second Stage - Delete files
    ################################################################################

    # If deletion key_list exist, delete the file
    if len(deletion_key_list) > 0:
        try:
            logger.info(
                f"Deleting file from staging bucket, List of s3_key: {json.dumps(deletion_key_list, indent=4)} "
            )
            res = s3.delete_s3_object_from_key(
                bucket_name=STAGING_BUCKET, key_list=deletion_key_list
            )
            logger.debug(f"Deletion response: {json.dumps(res, indent=4)}")
            logger.info(f"Deletion job success!")

        except Exception as e:
            logger.error("Something went wrong on deleting s3 keys")
            logger.error(e)
            return f"Something went wrong. Error: {e}"
    else:
        logger.info(f"No data to delete, proceeding next stage")

    ################################################################################
    # Third stage - Create Readme to not use this bucket directory again.
    ################################################################################
    logger.info(f"Creating README.txt file")
    readme_key = submission_directory.strip("/") + "/" + "README.txt"
    file_content = (
        f"Submission in '{submission_directory}' has successfully stored in the store bucket.\n"
        f"Please check store bucket if you need to access the content.\n"
        f"If you need to upload/modify the content, please submit as a new submission.\n"
        f"This folder is now locked to prevent overlapping with succeeded submission. \n"
    )

    s3.upload_s3_object_from_string(
        bucket_name=STAGING_BUCKET,
        byte_of_string=file_content,
        s3_key_destination=readme_key,
    )
    logger.info("Uploaded README file")
