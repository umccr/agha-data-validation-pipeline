#!/usr/bin/env python3
import os
import argparse
import sys
import json
import logging
import time

import pandas as pd

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "lambdas", "layers", "util")
sys.path.append(SOURCE_PATH)

import util
from util import agha, s3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Setting some variables

# Buckets
STAGING_BUCKET = os.environ.get("STAGING_BUCKET")
STORE_BUCKET = os.environ.get("STORE_BUCKET")
RESULT_BUCKET = os.environ.get("RESULT_BUCKET")

# Dynamodb
DYNAMODB_STAGING_TABLE_NAME = os.environ.get("DYNAMODB_STAGING_TABLE_NAME")
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = os.environ.get(
    "DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"
)
DYNAMODB_STORE_TABLE_NAME = os.environ.get("DYNAMODB_STORE_TABLE_NAME")
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = os.environ.get("DYNAMODB_ARCHIVE_STORE_TABLE_NAME")
DYNAMODB_RESULT_TABLE_NAME = os.environ.get("DYNAMODB_RESULT_TABLE_NAME")
DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = os.environ.get(
    "DYNAMODB_ARCHIVE_RESULT_TABLE_NAME"
)
DYNAMODB_ETAG_TABLE_NAME = os.environ.get("DYNAMODB_ETAG_TABLE_NAME")

FLAGSHIP_LIST_CHOICE = list(
    set(agha.FlagShip.list_flagship_enum()) - {"TEST", "UNKNOWN"}
)


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket_name",
        required=True,
        type=str,
        choices=[STAGING_BUCKET, STORE_BUCKET],
        help="The bucket name to search (either the staging/store name)",
    )
    parser.add_argument(
        "--flagship",
        required=True,
        type=str,
        choices=FLAGSHIP_LIST_CHOICE,
        help="Directory prefix for the submission. Example: EE",
    )
    return parser.parse_args()


def find_and_duplicates(bucket_name, flagship):
    ################################################################################
    # Finding what to delete

    if not flagship.endswith("/"):
        flagship += "/"

    list_of_metadata = s3.get_s3_object_metadata(
        bucket_name=bucket_name, directory_prefix=flagship
    )

    metadata_df = pd.json_normalize(list_of_metadata)

    etag_df = metadata_df["ETag"]
    duplicates_record = metadata_df[
        etag_df.isin(etag_df[etag_df.duplicated()])
    ].sort_values(by="Key")
    print(f"Number of duplication: {len(duplicates_record)}")

    # Alternative:
    # etag_count_df = metadata_df.ETag.value_counts()
    # duplicates_record = metadata_df[metadata_df.ETag.isin(etag_count_df.index[etag_count_df.gt(1)])]

    # Group Etag so only 1 file are kept based on the first submission appear (sorted by Key)
    records_to_keep = (
        duplicates_record.sort_values(by="Key").groupby("ETag", as_index=False).first()
    )
    print(f"Number of records to keep: {len(records_to_keep)}")

    # Find the deletion list (difference between duplicated_df and records_to_keep_df)
    to_delete_df = pd.concat([duplicates_record, records_to_keep]).drop_duplicates(
        keep=False
    )
    print(f"Number of records to delete: {len(to_delete_df)}")

    duplicate_list_to_delete = to_delete_df["Key"].tolist()

    deletion_s3_list = duplicate_list_to_delete.copy()

    # Add index file to the deletion list if the main file is in the deletion list
    for key in [
        s3_key
        for s3_key in duplicate_list_to_delete
        if not agha.FileType.is_index_file(s3_key)
        and agha.FileType.from_name(s3_key).get_name()
    ]:

        try:
            if key.endswith("vcf.gz"):
                key += ".tbi"

            elif key.endswith(".bam"):
                key += ".bai"

            else:
                continue

            util.get_record_from_given_field_and_panda_df(metadata_df, "Key", key)
            deletion_s3_list.append(key)
        except IndexError:
            continue

    ################################################################################
    # Deleting ...

    print(
        f"Number of Index files deleted: {len([key for key in deletion_s3_list if agha.FileType.is_index_file(key)])}"
    )
    print(f"File to delete from s3 store: {json.dumps(deletion_s3_list, indent=4)}")

    s3.delete_s3_object_from_key(bucket_name=bucket_name, key_list=deletion_s3_list)

    ################################################################################
    # Link associated file after post process after deletion

    time.sleep(
        5
    )  # Just some buffer time to let lambda s3_event listener to finish execute

    submission_prefix_list = list(
        set([("/".join(key.split("/")[:-1]) + "/") for key in deletion_s3_list])
    )
    submission_prefix_list.sort()

    for submission in submission_prefix_list:

        if bucket_name == STORE_BUCKET:
            # Update manifests.txt via 'manifest_txt_update.py' file
            os.system(
                f"python ../util/manifest_txt_update.py --s3_key_object {submission}"
            )

        # Update results bucket content via sync python file
        os.system(
            f"python ../util/sync_results_bucket.py --sync_from {STORE_BUCKET} --sort_key_prefix {submission}"
        )


if __name__ == "__main__":
    args = get_arguments()

    find_and_duplicates(args.bucket_name, args.flagship)
