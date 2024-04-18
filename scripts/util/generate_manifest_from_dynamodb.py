import sys
import logging
import util.s3 as s3
import util.dynamodb as dy
import util.dynamodb as dynamodb
import util.agha as agha
import util as util
import util.batch as batch
import json
import time
import os
from boto3.dynamodb.conditions import Attr

import botocore

STORE_BUCKET_NAME = "agha-gdr-store-2.0"
STAGING_BUCKET_NAME = "agha-gdr-staging-2.0"
RESULTS_BUCKET_NAME = "agha-gdr-results-2.0"

DYNAMODB_STAGING_TABLE = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"


def generate_manifest_from_dydb():

    flagship_list = s3.aws_s3_ls(STORE_BUCKET_NAME, "")

    for flagship in flagship_list:

        submission_date_list = s3.aws_s3_ls(STORE_BUCKET_NAME, flagship)

        for submission in submission_date_list:

            print("Processing: ", submission)

            manifest_item = dynamodb.get_batch_item_from_pk_and_sk(
                DYNAMODB_STORE_TABLE,
                dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                submission,
            )

            # Create temporary manifest.txt
            manifest_filename = "manifest.txt"
            f = open(manifest_filename, "w")

            first_line = "checksum\tfilename\tagha_study_id\n"
            f.write(first_line)
            for item in manifest_item:
                checksum = item["provided_checksum"]
                filename = item["filename"]
                agha_study_id = item["agha_study_id"]

                item_line = f"{checksum}\t{filename}\t{agha_study_id}\n"

                f.write(item_line)
            f.close()

            destination_s3_key = submission + manifest_filename
            s3.upload_s3_object_local_file(
                bucket_name=STORE_BUCKET_NAME,
                local_file=manifest_filename,
                s3_key_destination=destination_s3_key,
            )

            # Removing
            os.remove(manifest_filename)


if __name__ == "__main__":
    generate_manifest_from_dydb()
