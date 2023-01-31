from util import s3
import json
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "lambdas", "layers", "util")
sys.path.append(SOURCE_PATH)

# Environment variable that may be used in Utils
os.environ["DYNAMODB_ARCHIVE_RESULT_TABLE_NAME"] = "agha-gdr-result-bucket-archive"
os.environ["DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"] = "agha-gdr-staging-bucket-archive"
os.environ["DYNAMODB_ARCHIVE_STORE_TABLE_NAME"] = "agha-gdr-store-bucket-archive"
os.environ["DYNAMODB_ETAG_TABLE_NAME"] = "agha-gdr-e-tag"
os.environ["DYNAMODB_RESULT_TABLE_NAME"] = "agha-gdr-result-bucket"
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = "agha-gdr-staging-bucket"
os.environ["DYNAMODB_STORE_TABLE_NAME"] = "agha-gdr-store-bucket"
os.environ["STAGING_BUCKET"] = "agha-gdr-staging-2.0"
os.environ["RESULT_BUCKET"] = "agha-gdr-results-2.0"
os.environ["STORE_BUCKET"] = "agha-gdr-store-2.0"

STAGING_BUCKET = "agha-gdr-staging-2.0"
RESULT_BUCKET = "agha-gdr-results-2.0"
STORE_BUCKET = "agha-gdr-store-2.0"

##################################################################################################################
# TODO: Update the following information before running the script.
# TODO: Makesure AWS_PROFILE is set for the script to run
"""
To run the script:
cd scripts/handle_delete_files
python3 main.py

"""
DRY_RUN = False
FILE_BUCKET_LOCATION = "agha-gdr-store-2.0"
S3_DELETION_LIST = [...]

##################################################################################################################


def delete_file():
    # Find directory prefix
    directory_prefix = set()
    for key in S3_DELETION_LIST:
        prefix = "/".join(key.split("/")[:-1]) + "/"
        directory_prefix.add(prefix)
    directory_prefix = list(directory_prefix)

    #########################################################
    # Deleting object file from bucket
    if not DRY_RUN:
        res = s3.delete_s3_object_from_key(STORE_BUCKET, S3_DELETION_LIST)
        print(res)
    else:
        print(f"Deleting: {json.dumps(S3_DELETION_LIST, indent=4)}")

    #########################################################
    # Delete associated results data from the file at Results bucket

    if not DRY_RUN:
        for submission in directory_prefix:
            os.system(
                f"python ../util/sync_results_bucket.py --sync_from {FILE_BUCKET_LOCATION} --sort_key_prefix {submission}"
            )
    else:
        print(
            f"Sync results bucket for the following submissions: {json.dumps(directory_prefix, indent=4)}"
        )

    #########################################################
    # Update new manifest.txt file at store bucket

    if not DRY_RUN and FILE_BUCKET_LOCATION == STORE_BUCKET:
        for submission in directory_prefix:
            os.system(
                f"python ../util/manifest_txt_update.py --s3_key_object {submission}"
            )
    elif DRY_RUN:
        print(
            f"Updating `manifest.txt` for the following submissions: {json.dumps(directory_prefix, indent=4)}"
        )
    else:
        print("Not updating manifest.txt")


if __name__ == "__main__":
    print(len(S3_DELETION_LIST))
    delete_file()
