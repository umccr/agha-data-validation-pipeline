import json
import os
import time
from boto3.dynamodb.conditions import Attr
import pandas as pd

from util import dynamodb, submission_data

STORE_BUCKET = "agha-gdr-store-2.0"
DYNAMODB_STORE_TABLE_NAME = "agha-gdr-store-bucket"


def read_lines_from_file(filename):
    f = open(filename, "r")
    some_file = f.read()
    f.close()

    split_lines = some_file.split("\n")
    del split_lines[0]

    return split_lines


def update_files_from_new_manifest():
    ################################################
    # TODO: fill the information

    NEW_MANIFEST_PATH = "File/to/new/manifest"  # or filename if is in this directory
    SUBMISSION_PREFIX = "Flagship/Submissions/"

    ################################################
    job_list = []

    # Getting current data
    list_of_files = dynamodb.get_batch_item_from_pk_and_sk(
        DYNAMODB_STORE_TABLE_NAME,
        dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
        SUBMISSION_PREFIX,
    )
    current_data_pd = pd.json_normalize(list_of_files)

    # Getting new data
    new_manifest = read_lines_from_file(NEW_MANIFEST_PATH)
    for line in new_manifest:
        split_line = line.split("\t")

        filename = split_line[1]
        correct_study_id = split_line[2]

        current_study_id = submission_data.find_study_id_from_manifest_df_and_filename(
            current_data_pd, filename
        )

        if correct_study_id != current_study_id:
            job_list.append(
                {
                    "key": f"{SUBMISSION_PREFIX}{filename}",
                    "override_values": {"agha_study_id": f"{correct_study_id}"},
                }
            )

    print("To Process: \n", json.dumps(job_list, indent=4))
    print("Number of job: ", len(job_list))

    # Update the file value without updating the manifest.txt
    # Changes can happen more than once but manifest.txt only need to be updated once
    for job in job_list:
        key = job["key"]
        os.system(
            f"./execute.sh "
            f"{STORE_BUCKET} "
            f"{dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value} "
            f"{key} "
            f"{key} "
            f"""'{json.dumps(job['override_values'])}' """  # Put empty object for no changes. Example: '{}'
            f"false"
        )
        time.sleep(1)  # Allow for time buffering

    # Update just the manifest.txt
    # Any key will work, as it will look up from the directory prefix
    key_manifest = job_list[0]["key"]
    os.system(
        f"./execute.sh "
        f"{STORE_BUCKET} "
        f"{dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value} "
        f"{key_manifest} "
        f"{key_manifest} "
        """'{}' """  # Put empty object for no changes. Example: '{}'
        f"true"
    )


def execute_change():
    ################################################
    # TODO: fill the information
    filter_expr = Attr("agha_study_id").eq("XXXXXX")
    SUBMISSION_PREFIX = "Flagship/Submissions/"

    ################################################

    list_of_files = dynamodb.get_batch_item_from_pk_and_sk(
        DYNAMODB_STORE_TABLE_NAME,
        dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
        SUBMISSION_PREFIX,
        filter_expr=filter_expr,
    )
    key_of_list = [metadata["sort_key"] for metadata in list_of_files]
    print(json.dumps(list_of_files, indent=4))
    print(len(list_of_files))

    for key in key_of_list:
        new_key = f"NewFlagship/Submission/{key.split('/')[-1]}"

        os.system(
            f"./execute.sh "
            f"{STORE_BUCKET} "
            f"{dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value} "
            f"{key} "
            f"{new_key} "
            f"""'{json.dumps({'agha_study_id': 'abcde'})}' """  # Put empty object for no changes. Example: '{}'
            f"true"
        )
        time.sleep(1)  # Allow for time buffering


if __name__ == "__main__":
    # TODO: Fill in the section at the beginning of the function
    # Just run ONE of the function below depending what is needed.

    # Execute change for individual changes Fill in the
    execute_change()

    # Execute change from a new manifest file (will read a new local manifest file)
    update_files_from_new_manifest()
