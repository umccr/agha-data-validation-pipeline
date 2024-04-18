import json
import os
import sys

import pandas as pd
import numpy as np
import collections

from typing import Dict, List
import util
from util import agha, s3, dynamodb, submission_data

BUCKET_NAME_STORE = "agha-gdr-store-2.0"
DYNAMODB_STORE_ARCHIVE_TABLE = "agha-gdr-store-bucket-archive"
DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"

LEGACY_SUBS = {
    "acute_care_genomics": "2019-05-16",
    "brain_malformations": "2019-07-22",
    "leukodystrophies": "2019-08-12",
    "mitochondrial_disease": "2019-04-18",
}

submission_skip_list = [
    "GI/2020-06-04/",
    "GI/2019-08-27/",
    "Mito/2019-04-18/",
    "HIDDEN/2019-09-27/",
]


def find_new_from_old_s3_key(old_s3_key_path: str):
    old_flagship = old_s3_key_path.split("/")[0]
    filename = s3.get_s3_filename_from_s3_key(old_s3_key_path)
    new_flagship_name = agha.FlagShip.from_name(old_flagship).preferred_code()

    if old_flagship in LEGACY_SUBS.keys():
        submission_prefix = LEGACY_SUBS[old_flagship]
        new_s3_key = new_flagship_name + "/" + submission_prefix + "/" + filename
    else:
        submission_postfix = old_s3_key_path[old_s3_key_path.index("/") :]
        new_s3_key = new_flagship_name + submission_postfix

    return new_s3_key


def parse_pandas_with_true_consent():
    f = open("agha_store_temp.json")
    data = json.load(f)
    f.close()

    pd_df = pd.json_normalize(data)
    pd_df.fillna(value=False, inplace=True)

    filtered_pd = pd_df.loc[pd_df["consent"] == bool(True)]

    return filtered_pd


def tag_object(bucket: str, key: str, tag_set: List[dict]):
    """
    Add tags to an S3 object.
    TagSet in the form:
    [
        {
            'Key': 'key1',
            'Value': 'value1'
        },
        {
            'Key': 'key2',
            'Value': 'value2'
        }
    ]

    :param bucket: the S3 bucket name the object is in
    :param key: the full S3 key of the object
    :param tag_set: the List[dict] of tag key/value pairs
    :return: the response of the tagging request
    """

    client_s3 = util.get_client("s3")

    response = client_s3.put_object_tagging(
        Bucket=bucket, Key=key, Tagging={"TagSet": tag_set}
    )
    return response


def tag_s3_key(s3_key_list):
    tag_set = [{"Key": "Consent", "Value": "True"}]

    success_array = []
    error_array = []
    for s3_key in s3_key_list:

        print("tagging: ", s3_key)

        try:
            tag_resp = tag_object(bucket=BUCKET_NAME_STORE, key=s3_key, tag_set=tag_set)
            success_array.append(s3_key)
        except:
            error_array.append(s3_key)

    return success_array, error_array


def update_dynamodb(s3_key_list):
    update_array = []

    # Fetch existing data
    for new_s3_key in s3_key_list:
        print("Fetching: ", new_s3_key)
        pk = dynamodb.FileRecordPartitionKey.FILE_RECORD.value
        dy_res = dynamodb.get_item_from_exact_pk_and_sk(
            table_name=DYNAMODB_STORE_TABLE, partition_key=pk, sort_key=new_s3_key
        )

        dy_item = dy_res["Items"][0]
        dy_item["Consent"] = True

        update_array.append(dy_item)

    print(json.dumps(update_array, indent=4, cls=util.JsonSerialEncoder))

    # Batch update array
    print("Updating dydb...")
    dynamodb.batch_write_objects(DYNAMODB_STORE_TABLE, update_array)
    dynamodb.batch_write_objects_archive(
        DYNAMODB_STORE_ARCHIVE_TABLE, update_array, "ObjectUpdate"
    )

    return update_array


def main():
    parsed_pd = parse_pandas_with_true_consent()

    print(len(parsed_pd))
    sys.exit(1)

    s3_key_list = []
    for row in parsed_pd.itertuples():

        s3_key = row.s3_key
        new_s3_key = find_new_from_old_s3_key(s3_key)

        # Skipping skipped files
        submission_prefix = os.path.dirname(new_s3_key) + "/"
        if submission_prefix in submission_skip_list:
            continue

        if submission_data.is_file_skipped(filename=s3_key):
            continue

        # Only works for compressed file
        if agha.FileType.is_compressable_file(
            new_s3_key
        ) and not agha.FileType.is_compress_file(new_s3_key):
            new_s3_key += ".gz"

        s3_key_list.append(new_s3_key)

        if agha.FileType.from_name(new_s3_key) == agha.FileType.VCF:
            new_s3_key += ".tbi"

        elif agha.FileType.from_name(new_s3_key) == agha.FileType.BAM:
            new_s3_key += ".bai"

        else:
            # Continue to next s3_key value
            continue

        s3_key_list.append(new_s3_key)

    # Start tagging
    print("Tagging array generated:", json.dumps(s3_key_list, indent=4))
    success_array, error_array = tag_s3_key(s3_key_list)

    # Result writing
    result = {"success_array": success_array, "error_array": error_array}
    f = open("result_tagging002.json", "w")
    f.write(json.dumps(result, indent=4, cls=util.JsonSerialEncoder))
    f.close()

    print("Success :", len(success_array))
    print("Error: ", len(error_array))

    update_dynamodb(s3_key_list)


if __name__ == "__main__":
    main()
