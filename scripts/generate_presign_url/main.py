import json
import os
import sys
import pandas as pd
from enum import Enum
from typing import Dict, List
from boto3.dynamodb.conditions import Attr

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "lambdas", "layers", "util"
)
sys.path.append(SOURCE_PATH)

import util
from util import dynamodb, agha

DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = 'agha-gdr-result-bucket-archive'
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = 'agha-gdr-staging-bucket-archive'
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = 'agha-gdr-store-bucket-archive'
DYNAMODB_ETAG_TABLE_NAME = 'agha-gdr-e-tag'
DYNAMODB_RESULT_TABLE_NAME = 'agha-gdr-result-bucket'
DYNAMODB_STAGING_TABLE_NAME = 'agha-gdr-staging-bucket'
DYNAMODB_STORE_TABLE_NAME = 'agha-gdr-store-bucket'
STAGING_BUCKET = 'agha-gdr-staging-2.0'
RESULT_BUCKET = 'agha-gdr-results-2.0'
STORE_BUCKET = 'agha-gdr-store-2.0'

CLIENT_S3 = util.get_client('s3')

##################################################################################################################
# TODO: Update the following information before running the script.
# TODO: Make sure AWS_PROFILE is set for the script to run
# NOTE: Only works in store bucket
"""
To run the script:
cd scripts/generate_presign_url
python3 main.py
"""


def get_argument():
    return {
        "dry_run": False,
        "agha_study_id_list": ['A000000'],
        "filetype_list": ["BAM", "BAM_INDEX", "VCF"],  # Options: BAM, VCF, FASTQ. Default: All filetype
        "flagship": "AC"  # Please refer to preferred_code() in class in https://github.com/umccr/agha-data-validation-pipeline/blob/d17f55d8642dff0921b088f18884c50a536c12d8/lambdas/layers/util/util/agha.py#L9
    }


def generate_presign_s3_url(agha_study_id_list: List[str], flagship: str, filetype_list: List[str], dry_run: bool):
    sort_key_flagship_prefix = agha.FlagShip.from_name(flagship).preferred_code()
    filetype_list = run_filetype_sanitize(filetype_list)
    file_metadata_list = []

    # Find files with relevant study_id, flagship, and filetype
    for study_id in agha_study_id_list:

        # study_id filter
        filter_expr = Attr('agha_study_id').eq(study_id)

        # filetype filter
        if filetype_list:
            filetype_attr_expr = Attr('filetype').eq(filetype_list[0])  # Init expression
            for filetype in filetype_list[1:]:
                filetype_attr_expr = filetype_attr_expr | Attr('filetype').eq(filetype)  # Appending expression
            filter_expr = filter_expr & filetype_attr_expr

        # query to dydb
        file_list = dynamodb.get_batch_item_from_pk_and_sk(table_name=DYNAMODB_STORE_TABLE_NAME,
                                                           partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                           sort_key_prefix=sort_key_flagship_prefix,
                                                           filter_expr=filter_expr)
        file_metadata_list.extend(file_list)

        # Logging
        sort_key_list = [metadata['sort_key'] for metadata in file_list]
        print(
            f"File with matching study_id ({study_id}) and flagship ({sort_key_flagship_prefix}) are: {json.dumps(sort_key_list, indent=4)}")

    if dry_run:
        return

    # Generate pre-sign url from metadata list
    for file_metadata in file_metadata_list:
        s3_key = file_metadata['sort_key']
        presigned_url = CLIENT_S3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': STORE_BUCKET, 'Key': s3_key},
            ExpiresIn=604800  # 7 full days
        )

        # Append presign_url to metadata
        file_metadata['presigned_url'] = presigned_url

    # Write output file
    output_filename = f"presign_url_{sort_key_flagship_prefix}_{json.dumps(agha_study_id_list)}.txt"
    f = open(output_filename, 'w')
    f.write("agha_study_id\tfilename\tchecksum\tpresigned_url\n")
    for metadata in file_metadata_list:
        # parse data
        study_id = metadata["agha_study_id"]
        filename = metadata["filename"]
        checksum = metadata["provided_checksum"]
        presigned_url = metadata["presigned_url"]

        f.write(f"{study_id}\t{filename}\t{checksum}\t{presigned_url}\n")
    f.close()

    print(f"PresignUrl generated. Checkout: {output_filename}")


def run_filetype_sanitize(filetype_list: List[str]):
    result = set()
    for filetype in filetype_list:
        filetype_enum = agha.FileType.from_enum_name(filetype)
        # Raise for unknown filetype
        if filetype_enum == agha.FileType.UNSUPPORTED:
            raise ValueError

        # Add corresponding index when necessary
        if agha.FileType.is_indexable(filetype):
            if filetype_enum == agha.FileType.VCF:
                result.add(agha.FileType.VCF_INDEX.get_name())
            elif filetype_enum == agha.FileType.BAM:
                result.add(agha.FileType.BAM_INDEX.get_name())
            elif filetype_enum == agha.FileType.CRAM:
                result.add(agha.FileType.CRAM_INDEX.get_name())

        result.add(filetype_enum.get_name())
    return list(result)


if __name__ == '__main__':
    # parse_from_excel_by_pandas()  # For custom script

    args = get_argument()
    generate_presign_s3_url(**args)
