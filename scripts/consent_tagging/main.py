import json
import os
import sys
import pandas as pd
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

##################################################################################################################
# TODO: Update the following information before running the script.
# TODO: Make sure AWS_PROFILE is set for the script to run
# NOTE: Only works in store bucket
"""
To run the script:
cd scripts/consent_tagging
python3 main.py
"""

def get_argument():
    return {
        "dry_run": True,
        "agha_study_id_list": ['A0128001'],
        "flagship": 'GI'
    }


def parse_from_excel_by_pandas():
    """
    This function is made specifically for consent tagging request from Excel workbook.
    Details and files: https://trello.com/c/Bbys40EC

    To use this function, comment the content of main function and call this function instead.
    Remove:
        args = get_argument()
        run_modify_tagging(**args)
    Add:
        parse_from_excel_by_pandas()
    """

    for flagship in ['AC', 'Mito', 'EE', 'GI', 'Leuko', 'BM', 'chILDRANZ', 'KidGen']:
        try:
            pd_df = pd.read_excel('consent_tagging_sc_20220321_excl_ICCon.xlsx', flagship)
        except ValueError:
            continue

        pd_df = pd_df.loc[pd_df['What kind of data sharing is the participant eligible for?'] == 'All ethically-approved research projects']

        agha_study_id_list = pd_df['Study Number:'].to_list()
        sort_key_modify_list = run_modify_tagging(agha_study_id_list=agha_study_id_list, flagship=flagship,
                                                  dry_run=False)

        # Appending to report
        f = open('tagging_result.txt', 'a')
        f.write(f'Flagship: {flagship}\n')
        f.write(f'agha_study_id list: {agha_study_id_list}\n')
        f.write(f'Number of files affected: {len(sort_key_modify_list)}\n')
        f.write(f'Affected files: {json.dumps(sort_key_modify_list, indent=4)}\n')
        f.write('------------------------------------------------------------------------------------\n')
        f.close()


def run_modify_tagging(agha_study_id_list: List, flagship: str, dry_run: bool):
    sort_key_flagship_prefix = agha.FlagShip.from_name(flagship).preferred_code()
    s3_key_list_to_tag = []

    # Grab sort_key from agha_study_id and flagship
    for study_id in agha_study_id_list:
        filter_expr = Attr('agha_study_id').eq(study_id)

        file_list = dynamodb.get_batch_item_from_pk_and_sk(table_name=DYNAMODB_STORE_TABLE_NAME,
                                                           partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                           sort_key_prefix=sort_key_flagship_prefix,
                                                           filter_expr=filter_expr)
        s3_key_list_to_tag.extend([metadata['sort_key'] for metadata in file_list])

    # Start tagging object
    print(f'Tagging the `Consent` for the following list {(len(s3_key_list_to_tag))}:',
          json.dumps(s3_key_list_to_tag, indent=4))
    if dry_run:
        return s3_key_list_to_tag
    success_array, error_array = tag_object_from_s3_key_list(s3_key_list_to_tag)

    # Result writing and logging
    if error_array:
        print(f'Error files: {error_array}')
        err_file = open(f'tagging-error-{util.get_datetimestamp()}.json', 'w')
        err_file.write(json.dumps(error_array))
        err_file.close()

    update_dynamodb(s3_key_list_to_tag)

    return s3_key_list_to_tag


def update_dynamodb(s3_key_list):
    update_array = []

    # Fetch existing data
    for new_s3_key in s3_key_list:
        print('Processing: ', new_s3_key)
        pk = dynamodb.FileRecordPartitionKey.FILE_RECORD.value
        dy_res = dynamodb.get_item_from_exact_pk_and_sk(table_name=DYNAMODB_STORE_TABLE_NAME,
                                                        partition_key=pk,
                                                        sort_key=new_s3_key)

        dy_item = dy_res['Items'][0]
        dy_item['Consent'] = True

        update_array.append(dy_item)

    print(json.dumps(update_array, indent=4, cls=util.JsonSerialEncoder))

    # Batch update array
    print('Updating dydb...')
    dynamodb.batch_write_objects(DYNAMODB_STORE_TABLE_NAME, update_array)
    dynamodb.batch_write_objects_archive(DYNAMODB_ARCHIVE_STORE_TABLE_NAME, update_array, 'ObjectUpdate')

    return update_array


def tag_s3_object(bucket: str, key: str, tag_set: List[dict]):
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

    client_s3 = util.get_client('s3')

    response = client_s3.put_object_tagging(
        Bucket=bucket,
        Key=key,
        Tagging={
            'TagSet': tag_set
        }
    )
    return response


def tag_object_from_s3_key_list(s3_key_list):
    tag_set = [
        {
            'Key': 'Consent',
            'Value': 'True'
        }
    ]

    success_array = []
    error_array = []
    for s3_key in s3_key_list:

        print('tagging: ', s3_key)

        try:
            tag_resp = tag_s3_object(bucket=STORE_BUCKET, key=s3_key, tag_set=tag_set)
            success_array.append(s3_key)
        except:
            error_array.append(s3_key)

    return success_array, error_array


if __name__ == '__main__':
    # parse_from_excel_by_pandas()  # For custom script

    args = get_argument()
    run_modify_tagging(**args)
