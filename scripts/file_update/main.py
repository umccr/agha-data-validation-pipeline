import json
import os
import time
from boto3.dynamodb.conditions import Attr

from util import dynamodb, s3

STORE_BUCKET = 'agha-gdr-store-2.0'
DYNAMODB_STORE_TABLE_NAME = 'agha-gdr-store-bucket'



def execute_change():
    filter_expr = Attr('agha_study_id').eq('XXXXXX')
    SUBMISSION_PREFIX = "Flagship/Submission/"

    list_of_files = dynamodb.get_batch_item_from_pk_and_sk(DYNAMODB_STORE_TABLE_NAME,
                                                           dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
                                                           SUBMISSION_PREFIX,
                                                           filter_expr=filter_expr)
    key_of_list = [metadata['sort_key'] for metadata in list_of_files]
    print(json.dumps(list_of_files, indent=4))
    print(len(list_of_files))

    for key in key_of_list:
        new_key = f"NewFlagship/Submission/{key.split('/')[-1]}"

        os.system(f"./execute.sh "
                  f"{STORE_BUCKET} "
                  f"{dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value} "
                  f"{key} "
                  f"{new_key} "
                  f"""'{json.dumps({'agha_study_id': 'abcde'})}' """  # Put empty object for no changes. Example: '{}'
                  f"true")
        time.sleep(1)  # Allow for time buffering


if __name__ == '__main__':
    execute_change()
