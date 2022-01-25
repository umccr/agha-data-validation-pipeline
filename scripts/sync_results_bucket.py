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
from boto3.dynamodb.conditions import Attr

import botocore

STORE_BUCKET_NAME = 'agha-gdr-store-2.0'
STAGING_BUCKET_NAME = 'agha-gdr-staging-2.0'
RESULTS_BUCKET_NAME = 'agha-gdr-results-2.0'


def sync_result_bucket():
    # TODO: Which bucket to sync to (staging/store)
    main_bucket = STAGING_BUCKET_NAME
    # TODO: Change the submission prefix
    sort_key_prefix = "HIDDEN/2021-10-26/"

    metadata_list_main_bucket = s3.get_s3_object_metadata(bucket_name=main_bucket, directory_prefix=sort_key_prefix)
    filename_main_bucket = [metadata['Key'].split('/')[-1] for metadata in metadata_list_main_bucket]

    metadata_list_results_bucket = s3.get_s3_object_metadata(bucket_name=RESULTS_BUCKET_NAME,
                                                             directory_prefix=sort_key_prefix)
    s3key_results_bucket = [metadata['Key'] for metadata in metadata_list_results_bucket]

    list_for_deletion = []

    for s3_key in s3key_results_bucket:
        result_filename = s3_key.split('/')[-1]

        if result_filename.endswith('__log.txt'):
            basename = result_filename.strip('__log.txt')
        elif result_filename.endswith('__results.json'):
            basename = result_filename.strip('__results.json')
        elif result_filename.endswith('.bai'):
            basename = result_filename.strip('.bai')
        elif result_filename.endswith('.tbi'):
            basename = result_filename.strip('.tbi')
        elif result_filename.endswith('.gz'):
            basename = result_filename.strip('.gz')
        else:
            continue

        if basename not in filename_main_bucket:
            list_for_deletion.append(s3_key)
    print(list_for_deletion)

    return
    res = s3.delete_s3_object_from_key(RESULTS_BUCKET_NAME, list_for_deletion)

    print(res)


if __name__ == '__main__':
    sync_result_bucket()
