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

mapping = {
  # # "KidGen": "KidGen",
  # # "CHW": "chILDRANZ",
  # # "BM": "BM",
  # # "ICCON": "ICCon",
  # # "MCD": "BM",
  # # "brain_malformations": ["BM", "2019-07-22"],
  # # "ID": "ID",
  # # "CARDIAC": "Cardiac",
  # # "mitochondrial_disease": ["Mito", "2019-04-18"],
  # # "leukodystrophies":[ "Leuko","2019-08-12"],
  # # "HIDDEN": "HIDDEN",
  # # "GI": "GI",
  # # "LD": "Leuko",
  # # "EE": "EE",
  # # "MITO": "Mito",
  # "acute_care_genomics": ["AC", "2019-05-16"],
  "ACG": "AC"
}

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

old_store_bucket_name = 'agha-gdr-store'
new_store_bucket_name = 'agha-gdr-store-2.0'
new_staging_bucket_name = 'agha-gdr-staging-2.0'



def check_etag_and_size_match():

    for old_flagship in mapping:
        report_message = ['Moving Old Pipeline to New Pipeline Report\n']

        val=mapping[old_flagship]
        move_submission = None

        if not isinstance(val, list):
            new_flagship = val

            if not old_flagship.endswith('/'):
                tmp_flagship = old_flagship + '/'
            else:
                tmp_flagship = old_flagship

            submission_s3_list = s3.aws_s3_ls(old_store_bucket_name, tmp_flagship)
            report_message.append(f'Number of submission checked for {new_flagship}: {len(submission_s3_list)}')
        else:
            new_flagship = val[0]
            submission_s3_list = [old_flagship]
            move_submission = val[1]

        report_message.append(f"Moving report for {new_flagship}")
        report_message.append(f"Moving flagship name from {old_flagship} to {new_flagship}")

        for submission_prefix in submission_s3_list:
            print(f'Checking submission {submission_prefix}')
            report_message.append(f'\nSubmission: {submission_prefix}')

            old_metadata_list = s3.get_s3_object_metadata(bucket_name=old_store_bucket_name,
                                                          directory_prefix=submission_prefix)

            number_of_index_or_compress_file = 0
            error_list = []

            for metadata in old_metadata_list:

                metadata['bucket_name'] = old_store_bucket_name
                old_store_s3_key = metadata['Key']
                old_store_etag = metadata['ETag']
                old_store_size = metadata['Size']

                if not move_submission == None:
                    old_store_s3_key = add_new_submission(old_store_s3_key, move_submission)

                # Skipping for Index and manifest file
                if agha.FileType.is_index_file(old_store_s3_key) or \
                        agha.FileType.is_compressable(agha.FileType.from_name(old_store_s3_key).value) or \
                        agha.FileType.from_name(old_store_s3_key) == agha.FileType.MANIFEST:
                    number_of_index_or_compress_file += 1
                    continue

                # Grab new store metadata
                new_store_s3_key = replace_old_flagship_name(old_store_s3_key, new_flagship)

                try:
                    # Searching file in staging bucket
                    new_bucket_name = new_staging_bucket_name
                    new_metadata = get_object_metadata(bucket_name=new_bucket_name, s3_key=new_store_s3_key)
                    new_metadata['bucket_name'] = new_bucket_name
                    new_metadata['Key'] = new_store_s3_key
                except ValueError:
                    # Searching file in store bucket
                    try:
                        new_bucket_name = new_store_bucket_name
                        new_metadata = get_object_metadata(bucket_name=new_bucket_name, s3_key=new_store_s3_key)
                        new_metadata['bucket_name'] = new_bucket_name
                        new_metadata['Key'] = new_store_s3_key
                    except ValueError:
                        report_dict = construct_mismatch_dict(metadata, reason='No key found at new staging/store bucket')
                        error_list.append(report_dict)
                        continue

                new_store_etag = new_metadata['ETag']
                new_store_size = new_metadata['ContentLength']

                if old_store_etag != new_store_etag or old_store_size != new_store_size:
                    report_dict = construct_mismatch_dict(old_pipeline_dict=metadata, new_pipeline_dict=new_metadata, reason='Mismatch etag and/or size value')
                    error_list.append(report_dict)

            report_message.append(f'Total number of file from old store: {len(old_metadata_list)}')
            report_message.append(f'Total number of file skipped (Skipping index and/or manifest files) : {number_of_index_or_compress_file}')
            report_message.append(f'Total number of file checked: {len(old_metadata_list)-number_of_index_or_compress_file}')
            report_message.append(f'Total number of mismatch file: {len(error_list)}')

            if len(error_list)>0:
                report_message.append(f'Mismatch file:')
                report_message.append(json.dumps(error_list, indent=4))

        print(old_flagship)
        write_message_to_file(filename=f'{old_flagship}_new_pipeline_report_{util.get_datetimestamp()}.txt', message_list=report_message)

    print('Done')


def get_object_metadata(bucket_name: str, s3_key: str) -> dict:
    s3 = util.get_client('s3')

    try:
        object_metadata = s3.get_object(Bucket=bucket_name, Key=s3_key)
    except s3.exceptions.NoSuchKey:
        raise ValueError('No Such Key Found')

    return object_metadata


def replace_old_flagship_name(key_with_old_flagship: str, new_flagship: str) -> str:
    list_key = key_with_old_flagship.split('/')

    # Replace old flagship
    del list_key[0]
    list_key.insert(0, new_flagship)

    return '/'.join(list_key)

def add_new_submission(key_with_no_submission:str, submission:str):
    list_key = key_with_no_submission.split('/')

    list_key.insert(1, submission)

    return '/'.join(list_key)

def write_message_to_file(filename:str='output_result.txt', message_list=None):

    f = open(filename, 'w')

    for message in message_list:
        f.write(message)
        f.write('\n')

def construct_mismatch_dict(old_pipeline_dict, new_pipeline_dict=None, reason=''):

    if new_pipeline_dict== None:
        new_pipeline_dict = {
            "bucket_name":'-',
            "Key":'-',
            "ETag":'-',
            "ContentLength":'-',
        }

    report_dict = {}
    report_dict['reason'] = reason
    report_dict['old_bucket_name'] = old_pipeline_dict['bucket_name']
    report_dict['old_store_s3_key'] = old_pipeline_dict['Key']
    report_dict['old_store_etag'] = old_pipeline_dict['ETag']
    report_dict['old_store_size'] = old_pipeline_dict['Size']
    report_dict['new_bucket_name'] = new_pipeline_dict['bucket_name']
    report_dict['new_store_s3_key'] = new_pipeline_dict['Key']
    report_dict['new_store_etag'] = new_pipeline_dict['ETag']
    report_dict['new_store_size'] = new_pipeline_dict['ContentLength']

    return report_dict

if __name__ == '__main__':

    check_etag_and_size_match()
