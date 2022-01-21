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
    "KidGen": "KidGen",
    "CHW": "chILDRANZ",
    "BM": "BM",
    "ICCON": "ICCon",
    "MCD": "BM",
    "brain_malformations": ["BM", "2019-07-22"],
    "ID": "ID",
    "CARDIAC": "Cardiac",
    "mitochondrial_disease": ["Mito", "2019-04-18"],
    "leukodystrophies": ["Leuko", "2019-08-12"],
    "HIDDEN": "HIDDEN",
    "GI": "GI",
    "LD": "Leuko",
    "EE": "EE",
    "MITO": "Mito",
    "acute_care_genomics": ["AC", "2019-05-16"],
    "ACG": "AC"
}

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

OLD_STORE_BUCKET_NAME = 'agha-gdr-store'
NEW_STORE_BUCKET_NAME = 'agha-gdr-store-2.0'
NEW_STAGING_BUCKET_NAME = 'agha-gdr-staging-2.0'

skipped_list = [
    'Mito/2019-04-18/',
    'HIDDEN/2019-09-27/'
]

def check_etag_and_size_match():
    report_message = ['Moving Old Pipeline to New Pipeline Report\n']
    attention_list = []

    for old_flagship in mapping:
        print(f'Processing: {old_flagship}')

        val = mapping[old_flagship]
        move_submission = None

        # Parsing on different submission type
        if not isinstance(val, list):

            # Normal Move from old flagship that have date as submission prefix
            new_flagship_name = val

            if not old_flagship.endswith('/'):
                tmp_flagship = old_flagship + '/'
            else:
                tmp_flagship = old_flagship

            submission_s3_list = s3.aws_s3_ls(OLD_STORE_BUCKET_NAME, tmp_flagship)
        else:
            # Submission is on the flagship folder
            new_flagship_name = val[0]
            move_submission = '/'.join(val)
            move_submission += '/'
            submission_s3_list = [old_flagship]  # The flagship content is already the files (putting it in a list for same formatting)

        report_message.append(
            f'Number of submission checked for \'{old_flagship}\' (old flagship): {len(submission_s3_list)}')

        report_message.append(f"Moving report for \'{new_flagship_name}\'")
        report_message.append(f"Moving flagship name from \'{old_flagship}\' to \'{new_flagship_name}\'")

        for submission_prefix in submission_s3_list:
            report_message.append(f'\nSubmission: {submission_prefix}')

            if move_submission != None:
                new_submission_prefix = move_submission
            else:
                new_submission_prefix = replace_old_flagship_name(submission_prefix, new_flagship_name)

            # Skipped file list
            if new_submission_prefix in skipped_list:
                continue

            # Grabbing old metadata for old submission
            old_metadata_list = s3.get_s3_object_metadata(bucket_name=OLD_STORE_BUCKET_NAME,
                                                          directory_prefix=submission_prefix)

            # List all seperated files
            list_of_old_submission_s3_keys = [f"{metadata['Size']}:{metadata['ETag']}:{s3.get_s3_filename_from_s3_key(metadata['Key'])}" for metadata in
                                              old_metadata_list] # Combination of Size:Etag:Key for combined checking
            list_of_old_store_compressible_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_compressable(agha.FileType.from_name(metadata).get_name())]
            list_of_old_store_index_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_index_file(metadata)]
            list_of_old_store_manifest_and_md5_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_manifest_file(metadata) or agha.FileType.is_md5_file(metadata)]

            # Create Statistic
            statistic_report_old_submission = get_filetype_statistic_from_key_array(list_of_old_submission_s3_keys)
            statistic_report_old_submission['No. of Index file'] = len(list_of_old_store_index_file)
            statistic_report_old_submission['No. of compressible file'] = len(list_of_old_store_compressible_file)
            statistic_report_old_submission['Total number of files:'] = len(list_of_old_submission_s3_keys)
            statistic_report_old_submission['bucket_name'] = OLD_STORE_BUCKET_NAME
            statistic_report_old_submission['submission_prefix'] = submission_prefix

            # Grabbing new metadata list for new submission
            print(new_submission_prefix)
            new_metadata_list = s3.get_s3_object_metadata(bucket_name=NEW_STORE_BUCKET_NAME,
                                                          directory_prefix=new_submission_prefix)
            list_of_new_submission_s3_keys = [f"{metadata['Size']}:{metadata['ETag']}:{s3.get_s3_filename_from_s3_key(metadata['Key'])}" for metadata in
                                              new_metadata_list] # Combination of Size:Etag:Key for combined checking
            list_of_new_store_compressible_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_compressable(agha.FileType.from_name(metadata).get_name())]
            list_of_new_store_index_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_index_file(metadata)]


            statistic_report_new_submission = get_filetype_statistic_from_key_array(list_of_new_submission_s3_keys)
            statistic_report_new_submission['No. of Index file'] = len(list_of_new_store_index_file)
            statistic_report_new_submission['No. of compressible file'] = len(list_of_new_store_compressible_file)
            statistic_report_new_submission['Total number of files:'] = len(list_of_new_submission_s3_keys)
            statistic_report_new_submission['bucket_name'] = NEW_STORE_BUCKET_NAME
            statistic_report_new_submission['submission_prefix'] = new_submission_prefix

            old_minus_new_list = list(set(list_of_old_submission_s3_keys) - set(list_of_new_submission_s3_keys))
            old_minus_new_and_etc_list = list(set(list_of_old_submission_s3_keys) - set(list_of_new_submission_s3_keys)  -
                                      set(list_of_old_store_manifest_and_md5_file) - set(list_of_old_store_index_file) -
                                              set(list_of_old_store_compressible_file))

            overall_report = {}
            overall_report[f'{OLD_STORE_BUCKET_NAME}'] = statistic_report_old_submission
            overall_report[f'{NEW_STORE_BUCKET_NAME}'] = statistic_report_new_submission
            overall_report['No. of files in old bucket but not in the new bucket (including compressible, index, md5, and manifest file)'] = len(old_minus_new_list)
            overall_report['No. of files in old bucket but not in the new bucket (excluding compressible, index, md5, and manifest file)'] = len(old_minus_new_and_etc_list)
            overall_report['Files in old bucket but not in the new bucket (including compressible, index, md5, and manifest file) (format Size:Etag:Filename)'] = old_minus_new_list
            overall_report['Files in old bucket but not in the new bucket (excluding compressible, index, md5, and manifest file) (format Size:Etag:Filename)'] = old_minus_new_and_etc_list

            if len(old_minus_new_and_etc_list) > 0:
                attention_dict = {
                    "old_submission_prefix":submission_prefix,
                    "new_submission_prefix":new_submission_prefix,
                    "file_expected_in_new_store (format Size:Etag:Filename)":old_minus_new_and_etc_list
                }

                attention_list.append(attention_dict)

            report_message.append(json.dumps(overall_report, indent=4, cls=util.JsonSerialEncoder))


    report_message.append('\n Please double check the following array: \n\n')
    report_message.append(json.dumps(attention_list, indent=4, cls=util.JsonSerialEncoder))
    write_message_to_file(filename=f'old_store_to_new_store_pipeline_report_{util.get_datetimestamp()}.txt',
                              message_list=report_message)

    print('Done')


def get_filetype_statistic_from_key_array(s3_key_list=None):
    statistic_result = {}

    for s3_key in s3_key_list:

        filetype = agha.FileType.from_name(s3_key).get_name()

        key_name = f'No. of {filetype}'

        if filetype == 'UNSUPPORTED':  # Make some annotation what is the unsupported file
            key_name += f" ({s3_key.split('.')[-1]})"

        if statistic_result.get(key_name) == None:
            statistic_result[key_name] = 1
        else:
            statistic_result[key_name] += 1
    return statistic_result


def get_object_metadata(bucket_name: str, s3_key: str) -> dict:
    s3 = util.get_client('s3')

    try:
        object_metadata = s3.get_object(Bucket=bucket_name, Key=s3_key)
    except s3.exceptions.NoSuchKey:
        raise ValueError('No Such Key Found')

    return object_metadata


def replace_old_flagship_name(key_with_old_flagship: str, new_flagship_name: str) -> str:
    list_key = key_with_old_flagship.split('/')

    # Replace old flagship
    del list_key[0]
    list_key.insert(0, new_flagship_name)

    return '/'.join(list_key)


def write_message_to_file(filename: str = 'output_result.txt', message_list=None):
    f = open(filename, 'w')

    for message in message_list:
        f.write(message)
        f.write('\n')


def construct_mismatch_dict(old_pipeline_dict, new_pipeline_dict=None, reason=''):
    if new_pipeline_dict == None:
        new_pipeline_dict = {
            "bucket_name": '-',
            "Key": '-',
            "ETag": '-',
            "ContentLength": '-',
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
