import sys
import logging
import util.s3 as s3
import util.dynamodb as dy
import util.dynamodb as dynamodb
import util.agha as agha
import util as util
import util.batch as batch
import util.submission_data as submission_data
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
    move_output = dict()

    for old_flagship in mapping:
        print(f'Processing: {old_flagship}')

        val = mapping[old_flagship]
        move_submission = None

        flagship_report = dict()

        # Need to append '/' at the end for listing directories
        if not old_flagship.endswith('/'):
            tmp_flagship = old_flagship + '/'
        else:
            tmp_flagship = old_flagship

        # Parsing on different submission type
        if not isinstance(val, list):

            # Normal Move from old flagship that have date as submission prefix
            new_flagship_name = val

            submission_s3_list = s3.aws_s3_ls(OLD_STORE_BUCKET_NAME, tmp_flagship)
        else:
            # Submission is on the flagship folder
            new_flagship_name = val[0]
            move_submission = '/'.join(val)
            move_submission += '/'

            # The flagship content is already the files (putting it in a list for same formatting)
            submission_s3_list = [tmp_flagship]

        report_message.append(
            f'Number of submission checked for \'{old_flagship}\' (old flagship): {len(submission_s3_list)}')

        report_message.append(f"Moving report for \'{new_flagship_name}\'")
        report_message.append(f"Moving flagship name from \'{old_flagship}\' to \'{new_flagship_name}\'")

        # Check submission prefix from old bucket
        for submission_prefix in submission_s3_list:

            report_message.append(f'\nSubmission: {submission_prefix}')

            # Check if submission now has a submission prefix
            if move_submission is not None:
                new_submission_prefix = move_submission
            else:
                new_submission_prefix = replace_old_flagship_name(submission_prefix, new_flagship_name)

            # Skipped file list
            if new_submission_prefix in skipped_list:
                continue

            print(submission_prefix)

            # Using manifest data for checksums
            manifest_data = submission_data.retrieve_manifest_data(bucket_name=OLD_STORE_BUCKET_NAME, manifest_key=f'{submission_prefix}manifest.txt')

            # Grabbing old metadata for old submission
            old_metadata_list = s3.get_s3_object_metadata(bucket_name=OLD_STORE_BUCKET_NAME,
                                                          directory_prefix=submission_prefix)
            # Categorizing files
            list_of_old_submission_s3_keys = []
            use_etag_array = []

            for metadata in old_metadata_list:
                s3_key = metadata['Key']
                size = metadata['Size']
                etag = metadata['ETag']
                filename = s3.get_s3_filename_from_s3_key(s3_key)

                try:
                    checksum = submission_data.find_checksum_from_manifest_df_and_filename(manifest_data, filename)

                    if checksum == 'Unknown':
                        raise IndexError
                except IndexError:
                    # Substitute with ETag if it is not find
                    checksum = etag

                    # Notify for the store use etag instead of their generated checksum
                    # Keep track by using etag array
                    use_etag_array.append(filename)

                checksum = checksum.strip('"')
                list_of_old_submission_s3_keys.append(f'{size}:{checksum}:{filename}')
            print(json.dumps(list_of_old_submission_s3_keys, indent=4))
            list_of_old_store_compressible_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_compressable(agha.FileType.from_name(metadata).get_name())]
            list_of_old_store_compressed_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_compress_file(metadata)]
            list_of_old_store_uncompress_file = list(set(list_of_old_store_compressible_file) - set(list_of_old_store_compressed_file))
            list_of_old_store_indexable_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_indexable(agha.FileType.from_name(metadata).get_name())]
            list_of_old_store_indexed_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_index_file(metadata)]
            list_of_old_store_md5_file = [metadata for metadata in list_of_old_submission_s3_keys if agha.FileType.is_md5_file(metadata)]

            # Create Statistic
            statistic_report_old_submission = get_filetype_statistic_from_key_array(list_of_old_submission_s3_keys)
            statistic_report_old_submission['number_of_indexed_file'] = len(list_of_old_store_indexed_file)
            statistic_report_old_submission['number_of_indexable_file'] = len(list_of_old_store_indexable_file)
            statistic_report_old_submission['number_of_uncompress_file'] = len(list_of_old_store_uncompress_file)
            statistic_report_old_submission['number_of_compressed_file'] = len(list_of_old_store_compressed_file)
            statistic_report_old_submission['number_of_files'] = len(list_of_old_submission_s3_keys)

            # Grabbing new metadata list for new submission
            new_metadata_list = s3.get_s3_object_metadata(bucket_name=NEW_STORE_BUCKET_NAME, directory_prefix=new_submission_prefix)

            # Separating files
            list_of_new_submission_s3_keys = []

            # Categorizing files
            for metadata in new_metadata_list:
                s3_key = metadata['Key']
                size = metadata['Size']
                etag = metadata['ETag']
                filename = s3.get_s3_filename_from_s3_key(s3_key)

                res = dynamodb.get_item_from_exact_pk_and_sk(table_name='agha-gdr-store-bucket', partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value, sort_key=s3_key)
                if filename not in use_etag_array and res['Count'] > 0:
                    checksum = json.dumps(res['Items'][0]['provided_checksum'])
                else:
                    checksum = etag

                checksum = checksum.strip('"')
                list_of_new_submission_s3_keys.append(f'{size}:{checksum}:{filename}')
            list_of_new_store_compressible_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_compressable(agha.FileType.from_name(metadata).get_name())]
            list_of_new_store_compressed_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_compress_file(metadata)]
            list_of_new_store_uncompress_file = list(set(list_of_new_store_compressible_file) - set(list_of_new_store_compressed_file))
            list_of_new_store_indexable_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_indexable(agha.FileType.from_name(metadata).get_name())]
            list_of_new_store_indexed_file = [metadata for metadata in list_of_new_submission_s3_keys if agha.FileType.is_index_file(metadata)]

            statistic_report_new_submission = get_filetype_statistic_from_key_array(list_of_new_submission_s3_keys)
            statistic_report_new_submission['number_of_indexed_file'] = len(list_of_new_store_indexed_file)
            statistic_report_new_submission['number_of_indexable_file'] = len(list_of_new_store_indexable_file)
            statistic_report_new_submission['number_of_uncompress_file'] = len(list_of_new_store_uncompress_file)
            statistic_report_new_submission['number_of_compressed_file'] = len(list_of_new_store_compressed_file)
            statistic_report_new_submission['number_of_files'] = len(list_of_new_submission_s3_keys)

            old_minus_new_and_etc_list = list(
                set(list_of_old_submission_s3_keys) - set(list_of_new_submission_s3_keys) -
                set(list_of_old_store_md5_file) - set(list_of_old_store_indexed_file) -
                set(list_of_old_store_uncompress_file))

            new_minus_old_list = list(
                set(list_of_new_submission_s3_keys) - set(list_of_new_store_indexed_file) -
                set(list_of_new_store_compressible_file) - set(list_of_new_submission_s3_keys))

            # Some logic check
            check = dict()

            # 1. Number of non-index and index file must be the same in the new
            check['is_number_index_and_non_index_file_match_in_new_store'] = statistic_report_new_submission['number_of_indexable_file'] == statistic_report_new_submission['number_of_indexed_file']

            # 2. Number of compressed + uncompressed in old store is the same as number of compressed file in the new store
            check['is_number_of_compressible_file_in_old_store_is_the_same_as_number_of_compressed_file_in_new_store'] = len(list_of_old_store_compressible_file) == statistic_report_new_submission['number_of_compressed_file']

            # 3. Number of indexable file in staging and store are the same
            check['is_number_of_indexable_file_match_between_buckets'] = statistic_report_old_submission['number_of_indexable_file'] == statistic_report_new_submission['number_of_indexable_file']

            overall_report = dict()
            overall_report['old_bucket_name'] = OLD_STORE_BUCKET_NAME
            overall_report['new_bucket_name'] = NEW_STORE_BUCKET_NAME
            overall_report['old_submission_prefix'] = submission_prefix
            overall_report['new_submission_prefix'] = new_submission_prefix
            overall_report['validation'] = check
            overall_report['statistic'] = dict()
            overall_report['statistic']['old_bucket'] = statistic_report_old_submission
            overall_report['statistic']['new_bucket'] = statistic_report_new_submission
            overall_report['data'] = dict()
            overall_report['data']['old_submission'] = list_of_old_submission_s3_keys
            overall_report['data']['new_submission'] = list_of_new_submission_s3_keys
            overall_report['number_of_old_bucket_minus_new_bucket_excluding_index_uncompress_md5'] = len(old_minus_new_and_etc_list)
            overall_report['number_of_extra_files_in_new_bucket'] = len(new_minus_old_list)
            overall_report['files_in_old_bucket_minus_new_bucket_excluding_index_uncompress_md5'] = old_minus_new_and_etc_list
            overall_report['extra_files_in_new_bucket'] = new_minus_old_list

            validation_value_results_list = list(overall_report['validation'].values())

            if len(old_minus_new_and_etc_list) > 0 or [result for result in validation_value_results_list if result is True]:
                attention_dict = {
                    "old_submission_prefix": submission_prefix,
                    "new_submission_prefix": new_submission_prefix,
                    "file_expected_in_new_store": old_minus_new_and_etc_list
                }

                attention_list.append(attention_dict)

            report_message.append(json.dumps(overall_report, indent=4, cls=util.JsonSerialEncoder))

            flagship_report[submission_prefix] = overall_report

        move_output[old_flagship] = flagship_report

    # Write result in JSON
    write_message_to_file(filename=f'move_store_to_store_report.json', message_list=[json.dumps(move_output, indent=4)])

    # Append some filename needs attention
    report_message.append('\n Please double check the following array: \n\n')
    report_message.append(json.dumps(attention_list, indent=4, cls=util.JsonSerialEncoder))

    # Write result to json
    write_message_to_file(filename=f'old_store_to_new_store_pipeline_report_{util.get_datetimestamp()}.txt',
                          message_list=report_message)

    print('Done')


def get_filetype_statistic_from_key_array(s3_key_list=None):
    statistic_result = {}

    for s3_key in s3_key_list:

        filetype = agha.FileType.from_name(s3_key).get_name()

        key_name = f'number_of'

        if filetype == 'UNSUPPORTED':  # Make some annotation what is the unsupported file
            key_name += f"_{s3_key.split('.')[-1]}"

        key_name += f"_{filetype}"

        if statistic_result.get(key_name) is None:
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
