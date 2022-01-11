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

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

old_store_bucket_name = 'agha-gdr-store'
new_store_bucket_name = 'agha-gdr-store-2.0'
new_staging_bucket_name = 'agha-gdr-staging-2.0'

report_message = []


def check_move_object():
    report_message.append('Moving Staging to Store bucket Report\n')

    flagship = 'Cardiac'

    report_message.append(f"Report for {flagship}")

    if not flagship.endswith('/'):
        flagship_directory = flagship + '/'
    else:
        flagship_directory = flagship

    submission_s3_list = s3.aws_s3_ls(new_staging_bucket_name, flagship_directory)
    report_message.append(f'Number of submission checked for {flagship}: {len(submission_s3_list)}')

    need_double_check_array = []

    for submission_prefix in submission_s3_list:
        print(f'Checking submission {submission_prefix}')
        report_message.append(f'\nSubmission: {submission_prefix}')

        staging_bucket_list = s3.get_s3_object_metadata(bucket_name=new_staging_bucket_name,
                                                        directory_prefix=submission_prefix)
        # Current data
        all_file = [obj['Key'] for obj in staging_bucket_list]
        manifest_file = []
        index_file = []
        compress_file = []

        # Processing data
        indexable_file = []
        compressible_file = []
        compressible_and_indexable_file = []

        for each_staging_object in staging_bucket_list:
            staging_key = each_staging_object['Key']
            filetype = agha.FileType.from_name(staging_key).get_name()

            if filetype == agha.FileType.MANIFEST.get_name():
                manifest_file.append(staging_key)

            if agha.FileType.is_index_file(staging_key):
                index_file.append(staging_key)

            if agha.FileType.is_compress_file(staging_key):
                compress_file.append(staging_key)

            if agha.FileType.is_compressable(filetype) and not agha.FileType.is_compress_file(staging_key):
                compressible_file.append(staging_key)

            if agha.FileType.is_indexable(filetype):
                indexable_file.append(staging_key)

            if agha.FileType.is_compressable(filetype) and not agha.FileType.is_compress_file(staging_key) and \
                    agha.FileType.is_indexable(filetype) and not agha.FileType.is_index_file(staging_key):
                compressible_and_indexable_file.append(staging_key)

        report_message.append(f'Existing file at {submission_prefix}')
        report_message.append(f'Total number of files in staging bucket:{len(staging_bucket_list)}')
        report_message.append(f'Total number of manifest file: {len(manifest_file)}')
        report_message.append(f'Total number of index file: {len(index_file)}')
        report_message.append(f'Total number of compress file: {len(compress_file)}')
        report_message.append(f'More information about existing file:')
        report_message.append(f'Total number of compressible file: {len(compressible_file)}')
        report_message.append(f'Total number of indexable file: {len(indexable_file)}')
        report_message.append(
            f'Total number of compressible and indexable file: {len(compressible_and_indexable_file)}')

        should_not_exist_after_transfer = len(staging_bucket_list) - len(manifest_file) - len(index_file)

        if should_not_exist_after_transfer > 0:
            report_message.append(
                f'If s3 transfer have initiated, {should_not_exist_after_transfer} number of file should not exist.')

            attention_dict = {
                'submission': submission_prefix,
                'no_file_not_transferred_due_to_compressing': should_not_exist_after_transfer,
                'file_not_transferred_due_to_compressing': list(
                    set(all_file) - set(manifest_file) - set(index_file))}

            need_double_check_array.append(attention_dict)

    report_message.append(f'Some file is submitted not in compressed format. In this case, the pipeline will create\n'
                          f'a compression and will only copy the created compression to the store bucket.\n'
                          f'Please check the following if the compression work as intended.')
    report_message.append(json.dumps(need_double_check_array, indent=4))

    write_message_to_file(filename=f'{flagship}_staging_to_store_report.txt', message_list=report_message)

    print('Done')


def write_message_to_file(filename: str = 'output_result.txt', message_list=None):
    f = open(filename, 'w')

    for message in message_list:
        f.write(message)
        f.write('\n')


if __name__ == '__main__':
    check_move_object()
