#!/usr/bin/env python3
import argparse
import datetime
import enum
import json
import os
import pathlib
import sys


import boto3
import shared


# Logging and results store
LOGGER = shared.LOGGER
RESULTS_DATA = {
    'provided_checksum': 'not retrieved',
    'calculated_checksum': 'not run',
    'validated_checksum': 'not run',
    'provided_filetype': 'not retrieved',
    'calculated_filetype': 'not run',
    'validated_filetype': 'not run',
    'provided_index': 'not retrieved',
    'index_result': 'not run',
    'index_filename': 'na',
    'index_s3_bucket': 'na',
    'index_s3_key': 'na'
}
RESULTS_S3_BUCKET = 'umccr-agha-test-dev'
RESULTS_S3_KEY_PREFIX = 'result_files/'


class Tasks(enum.Enum):

    CHECKSUM = 'checksum'
    FILE_VALIDATE = 'validate_file_type'
    INDEX = 'create_index'


class FileTypes(enum.Enum):

    BAM = 'BAM'
    FASTQ = 'FASTQ'
    VCF = 'VCF'

    @classmethod
    def contains(cls, item):
        try:
            cls(item)
        except ValueError:
            return False
        return True


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--partition_key', required=True, type=str,
            help='DynamoDB partition key used to identify file')
    parser.add_argument('--sort_key', required=True, type=str,
            help='DynamoDB sort key used to identify file')
    parser.add_argument('--tasks', required=True, choices=[m.value for m in Tasks], nargs='+',
            help='Tasks to perform')
    parser.add_argument('--dynamodb_table', required=True, type=str,
            help='Name of DynamoDB table')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Get AWS clients
    CLIENT_DYNAMODB = shared.get_client('dynamodb')
    CLIENT_S3 = shared.get_client('s3')

    # Get file info and load into results store
    file_info = shared.get_record(args.partition_key, args.sort_key)
    RESULTS_DATA['provided_checksum'] = file_info['provided_checksum']
    RESULTS_DATA['provided_filetype'] = file_info['provided_filetype']
    RESULTS_DATA['provided_index'] = file_info['has_index']
    RESULTS_DATA['index_s3_bucket'] = file_info['index_s3_bucket']
    RESULTS_DATA['index_s3_key'] = file_info['index_s3_key']
    # Print file info to log
    msg_list = [f'{k}: {record[k]}' for k in sorted(file_info)]
    msg = '\r\t'.join(msg_list)
    LOGGER.info(f'got record:\r{msg}'

    # Stage file from S3 and then validate
    fp_local = stage_file(file_info['s3_bucket'], file_info['s3_key'], file_info['filename'])
    tasks = {Tasks(task_str) for task_str in args.tasks}
    if Tasks.CHECKSUM in tasks:
        run_checksum(fp_local, file_info)
    if Tasks.FILE_VALIDATE in tasks:
        file_type = run_file_type_validation(fp_local, file_info)
    if Tasks.INDEX in tasks and file_type == FileTypes.VCF and file_info['provided_index'] == 'none':
        index_fp, index_s3_bucket, index_s3_key = run_indexing(fp_local, file_info)
        S3_CLIENT.upload_file(index_fp, index_s3_bucket, index_s3_key)

    # Write completed result to log and S3
    write_results_s3(file_info)


def stage_file(s3_bucket, s3_key, filename):
    LOGGER.info(f'staging file from S3: s3://{s3_bucket}/{s3_key}')
    output_fp = pathlib.Path(filename)
    with output_fp.open('wb') as fh:
        S3_CLIENT.download_fileobj(s3_bucket, s3_key, fh)


def run_checksum(fp, file_info):
    LOGGER.info('running checksum')
    # Execute
    command = f"md5sum {fp} | cut -f1 -d' '"
    result = execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.critical(f'failed to run checksum ({command}): {stdstrm_msg}')
        RESULTS_DATA['calculated_checksum'] = 'failed'
        RESULTS_DATA['validated_checksum'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    # Determine results and store
    RESULTS_DATA['calculated_checksum'] = result.stdout.rstrip()
    if RESULTS_DATA['provided_checksum'] == RESULTS_DATA['calculated_checksum']:
        RESULTS_DATA['validated_checksum'] = 'valid'
    else:
        RESULTS_DATA['validated_checksum'] = 'invalid'
    # Log results
    provided_str = f'provided:   {RESULTS_DATA["provided_checksum"]}'
    calculated_str  = f'calculated: {RESULTS_DATA["calculated_checksum"]}'
    validated_str = f'validated:  {RESULTS_DATA["validated_checksum"]}'
    checksum_str = '{provided_str}\r\t{calculated_str}\r\t{validated_str}'
    LOGGER.info('checksum results: {checksum_str}')


def run_file_type_validation(fp, file_info):
    LOGGER.info('running file type validation')
    # Get file type
    fext_fastq = {'.fq', '.fq.gz', '.fastq', '.fastq.gz'}
    fext_bam = {'.bam'}
    fext_vcf = {'.vcf.gz', 'gvcf', 'gvcf.gz'}
    if any(fp.name.endswith(fext) for fext in fext_bam):
        file_type = FileTypes.BAM
        command = f'samtools quickcheck -q {fp}'
    elif any(fp.name.endswith(fext) for fext in fext_fastq):
        file_type = FileTypes.FASTQ
        command = f'fqtools validate {fp}'
    elif any(fp.name.endswith(fext) for fext in fext_vcf):
        file_type = FileTypes.VCF
        command = f'bcftools query -l {fp}'
    else:
        LOGGER.critical(f'could not infer file type from extension for {fp}')
        RESULTS_DATA['calculated_filetype'] = 'failed'
        RESULTS_DATA['validated_filetype'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    # Validate filetype
    RESULTS_DATA['calculated_filetype'] = file_type.value
    result = execute_command(command)
    if result.returncode != 0:
        LOGGER.info('file validation failed (invalid filetype or other failure)')
        RESULTS_DATA['validated_filetype'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    elif file_info['provided_filetype'] != file_type.value:
        LOGGER.info('file type does not match file type infered from extension')
        RESULTS_DATA['validated_filetype'] = 'filetype mismatch'
        write_results_s3(file_info)
        sys.exit(1)
    else:
        RESULTS_DATA['validated_filetype'] = 'valid'
    # Log results
    provided_str = f'provided:   {RESULTS_DATA["provided_filetype"]}'
    calculated_str  = f'calculated: {RESULTS_DATA["calculated_filetype"]}'
    validated_str = f'validated:  {RESULTS_DATA["validated_filetype"]}'
    filetype_str = '{provided_str}\r\t{calculated_str}\r\t{validated_str}'
    LOGGER.info('file type validation results: {filetype_str}')
    return file_type


def run_indexing(fp, file_info):
    LOGGER.info('running indexing')
    command = f"tabix {fp} -p 'vcf'"
    result = execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.critical(f'failed to run indexing ({command}): {stdstrm_msg}')
        RESULTS_DATA['index_result'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    RESULTS_DATA['index_result'] = 'succeeded'
    RESULTS_DATA['index_filename'] = pathlib.Path(f'{fp}.tbi')
    RESULTS_DATA['index_s3_bucket'] = file_info['s3_bucket']
    RESULTS_DATA['index_s3_key'] = f"{file_info['s3_key']}.tbi"
    # Log results
    result_str = f'result:    {RESULTS_DATA["index_result"]}'
    filename_str = f'filename:  {RESULTS_DATA["index_filename"]}'
    bucket_str = f'S3 bucket: {RESULTS_DATA["index_s3_bucket"]}'
    key_str = f'S3 key:    {RESULTS_DATA["index_s3_key"]}'
    filetype_str = '{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info('file type validation results: {filetype_str}')


def write_results_s3(file_info):
    # Create results json
    data = {
        'file_info': file_info,
        'results': RESULTS_DATA
    }
    s3_object_body = f'{json.dumps(data)}\n'
    # Upload to S3
    s3_key_filename = get_unique_s3_fn(file_info['filename'], partition_key, sort_key)
    s3_key_basedir = os.path.dirname(file_info['s3_key'])
    s3_key = os.path.join(RESULTS_S3_KEY_PREFIX, s3_key_basedir, s3_key_filename)
    LOGGER.info('writing results to s3://{RESULTS_S3_BUCKET}/{s3_key}:\r{s3_object_body}')
    S3_CLIENT.put_object(Body=s3_object_body, Bucket=RESULTS_S3_BUCKET, Key=s3_key)


def get_unique_s3_fn(filename, partition_key, sort_key):
    timestamp = '{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())
    partition_key_esc = partition_key.replace('/', '_')
    sort_key_esc = sort_key.replace('/', '_')
    return f'{filename}__{partition_key_esc}__{sort_key_esc}__{timestamp}.tsv'


if __name__ == '__main__':
    main()
