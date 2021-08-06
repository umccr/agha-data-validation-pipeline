#!/usr/bin/env python3
import argparse
import datetime
import decimal
import enum
import json
import logging
import os
import pathlib
import re
import sys


import boto3
import shared


# Logging and results store with defaults
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
RESULTS_DATA = {
    'provided_checksum': 'not retrieved',
    'calculated_checksum': 'not run',
    'validated_checksum': 'not run',
    'calculated_filetype': 'not run',
    'validated_filetype': 'not run',
    'provided_index': 'not retrieved',
    'index_result': 'not run',
    'index_filename': 'na',
    'index_s3_bucket': 'na',
    'index_s3_key': 'na',
    'validation_result': 'not determined'
}

# Get environment variables
STAGING_BUCKET = shared.get_environment_variable('STAGING_BUCKET')
STAGING_PREFIX = shared.get_environment_variable('STAGING_PREFIX')
DYNAMODB_TABLE = shared.get_environment_variable('DYNAMODB_TABLE')
# NOTE(SW): these could be lifted up to the CDK stack or even to SSM
RESULTS_S3_KEY_PREFIX = 'result_files/'
RESULTS_S3_INDEX_PREFIX = 'indices/'

# Get AWS clients
RESOURCE_DYNAMODB = shared.get_dynamodb_table_resource(DYNAMODB_TABLE, region_name='ap-southeast-2')
CLIENT_S3 = shared.get_client('s3')

# Misc
UPLOAD_ID_RE = re.compile(fr'^{STAGING_PREFIX}/?(.+)/[^/]+?$')


class Tasks(enum.Enum):

    CHECKSUM = 'checksum'
    FILE_VALIDATE = 'validate_filetype'
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


INDEXABLE_FILES =  {
    FileTypes.BAM,
    FileTypes.VCF,
}


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--partition_key', required=True, type=str,
            help='DynamoDB partition key used to identify file')
    parser.add_argument('--sort_key', required=True, type=str,
            help='DynamoDB sort key used to identify file')
    parser.add_argument('--tasks', required=True, choices=[m.value for m in Tasks], nargs='+',
            help='Tasks to perform')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Get file info and load into results store
    file_info = get_record(args.partition_key, args.sort_key)
    RESULTS_DATA['provided_checksum'] = file_info['provided_checksum']
    RESULTS_DATA['provided_index'] = file_info['has_index']
    RESULTS_DATA['index_s3_bucket'] = file_info['index_s3_bucket']
    RESULTS_DATA['index_s3_key'] = file_info['index_s3_key']
    # Print file info to log
    msg_list = [f'{k}: {file_info[k]}' for k in sorted(file_info)]
    msg = '\r\t'.join(msg_list)
    LOGGER.info(f'got record:\r{msg}')

    # Stage file from S3 and then validate
    fp_local = stage_file(file_info['s3_bucket'], file_info['s3_key'], file_info['filename'])
    tasks = {Tasks(task_str) for task_str in args.tasks}
    if Tasks.CHECKSUM in tasks:
        run_checksum(fp_local, file_info)
    if Tasks.FILE_VALIDATE in tasks:
        filetype = run_filetype_validation(fp_local, file_info)
    # Simplify index requirement check
    create_index = Tasks.INDEX in tasks and filetype in INDEXABLE_FILES
    if create_index and not RESULTS_DATA['provided_index']:
        run_indexing(fp_local, file_info, filetype)

    # Set whether the file was validated (unpack for clarity)
    checksum_fail = RESULTS_DATA['validated_checksum'] != 'valid'
    filetype_fail = RESULTS_DATA['validated_filetype'] != 'valid'
    # NOTE(SW): we currently should *never* recieve an index, ignore this case here
    index_fail = RESULTS_DATA['index_result'] not in {'not run', 'succeeded'}
    vresult = checksum_fail or filetype_fail or index_fail
    RESULTS_DATA['validation_result'] = 'not valid' if vresult else 'valid'

    # Write completed result to log and S3
    write_results_s3(file_info)


def get_record(partition_key, sort_key):
    response = RESOURCE_DYNAMODB.get_item(
        Key={'partition_key': partition_key, 'sort_key': sort_key}
    )
    if 'Item' not in response:
        msg_key_text = f'partition key {partition_key} and sort key {sort_key}'
        LOGGER.critical(f'could not retrieve DynamoDB entry with {msg_key_text}')
        sys.exit(1)
    record_raw = response.get('Item')
    return replace_record_decimal_object(record_raw)


def replace_record_decimal_object(record):
    for k in record:
        if isinstance(record[k], decimal.Decimal):
            record[k] = int(record[k]) if record[k] % 1 == 0 else float(record[k])
    return record


def stage_file(s3_bucket, s3_key, filename):
    LOGGER.info(f'staging file from S3: s3://{s3_bucket}/{s3_key}')
    output_fp = pathlib.Path(filename)
    with output_fp.open('wb') as fh:
        CLIENT_S3.download_fileobj(s3_bucket, s3_key, fh)
    return output_fp


def run_checksum(fp, file_info):
    LOGGER.info('running checksum')
    # Execute
    command = f"md5sum {fp} | cut -f1 -d' '"
    result = shared.execute_command(command)
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
        RESULTS_DATA['validated_checksum'] = 'not valid'
    # Log results
    provided_str = f'provided:   {RESULTS_DATA["provided_checksum"]}'
    calculated_str  = f'calculated: {RESULTS_DATA["calculated_checksum"]}'
    validated_str = f'validated:  {RESULTS_DATA["validated_checksum"]}'
    checksum_str = f'{provided_str}\r\t{calculated_str}\r\t{validated_str}'
    LOGGER.info(f'checksum results: {checksum_str}')


def run_filetype_validation(fp, file_info):
    LOGGER.info('running file type validation')
    # Get file type
    fext_fastq = {'.fq', '.fq.gz', '.fastq', '.fastq.gz'}
    fext_bam = {'.bam'}
    fext_vcf = {'.vcf.gz', 'gvcf', 'gvcf.gz'}
    if any(fp.name.endswith(fext) for fext in fext_bam):
        filetype = FileTypes.BAM
        command = f'samtools quickcheck -q {fp}'
    elif any(fp.name.endswith(fext) for fext in fext_fastq):
        filetype = FileTypes.FASTQ
        command = f'fqtools validate {fp}'
    elif any(fp.name.endswith(fext) for fext in fext_vcf):
        filetype = FileTypes.VCF
        command = f'bcftools query -l {fp}'
    else:
        LOGGER.critical(f'could not infer file type from extension for {fp}')
        RESULTS_DATA['calculated_filetype'] = 'failed'
        RESULTS_DATA['validated_filetype'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    # Validate filetype
    RESULTS_DATA['calculated_filetype'] = filetype.value
    result = shared.execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.info('file validation failed (invalid filetype or other failure): {stdstrm_msg}')
        RESULTS_DATA['validated_filetype'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    else:
        RESULTS_DATA['validated_filetype'] = 'valid'
    # Log results
    calculated_str  = f'calculated: {RESULTS_DATA["calculated_filetype"]}'
    validated_str = f'validated:  {RESULTS_DATA["validated_filetype"]}'
    filetype_str = f'{calculated_str}\r\t{validated_str}'
    LOGGER.info(f'file type validation results: {filetype_str}')
    return filetype


def run_indexing(fp, file_info, filetype):
    # Run appropriate indexing command
    LOGGER.info('running indexing')
    if filetype == FileTypes.BAM:
        command = f'samtools index {fp}'
        index_fp = f'{fp}.bai'
    elif filetype == FileTypes.VCF:
        command = f"tabix {fp} -p 'vcf'"
        index_fp = f'{fp}.tbi'
    else:
        # You should never have come here
        assert False
    result = shared.execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.critical(f'failed to run indexing ({command}): {stdstrm_msg}')
        RESULTS_DATA['index_result'] = 'failed'
        write_results_s3(file_info)
        sys.exit(1)
    # Upload index and set results
    index_s3_key = upload_index(file_info, index_fp)
    RESULTS_DATA['index_result'] = 'succeeded'
    RESULTS_DATA['index_filename'] = index_fp
    RESULTS_DATA['index_s3_bucket'] = file_info['s3_bucket']
    RESULTS_DATA['index_s3_key'] = index_s3_key
    # Log results
    result_str = f'result:    {RESULTS_DATA["index_result"]}'
    filename_str = f'filename:  {RESULTS_DATA["index_filename"]}'
    bucket_str = f'S3 bucket: {RESULTS_DATA["index_s3_bucket"]}'
    key_str = f'S3 key:    {RESULTS_DATA["index_s3_key"]}'
    filetype_str = f'{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info(f'file type validation results: {filetype_str}')


def get_file_common_dir(s3_key):
    # Get the <flagship>/<date> component from the S3 key of format:
    #   <staging_prefix>/<flagship>/<date>/manifest.txt
    if not (upload_id_result := UPLOAD_ID_RE.match(s3_key)):
        LOGGER.critical(f'could not obtain upload S3 key from \'{s3_key}\' using \'{UPLOAD_ID_RE}\'')
        sys.exit(1)
    return upload_id_result.group(1)


def upload_index(file_info, index_fp):
    # Get a unique directory to store
    partition_key_esc = file_info['partition_key'].replace('/', '_')
    sort_key_esc = file_info['sort_key'].replace('/', '_')
    s3_unique_dir = f'{partition_key_esc}__{sort_key_esc}'
    # Construct full key
    s3_key_basedir = get_file_common_dir(file_info['s3_key'])
    s3_key = os.path.join(
        RESULTS_S3_INDEX_PREFIX,
        s3_key_basedir,
        s3_unique_dir,
        index_fp
    )
    LOGGER.info(f'writing index to s3://{STAGING_BUCKET}/{s3_key}')
    CLIENT_S3.upload_file(index_fp, STAGING_BUCKET, s3_key)
    return s3_key


def write_results_s3(file_info):
    # Create results json
    data = {
        'file_info': file_info,
        'results': RESULTS_DATA
    }
    s3_object_body = f'{json.dumps(data, indent=4)}\n'
    # Upload to S3
    s3_key_filename = get_unique_s3_fn(
        file_info['filename'],
        file_info['partition_key'],
        file_info['sort_key']
    )
    s3_key_basedir = get_file_common_dir(file_info['s3_key'])
    s3_key = os.path.join(RESULTS_S3_KEY_PREFIX, s3_key_basedir, s3_key_filename)
    s3_object_body_log = s3_object_body.replace('\n', '\r')
    LOGGER.info(f'writing results to s3://{STAGING_BUCKET}/{s3_key}:\r{s3_object_body_log}')
    CLIENT_S3.put_object(Body=s3_object_body, Bucket=STAGING_BUCKET, Key=s3_key)


def get_unique_s3_fn(filename, partition_key, sort_key):
    timestamp = '{:%Y%m%d_%H%M%S}'.format(datetime.datetime.now())
    partition_key_esc = partition_key.replace('/', '_')
    sort_key_esc = sort_key.replace('/', '_')
    return f'{filename}__{partition_key_esc}__{sort_key_esc}__{timestamp}.json'


if __name__ == '__main__':
    main()
