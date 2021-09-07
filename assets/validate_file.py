#!/usr/bin/env python3
import argparse
import decimal
import enum
import json
import logging
import os
import pathlib
import sys


import boto3
import util


# Logging
LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)
# Add log file handler so we can upload log messages to S3
LOG_FILE_NAME = 'log.txt'
LOG_FILE_HANDLER = util.FileHandlerNewLine(LOG_FILE_NAME)
LOGGER.addHandler(LOG_FILE_HANDLER)

# Results store with defaults
RESULTS_DATA = {
    'ts_validation_job': 'na',
    'provided_checksum': 'not retrieved',
    'calculated_checksum': 'not run',
    'valid_checksum': 'not run',
    'inferred_filetype': 'not run',
    'valid_filetype': 'not run',
    'index_result': 'not run',
    'index_filename': 'na',
    'index_s3_bucket': 'na',
    'index_s3_key': 'na',
    'results_s3_bucket': 'na',
    'results_data_s3_key': 'na',
    'results_log_s3_key': 'na',
    'tasks_completed': 'no'
}

# Get environment variables
DYNAMODB_TABLE = util.get_environment_variable('DYNAMODB_TABLE')
RESULTS_BUCKET = util.get_environment_variable('RESULTS_BUCKET')
RESULTS_KEY_PREFIX = util.get_environment_variable('RESULTS_KEY_PREFIX')
# Emitted to log for record purposes
BATCH_JOBID = util.get_environment_variable('AWS_BATCH_JOB_ID')

# Get AWS clients
RESOURCE_DYNAMODB = util.get_dynamodb_table_resource(DYNAMODB_TABLE, region_name='ap-southeast-2')
CLIENT_S3 = util.get_client('s3')


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
    parser.add_argument('--sort_key', required=True, type=int,
            help='DynamoDB sort key used to identify file')
    parser.add_argument('--tasks', required=True, choices=[m.value for m in Tasks], nargs='+',
            help='Tasks to perform')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Log Batch job id and set job datetime stamp
    LOGGER.info(f'starting job: {BATCH_JOBID}')
    RESULTS_DATA['ts_validation_job'] = util.get_datetimestamp()

    # Get file info and load into results store
    file_info = get_record(args.partition_key, args.sort_key)
    RESULTS_DATA['provided_checksum'] = file_info['provided_checksum']
    RESULTS_DATA['index_s3_bucket'] = file_info['index_s3_bucket']
    RESULTS_DATA['index_s3_key'] = file_info['index_s3_key']
    # Set upload target for job output files
    RESULTS_DATA['results_s3_bucket'] = RESULTS_BUCKET
    RESULTS_DATA['results_data_s3_key'] = get_results_data_s3_key(file_info)
    RESULTS_DATA['results_log_s3_key'] = get_log_s3_key(file_info)
    # Print file info to log
    msg_list = [f'{k}: {file_info[k]}' for k in sorted(file_info)]
    msg = '\r\t'.join(msg_list)
    LOGGER.info(f'got record:\r\t{msg}')

    # Stage file from S3 and then validate
    fp_local = stage_file(
        file_info['s3_bucket'],
        file_info['s3_key'],
        file_info['filename']
    )
    tasks = {Tasks(task_str) for task_str in args.tasks}
    if Tasks.CHECKSUM in tasks:
        run_checksum(fp_local, file_info)
    if Tasks.FILE_VALIDATE in tasks:
        filetype = run_filetype_validation(fp_local, file_info)
    # Simplify index requirement check
    if Tasks.INDEX in tasks and filetype in INDEXABLE_FILES:
        run_indexing(fp_local, file_info, filetype)

    # Set whether the file was validated (unpack for clarity)
    checksum_fail = RESULTS_DATA['valid_checksum'] not in {'not run', 'yes', 'na'}
    filetype_fail = RESULTS_DATA['valid_filetype'] not in {'not run', 'yes'}
    index_fail = RESULTS_DATA['index_result'] not in {'not run', 'succeeded'}
    vresult = checksum_fail or filetype_fail or index_fail
    RESULTS_DATA['tasks_completed'] = 'no' if vresult else 'yes'

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
    result = util.execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.critical(f'failed to run checksum ({command}): {stdstrm_msg}')
        RESULTS_DATA['calculated_checksum'] = 'failed'
        RESULTS_DATA['valid_checksum'] = 'no'
        write_results_s3(file_info)
        sys.exit(1)
    # Determine results and store
    RESULTS_DATA['calculated_checksum'] = result.stdout.rstrip()
    if RESULTS_DATA['provided_checksum'] == 'not provided':
        RESULTS_DATA['valid_checksum'] = 'na'
    if RESULTS_DATA['provided_checksum'] == RESULTS_DATA['calculated_checksum']:
        RESULTS_DATA['valid_checksum'] = 'yes'
    else:
        RESULTS_DATA['valid_checksum'] = 'no'
    # Log results
    provided_str = f'provided:   {RESULTS_DATA["provided_checksum"]}'
    calculated_str  = f'calculated: {RESULTS_DATA["calculated_checksum"]}'
    validated_str = f'validated:  {RESULTS_DATA["valid_checksum"]}'
    checksum_str = f'{provided_str}\r\t{calculated_str}\r\t{validated_str}'
    LOGGER.info(f'checksum results:\r\t{checksum_str}')


def run_filetype_validation(fp, file_info):
    LOGGER.info('running file type validation')
    # Get file type
    if any(fp.name.endswith(fext) for fext in util.FEXT_BAM):
        filetype = FileTypes.BAM
        command = f'samtools quickcheck -q {fp}'
    elif any(fp.name.endswith(fext) for fext in util.FEXT_FASTQ):
        filetype = FileTypes.FASTQ
        command = f'fqtools validate {fp}'
    elif any(fp.name.endswith(fext) for fext in util.FEXT_VCF):
        filetype = FileTypes.VCF
        command = f'bcftools query -l {fp}'
    else:
        LOGGER.critical(f'could not infer file type from extension for {fp}')
        RESULTS_DATA['inferred_filetype'] = 'failed'
        RESULTS_DATA['valid_filetype'] = 'no'
        write_results_s3(file_info)
        sys.exit(1)
    # Validate filetype
    RESULTS_DATA['inferred_filetype'] = filetype.value
    result = util.execute_command(command)
    if result.returncode != 0:
        stdstrm_msg = f'\r\tstdout: {result.stdout}\r\tstderr {result.stderr}'
        LOGGER.info(f'file validation failed (invalid filetype or other failure): {stdstrm_msg}')
        RESULTS_DATA['valid_filetype'] = 'no'
        write_results_s3(file_info)
        sys.exit(1)
    else:
        RESULTS_DATA['valid_filetype'] = 'yes'
    # Log results
    inferred_str  = f'inferred:  {RESULTS_DATA["inferred_filetype"]}'
    validated_str = f'validated:  {RESULTS_DATA["valid_filetype"]}'
    filetype_str = f'{inferred_str}\r\t{validated_str}'
    LOGGER.info(f'file type validation results:\r\t{filetype_str}')
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
    result = util.execute_command(command)
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
    RESULTS_DATA['index_s3_bucket'] = RESULTS_BUCKET
    RESULTS_DATA['index_s3_key'] = index_s3_key
    # Log results
    result_str = f'result:    {RESULTS_DATA["index_result"]}'
    filename_str = f'filename:  {RESULTS_DATA["index_filename"]}'
    bucket_str = f'S3 bucket: {RESULTS_DATA["index_s3_bucket"]}'
    key_str = f'S3 key:    {RESULTS_DATA["index_s3_key"]}'
    filetype_str = f'{result_str}\r\t{filename_str}\r\t{bucket_str}\r\t{key_str}'
    LOGGER.info(f'file indexing results:\r\t{filetype_str}')


def get_results_data_s3_key(file_info):
    s3_key_fn = f'{file_info["filename"]}__results.json'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def get_log_s3_key(file_info):
    s3_key_fn = f'{file_info["filename"]}__log.txt'
    return os.path.join(RESULTS_KEY_PREFIX, s3_key_fn)


def upload_index(file_info, index_fp):
    s3_key = os.path.join(RESULTS_KEY_PREFIX, index_fp)
    LOGGER.info(f'writing index to s3://{RESULTS_BUCKET}/{s3_key}')
    CLIENT_S3.upload_file(index_fp, RESULTS_BUCKET, s3_key)
    return s3_key


def write_results_s3(file_info):
    # Create results json
    data = {
        'existing_record': file_info,
        'validation_results': RESULTS_DATA
    }
    s3_object_body = f'{json.dumps(data, indent=4)}\n'
    # Upload results and log to S3
    s3_key = get_results_data_s3_key(file_info)
    s3_object_body_log = s3_object_body.replace('\n', '\r')
    LOGGER.info(f'writing results to s3://{RESULTS_BUCKET}/{s3_key}:\r{s3_object_body_log}')
    CLIENT_S3.put_object(Body=s3_object_body, Bucket=RESULTS_BUCKET, Key=s3_key)
    upload_log(file_info)


def upload_log(file_info):
    s3_key = get_log_s3_key(file_info)
    LOGGER.info(f'writing log file to s3://{RESULTS_BUCKET}/{s3_key}')
    LOG_FILE_HANDLER.flush()
    CLIENT_S3.upload_file(LOG_FILE_NAME, RESULTS_BUCKET, s3_key)


if __name__ == '__main__':
    main()
