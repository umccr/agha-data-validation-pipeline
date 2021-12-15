#!/usr/bin/env python3
import uuid
import os
import re
import textwrap
import enum

import util

JOB_NAME_RE = re.compile(r'[.\\/]')

TASKS_AVAILABLE = ['checksum', 'validate_filetype', 'create_index']
FINISH_STATUS = ['SUCCESS', 'FAILURE']

BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
JOB_DEFINITION_ARN = os.environ.get('JOB_DEFINITION_ARN')

# Dynamodb table name
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')

# Bucket names
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')


class Tasks(enum.Enum):
    CHECKSUM = 'checksum'
    FILE_VALIDATE = 'validate_filetype'
    INDEX = 'create_index'
    COMPRESS = 'create_compress'


def get_tasks_list():
    """
    Trigger which tasks should run
    """

    tasks_list = list()

    # Always run checksum
    tasks_list.append(Tasks.CHECKSUM.value)

    # Always validate filetype
    tasks_list.append(Tasks.FILE_VALIDATE.value)

    # Always create index if supported (In this case BAM and VCF file)
    tasks_list.append(Tasks.INDEX.value)

    # Always compress file when it is uncompressed for FASTQ and VCF
    tasks_list.append(Tasks.COMPRESS.value)

    return tasks_list


def create_job_data(partition_key, sort_key, tasks_list, file_record):
    name_raw = f'agha_validation__{partition_key}__{sort_key}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'
    tasks = ' '.join(tasks_list)
    command = textwrap.dedent(f'''
        /opt/validate_file.py \
        --partition_key {partition_key} \
        --tasks {tasks}
    ''')
    return {'name': name, 'command': command, 'output_prefix': file_record.output_prefix}


def submit_batch_job(job_data):
    client_batch = util.get_client('batch')

    command = ['bash', '-o', 'pipefail', '-c', job_data['command']]
    environment = [
        {'name': 'RESULTS_BUCKET', 'value': RESULTS_BUCKET},
        {'name': 'STAGING_BUCKET', 'value': STAGING_BUCKET},
        {'name': 'DYNAMODB_STAGING_TABLE_NAME', 'value': DYNAMODB_STAGING_TABLE_NAME},
        {'name': 'RESULTS_KEY_PREFIX', 'value': job_data['output_prefix']},
    ]
    client_batch.submit_job(
        jobName=job_data['name'],
        jobQueue=BATCH_QUEUE_NAME,
        jobDefinition=JOB_DEFINITION_ARN,
        containerOverrides={
            'memory': 4000,
            'environment': environment,
            'command': command
        }
    )
