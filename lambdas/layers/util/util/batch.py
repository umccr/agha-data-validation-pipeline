#!/usr/bin/env python3
import uuid
import os
import re
import textwrap
from lambdas.functions.file_processor.shared import DYNAMODB_TABLE

import util

JOB_NAME_RE = re.compile(r'[.\\/]')

TASKS_AVAILABLE = ['checksum', 'validate_filetype', 'create_index']
FINISH_STATUS = ['SUCCESS', 'FAILURE']
BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
JOB_DEFINITION_ARN = os.environ.get('JOB_DEFINITION_ARN')
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')

RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')

def get_tasks_list():
    """
    Trigger which tasks should run
    """

    tasks_list = list()

    # Check on which have not exist/run yet
    # For time being append all test to the list

    # if record['valid_checksum'] == 'not run':
    #     tasks_list.append('checksum')
    # if record['valid_filetype'] == 'not run':
    #     tasks_list.append('validate_filetype')
    # if record['index_result'] == 'not run':
    #     tasks_list.append('create_index')

    tasks_list.append('checksum')
    tasks_list.append('validate_filetype')
    tasks_list.append('create_index')

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
        {'name': 'DYNAMODB_TABLE', 'value': DYNAMODB_TABLE},
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
   