#!/usr/bin/env python3
import uuid
import os
import re
import textwrap
import enum
import json

import util
import util.agha as agha

JOB_NAME_RE = re.compile(r'[.\\/]')

FINISH_STATUS = ['SUCCESS', 'FAILURE']

BATCH_QUEUE_NAME = os.environ.get('BATCH_QUEUE_NAME')
JOB_DEFINITION_ARN = os.environ.get('JOB_DEFINITION_ARN')

# Dynamodb table name
DYNAMODB_STAGING_TABLE_NAME = os.environ.get('DYNAMODB_STAGING_TABLE_NAME')

# Bucket names
STAGING_BUCKET = os.environ.get('STAGING_BUCKET')
RESULTS_BUCKET = os.environ.get('RESULTS_BUCKET')


########################################################################################################################
# Batch result result file
class StatusBatchResult(enum.Enum):
    SUCCEED = 'SUCCEED'
    FAIL = 'FAIL'
    RUNNING = 'RUNNING'


class BatchJobResult:

    def __init__(self, staging_s3_key='', task_type='', value='', status='', source_file=None):
        self.staging_s3_key = staging_s3_key
        self.task_type = task_type
        self.value = value
        self.status = status
        self.source_file = source_file


########################################################################################################################
# Tasks available for batch task

class Tasks(enum.Enum):
    CHECKSUM_VALIDATION = 'CHECKSUM_VALIDATION'
    FILE_VALIDATION = 'FILE_VALIDATION'
    INDEX = 'CREATE_INDEX'
    COMPRESS = 'CREATE_COMPRESS'

    @staticmethod
    def is_valid(name: str) -> bool:
        for t in Tasks:
            if t.value == name:
                return True
        return False

    @staticmethod
    def tasks_to_list() -> list:
        return [task.value for task in Tasks]

    @staticmethod
    def tasks_create_file() -> list:
        return [Tasks.COMPRESS.value, Tasks.INDEX.value]


def get_tasks_list(filename: str = None):
    """
    Trigger which tasks should run
    """

    tasks_list = list()
    filetype = agha.FileType.from_name(filename).get_name()

    # Always run checksum
    tasks_list.append(Tasks.CHECKSUM_VALIDATION.value)

    # Always validate filetype
    tasks_list.append(Tasks.FILE_VALIDATION.value)

    # Always create index if supported (In this case BAM, CRAM and VCF file)
    if agha.FileType.is_indexable(filetype) or filename is None:  # Appended by default
        tasks_list.append(Tasks.INDEX.value)

    # Always compress file when it is uncompressed for FASTQ and VCF
    if agha.FileType.is_compressable(filetype) or filename is None:  # Appended by default
        tasks_list.append(Tasks.COMPRESS.value)

    return tasks_list


def create_job_data(s3_key: str, partition_key: str, tasks_list, output_prefix, filesize, checksum=None):
    name_raw = f'agha_validation__{s3_key}__{partition_key}'
    name = JOB_NAME_RE.sub('_', name_raw)
    # Job name must be less than 128 characters. If job name exceeds this length, truncate to the
    # first 120 characters and append a 7 character uid separated by an underscore.
    if len(name) > 128:
        name = f'{name[:120]}_{uuid.uuid1().hex[:7]}'

    command = []

    # append s3_key args
    command.extend(["--s3_key", s3_key])

    # append checksum args
    if checksum != None:
        command.extend(["--checksum", checksum])

    # append task lists args
    task_list = ["--tasks"]
    task_list.extend(tasks_list)
    command.extend(task_list)

    return {'name': name, 'command': command, 'output_prefix': output_prefix, 'filesize': filesize}


def find_suitble_batch_queue_name(filesize: int):
    '''
    To define appropriate job queue for the job.
    :param filesize: The size needed for the job
    :return:
    '''

    batch_queue_name_json = json.loads(BATCH_QUEUE_NAME)

    if filesize > 150000000000:  # Above 150gb
        return batch_queue_name_json['xlarge']
    elif filesize > 100000000000:  # Above 100gb
        return batch_queue_name_json['large']
    elif filesize > 50000000000:  # Above 50 gb
        return batch_queue_name_json['medium']
    else:
        return batch_queue_name_json['small']


def submit_batch_job(job_data):
    client_batch = util.get_client('batch')

    command = job_data['command']
    environment = [
        {'name': 'RESULTS_BUCKET', 'value': RESULTS_BUCKET},
        {'name': 'STAGING_BUCKET', 'value': STAGING_BUCKET},
        {'name': 'DYNAMODB_STAGING_TABLE_NAME', 'value': DYNAMODB_STAGING_TABLE_NAME},
        {'name': 'RESULTS_KEY_PREFIX', 'value': job_data['output_prefix']},
    ]
    res = client_batch.submit_job(
        jobName=job_data['name'],
        jobQueue=find_suitble_batch_queue_name(job_data['filesize']),
        jobDefinition=JOB_DEFINITION_ARN,
        containerOverrides={
            'environment': environment,
            'command': command,
            'resourceRequirements': [
                {
                    'value': '7000',
                    'type': 'MEMORY'
                },
                {
                    'value': '2',
                    'type': 'VCPU'
                }
            ]
        }
    )
    return res
