#!/usr/bin/env python3
import io
import logging
import os
import re
import sys
import textwrap
import uuid


import boto3
import pandas as pd


import util


# Logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Get environment variables
STAGING_BUCKET = util.get_environment_variable('STAGING_BUCKET')
RESULTS_BUCKET = util.get_environment_variable('RESULTS_BUCKET')
DYNAMODB_TABLE = util.get_environment_variable('DYNAMODB_TABLE')
BATCH_QUEUE_NAME = util.get_environment_variable('BATCH_QUEUE_NAME')
JOB_DEFINITION_ARN = util.get_environment_variable('JOB_DEFINITION_ARN')
FOLDER_LOCK_LAMBDA_ARN = util.get_environment_variable('FOLDER_LOCK_LAMBDA_ARN')
SLACK_NOTIFY = util.get_environment_variable('SLACK_NOTIFY')
EMAIL_NOTIFY = util.get_environment_variable('EMAIL_NOTIFY')
SLACK_HOST = util.get_environment_variable('SLACK_HOST')
SLACK_CHANNEL = util.get_environment_variable('SLACK_CHANNEL')
MANAGER_EMAIL = util.get_environment_variable('MANAGER_EMAIL')
SENDER_EMAIL = util.get_environment_variable('SENDER_EMAIL')

# Get AWS clients, resources
CLIENT_BATCH = util.get_client('batch')
CLIENT_LAMBDA = util.get_client('lambda')
CLIENT_S3 = util.get_client('s3')
CLIENT_SES = util.get_client('ses')
CLIENT_SSM = util.get_client('ssm')
RESOURCE_DYNAMODB = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)

# Get SSM value
SLACK_WEBHOOK_ENDPOINT = util.get_ssm_parameter(
    '/slack/webhook/endpoint',
    CLIENT_SSM,
    with_decryption=True
)

# Other
TASKS_AVAILABLE = ['checksum', 'validate_filetype', 'create_index']
EMAIL_SUBJECT = '[AGHA service] Submission received'
MANIFEST_REQUIRED_COLUMNS = {'filename', 'checksum', 'agha_study_id'}
JOB_NAME_RE = re.compile(r'[.\\/]')
MESSAGE_STORE = list()

# Manifest field validation related
AGHA_ID_RE = re.compile('^A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?$|^unknown$')
MD5_RE = re.compile('^[0-9a-f]{32}$')
FLAGSHIPS = {
    'ACG',
    'BM',
    'CARDIAC',
    'CHW',
    'EE',
    'GI',
    'HIDDEN',
    'ICCON',
    'ID',
    'KidGen',
    'LD',
    'MCD',
    'MITO',
    'NMD'
}


# Collection of input/submission data
class SubmissionData:

    # TODO: update these fields and check they are consistent for event types
    def __init__(self, record):
        self.record = record

        self.manifest_key = str()
        self.submission_prefix = str()
        self.flagship = str()
        self.study_id = str()

        self.file_metadata = list()
        self.file_etags = dict()

        self.manifest_data = pd.DataFrame()

        self.files_accepted = list()
        self.files_rejected = list()
        self.files_extra = list()


class FileRecord:

    def __init__(self):
        self.filename = str()
        self.output_prefix = str()
        self.flagship = str()
        self.study_id = str()
        self.s3_key = str()
        self.s3_etag = str()
        self.provided_checksum = str()
        self.file_size = int()

    @classmethod
    def from_manifest_record(cls, filename, output_prefix, data):
        # Check for some required data, and get record from manifest data
        assert not data.manifest_data.empty
        assert data.file_etags
        file_info = data.manifest_data.loc[data.manifest_data['filename']==filename].iloc[0]
        # Create class instance
        record = cls()
        record.filename = filename
        record.output_prefix = output_prefix
        record.flagship = data.flagship
        record.study_id = file_info['agha_study_id']
        record.submission_name = data.submission_prefix
        record.s3_key = os.path.join(data.submission_prefix, filename)
        record.s3_etag = data.file_etags[filename]
        record.provided_checksum = file_info['checksum']
        record.file_size = data.file_sizes[filename]
        return record


    @classmethod
    def from_filepath(cls, filepath, output_prefix, data):
        assert data.file_etags
        record = cls()
        record.filename = os.path.basename(filepath)
        record.output_prefix = output_prefix
        record.submission_name = data.submission_prefix
        record.s3_key = filepath
        record.s3_etag = data.file_etags[record.filename]
        record.file_size = data.file_sizes[filename]
        return record


class SubmitterInfo:

    def __init__(self):
        self.name = str()
        self.email = str()
        self.submission_prefix = str()


####################
# S3 OPERATIONS
####################
def get_flagship_from_key(s3_key, submitter_info=None, strict_mode=True):
    flagship_str, *others = s3_key.split('/')
    # Strict mode requires case matching for now
    if strict_mode:
        flagship = flagship_str
        flagship_list = FLAGSHIPS
    else:
        flagship = flagship_str.lower()
        flagship_list = {fs.lower() for fs in FLAGSHIPS}
    # Check discovered flagship is known
    if flagship not in flagship_list:
        flagships_str = '\r\t'.join(FLAGSHIPS)
        comp_type = 'sensitive' if strict_mode else 'insensitive'
        message_base = f'got unrecognised flagship \'{flagship}\', expected one from (case-{comp_type})'
        log_and_store_message(f'{message_base}:\r\t{flagships_str}', level='critical')
        notify_and_exit(submitter_info)
    return flagship_str


def get_s3_object_metadata(data, submitter_info=None):
    try:
        return util.get_s3_object_metadata(data.bucket_name, data.submission_prefix, CLIENT_S3)
    except Exception as e:
        message = f'could not retrieve manifest data from S3:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(submitter_info)


def get_s3_etags_by_filename(metadata_records):
    return {get_s3_filename(md): md.get('ETag') for md in metadata_records}


def get_s3_filesizes_by_filename(metadata_records):
    return {get_s3_filename(md): md.get('Size') for md in metadata_records}


def get_s3_filename(metadata_record):
    filepath = metadata_record.get('Key')
    return os.path.basename(filepath)


def get_output_prefix(submission_prefix):
    output_dn = f'{util.get_datetimestamp()}_{uuid.uuid1().hex[:7]}'
    return os.path.join(submission_prefix, output_dn)


####################
# MANIFEST
####################
def retrieve_manifest_data(data, submitter_info=None):
    LOGGER.info(f'Getting manifest from: {data.bucket_name}/{data.manifest_key}')
    try:
        manifest_obj = CLIENT_S3.get_object(Bucket=data.bucket_name, Key=data.manifest_key)
    except botocore.exceptions.ClientError as e:
        message = f'could not retrieve manifest data from S3:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(submitter_info)
    try:
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_data = pd.read_csv(manifest_str, sep='\t', encoding='utf8')
        manifest_data.fillna(value='not provided', inplace=True)
    except Exception as e:
        message = f'could not convert manifest into DataFrame:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(submitter_info)
    return manifest_data


def validate_manifest(data, submitter_info=None, strict_mode=True):
    # Check manifest columns
    columns_present = set(data.manifest_data.columns.tolist())
    columns_missing = MANIFEST_REQUIRED_COLUMNS.difference(columns_present)
    if columns_missing:
        plurality = 'column' if len(columns_missing) == 1 else 'columns'
        cmissing_str = '\r\t'.join(columns_missing)
        cfound_str = '\r\t'.join(columns_present)
        message_base = f'required {plurality} missing from manifest:'
        log_and_store_message(f'{message_base}\r\t{cmissing_str}\rGot:\r\t{cfound_str}', level='critical')
        notify_and_exit(submitter_info)

    # File discovery
    # Entry count
    log_and_store_message(f'Entries in manifest: {len(data.manifest_data)}')
    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_s3 = {get_s3_filename(md) for md in data.file_metadata}
    log_and_store_file_message(message_text, files_s3)
    # Files missing from S3
    files_manifest = set(data.manifest_data['filename'].to_list())
    files_missing_from_s3 = files_manifest.difference(files_s3)
    message_text = f'Entries in manifest, but not on S3'
    log_and_store_file_message(message_text, files_missing_from_s3)
    # Extra files present on S3
    files_missing_from_manifest = files_s3.difference(files_manifest)
    message_text = f'Entries on S3, but not in manifest'
    log_and_store_file_message(message_text, files_missing_from_manifest)
    # Files present in manifest *and* S3
    files_matched = files_manifest.intersection(files_s3)
    message_text = f'Entries matched in manifest and S3'
    log_and_store_file_message(message_text, files_matched)
    # Matched files that are accepted. Here files such as indices are filtered.
    files_matched_prohibited = list()
    files_matched_accepted = list()
    for filename in files_matched:
        if any(filename.endswith(fext) for fext in util.FEXT_ACCEPTED):
            files_matched_accepted.append(filename)
        else:
            files_matched_prohibited.append(filename)
    message_text = f'Matched entries excluded on file extension'
    log_and_store_file_message(message_text, files_matched_prohibited)
    message_text = f'Matched entries eligible for validation'
    log_and_store_file_message(message_text, files_matched_accepted)
    # Record error messages for extra files (other than manifest.txt) or missing files
    if 'manifest.txt' in files_missing_from_manifest:
        files_missing_from_manifest.remove('manifest.txt')
    messages_error = list()
    if files_missing_from_s3:
        messages_error.append('files listed in manifest were absent from S3')
    if files_missing_from_manifest:
        messages_error.append('files found on S3 absent from manifest.tsv')

    # Field validation
    for row in data.manifest_data.itertuples():
        # Study ID
        if not AGHA_ID_RE.match(row.agha_study_id):
            message = f'got malformed AGHA study ID for {row.Index} ({row.agha_study_id})'
            messages_error.append(message)
        # Checksum
        if not MD5_RE.match(row.checksum):
            message = f'got malformed MD5 checksum for {row.Index} ({row.checksum})'
            messages_error.append(message)

    # Check for error messages, exit in strict mode otherwise just emit warnings
    if messages_error:
        plurality = 'message' if len(messages_error) == 1 else 'messages'
        errors = '\r\t'.join(messages_error)
        message_base = f'Manifest failed validation with the following {plurality}'
        message = f'{message_base}:\r\t{errors}'
        if strict_mode:
            log_and_store_message(message, level='critical')
            notify_and_exit(submitter_info)
        else:
            log_and_store_message(message, level='warning')

    # Notify with success message
    message = f'Manifest successfully validated, continuing with file validation'
    log_and_store_message(message)
    send_notifications(
        MESSAGE_STORE,
        EMAIL_SUBJECT,
        submitter_info,
    )
    # NOTE(SW): returning files that are on S3 and (1) in manifest, and (2) not in manifest. This
    # will allow flexbility in the future for any refactor if we decide to modify handling of
    # missing files
    return files_matched_accepted, list(files_missing_from_manifest)


####################
# DynamoDB
####################
def get_existing_records(partition_key):
    records = get_records(partition_key)
    if records:
        records_current = [r for r in records if r['active']]
        if len(records_current) != 1:
            msg_records = '\r\t'.join(r.__repr__() for r in records_current)
            msg_base = f'expected one active record but got {len(records_current)}'
            msg = f'{msg_base}:\r\t{msg_records}'
            log_and_store_message(msg, level='critical')
            sys.exit(1)
    return records


def get_file_number(records):
    records_active = [r for r in records if r['active']]
    if not records_active:
        file_number = 0
    elif len(records_active) > 1:
        LOGGER.critical(f'found more than one active record: {records_active}')
        sys.exit(1)
    else:
        [record_current] = records_active
        file_number = record_current['file_number']
    return file_number


def get_records(partition_key):
    records = list()
    response = RESOURCE_DYNAMODB.query(
        KeyConditionExpression=boto3.dynamodb.conditions.Key('partition_key').eq(partition_key)
    )
    if 'Items' not in response:
        message = f'could not any records using partition key ({partition_key}) in {DYNAMODB_TABLE}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = DYNAMODB_TABLE.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('partition_key').eq(partition_key),
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    return records


def create_record(partition_key, sort_key, data):
    record = {
        'partition_key': partition_key,
        'sort_key': sort_key,
        'active': True,
        'study_id': data.study_id,
        'flagship': data.flagship,
        'filename': data.filename,
        'file_number': sort_key,
        'file_size': data.file_size,
        # Checksum
        'provided_checksum': data.provided_checksum,
        'calculated_checksum': 'not run',
        'valid_checksum': 'not run',
        # File type
        'inferred_filetype': 'not run',
        'valid_filetype': 'not run',
        # Index
        'index_result': 'not run',
        'index_filename': 'na',
        # Storage details
        's3_bucket': STAGING_BUCKET,
        's3_key': data.s3_key,
        'index_s3_bucket': 'na',
        'index_s3_key': 'na',
        'results_s3_bucket': 'na',
        'results_data_s3_key': 'na',
        'results_log_s3_key': 'na',
        # Misc
        'tasks_completed': 'no',
        'etag': data.s3_etag,
        'ts_record_creation': util.get_datetimestamp(),
        'ts_record_update': 'na',
        'ts_validation_job': 'na',
        'ts_moved_storage': 'na',
        'excluded': False,
    }
    LOGGER.info(f'creating record for {data.filename}: {record}')
    RESOURCE_DYNAMODB.put_item(Item=record)
    return record


def update_record(partition_key, sort_key, data):
    record_data = {
        'active': True,
        'filename': data.filename,
        'flagship': data.flagship,
        'study_id': data.study_id,
        'submission_name': data.submission_name,
        's3_key': data.s3_key,
        'etag': data.s3_etag,
        'provided_checksum': data.provided_checksum,
        'file_size': data.file_size,
        'ts_record_update': util.get_datetimestamp(),
    }
    results_str = '\r\t'.join(f'{k}: {v}' for k, v in record_data.items())
    # Get update expression string and attribute values
    update_expr_items = list()
    attr_values = dict()
    for i, k in enumerate(record_data):
        update_expr_key = f':{i}'
        assert update_expr_key not in attr_values
        update_expr_items.append(f'{k} = {update_expr_key}')
        attr_values[update_expr_key] = record_data[k]
    update_expr_items_str = ', '.join(update_expr_items)
    update_expr = f'SET {update_expr_items_str}'
    # Update record
    response = RESOURCE_DYNAMODB.update_item(
        Key={
            'partition_key': partition_key,
            'sort_key': sort_key,
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=attr_values,
        ReturnValues='ALL_NEW',
    )
    return response.get('Attributes')


def inactivate_existing_records(records):
    for record in records:
        if not record['active']:
            continue
        RESOURCE_DYNAMODB.update_item(
            Key={
                'partition_key': record['partition_key'],
                'sort_key': record['sort_key'],
            },
            UpdateExpression='SET active = :a',
            ExpressionAttributeValues={':a': False},
        )


####################
# BATCH
####################
def get_tasks_list(record):
    tasks_list = list()
    if record['valid_checksum'] == 'not run':
        tasks_list.append('checksum')
    if record['valid_filetype'] == 'not run':
        tasks_list.append('validate_filetype')
    if record['index_result'] == 'not run':
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
        --sort_key {sort_key} \
        --tasks {tasks}
    ''')
    return {'name': name, 'command': command, 'output_prefix': file_record.output_prefix}


def submit_batch_job(job_data):
    command = ['bash', '-o', 'pipefail', '-c', job_data['command']]
    environment = [
        {'name': 'RESULTS_BUCKET', 'value': RESULTS_BUCKET},
        {'name': 'DYNAMODB_TABLE', 'value': DYNAMODB_TABLE},
        {'name': 'RESULTS_KEY_PREFIX', 'value': job_data['output_prefix']},
    ]
    CLIENT_BATCH.submit_job(
        jobName=job_data['name'],
        jobQueue=BATCH_QUEUE_NAME,
        jobDefinition=JOB_DEFINITION_ARN,
        containerOverrides={
            'memory': 4000,
            'environment': environment,
            'command': command
        }
    )


####################
# LOGGING
####################
def log_and_store_file_message(message_text, files):
    # Notification only gets summary message; Lambda log gets both summary and full
    message_summary = f'{message_text}: {len(files)}'
    log_and_store_message(message_summary)
    if files:
        files_str = '\r\t'.join(files)
        LOGGER.info(f'{message_text}:\r\t{files_str}')


def log_and_store_message(message, level='info'):
    level_number = logging.getLevelName(level.upper())
    LOGGER.log(level_number, message)
    # Prefix message with 'ERROR' for display in notifications
    if level in {'error', 'critical'}:
        message = f'ERROR: {message}'
    MESSAGE_STORE.append(message)


def notify_and_exit(submitter_info):
    if submitter_info:
        send_notifications(
            MESSAGE_STORE,
            EMAIL_SUBJECT,
            submitter_info,
        )
    sys.exit(1)


def send_notifications(messages, subject, info):
    if EMAIL_NOTIFY == 'yes':
        LOGGER.info(f'Sending notifications messages', end='\r')
        LOGGER.info(*messages, sep='\r')
        LOGGER.info(f'Sending email to {info.submitter_name} <{info.submitter_email}>')
        recipients = [MANAGER_EMAIL, info.submitter_email]
        email_body = make_email_body_html(
            info.submission_prefix,
            info.submitter_name,
            messages
        )
        email_response = util.send_email(
            recipients,
            EMAIL_SENDER,
            EMAIL_SUBJECT,
            email_body,
            SES_CLIENT
        )
    if SLACK_NOTIFY == 'yes':
        LOGGER.info(f'Sending notification to {SLACK_CHANNEL}')
        slack_response = util.call_slack_webhook(
            subject,
            f'Submission: {submission_prefix} ({submitter_name})',
            '\n'.join(messages),
            SLACK_HOST,
            SLACK_CHANNEL,
            SLACK_WEBHOOK_ENDPOINT
        )
        LOGGER.info(f'Slack call response: {slack_response}')
