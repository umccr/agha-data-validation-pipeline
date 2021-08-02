#!/usr/bin/env python3
import io
import json
import os


import boto3
import botocore.exceptions
import pandas as pd


import shared


# Logging and message store
LOGGER = shared.LOGGER
MESSAGES_STORE = list()

# Environment variables
# Set with error handling
STAGING_BUCKET = None
DYNAMODB_TABLE = None
BATCH_QUEUE_NAME = None
JOB_DEFINITION_NAME = None

# Other
EMAIL_SUBJECT = '[AGHA service] Submission received'
MANIFEST_REQUIRED_COLUMNS = {'filename', 'checksum', 'agha_study_id'}


# Collection of input/submission data
class SubmissionData:

    def __init__(self, record):
        self.record = record

        self.submitter_name = str()
        self.submitter_email = str()

        self.submission_prefix = str()
        self.bucket_name = str()
        self.manifest_key = str()

        self.manifest_data = pd.DataFrame()
        self.manifest_files = list()
        self.extra_files = list()


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(context)}')

    # Get environment variables
    # Lambda-specific
    STAGING_BUCKET = shared.get_environment_variable('STAGING_BUCKET')
    DYNAMODB_TABLE = shared.get_environment_variable('DYNAMODB_TABLE')
    BATCH_QUEUE_NAME = shared.get_environment_variable('BATCH_QUEUE_NAME')
    JOB_DEFINITION_NAME = shared.get_environment_variable('JOB_DEFINITION_NAME')
    # Shared
    shared.SLACK_NOTIFY = shared.get_environment_variable('SLACK_NOTIFY')
    shared.EMAIL_NOTIFY = shared.get_environment_variable('EMAIL_NOTIFY')
    shared.SLACK_HOST = shared.get_environment_variable('SLACK_HOST')
    shared.SLACK_CHANNEL = shared.get_environment_variable('SLACK_CHANNEL')
    shared.MANAGER_EMAIL = shared.get_environment_variable('MANAGER_EMAIL')
    shared.SENDER_EMAIL = shared.get_environment_variable('SENDER_EMAIL')

    # Get AWS clients
    shared.CLIENT_BATCH = shared.get_client('batch')
    shared.CLIENT_DYNAMODB = shared.get_client('dynamodb')
    shared.CLIENT_IAM = shared.get_client('iam')
    shared.CLIENT_S3 = shared.get_client('s3')
    shared.CLIENT_SES = shared.get_client('ses')
    shared.CLIENT_SSM = shared.get_client('ssm')

    # Get SSM value
    shared.SLACK_WEBHOOK_ENDPOINT = shared.get_ssm_parameter(
        '/slack/webhook/endpoint',
        with_decryption=True
    )

    # Perform some checks before processing begins
    # Check we can access the staging bucket
    response = CLIENT_S3.list_buckets()
    buckets_available = response.get('Buckets', list())
    if STAGING_BUCKET not in buckets_available:
        buckets_str = '\r\t'.join(buckets)
        LOGGER.critical(f'could not find S3 bucket \'{STAGING_BUCKET}\', got:\r{buckets_str}')
        sys.exit(1)
    # Check job definition exists
    response = CLIENT_BATCH.describe_job_definitions(jobDefinitionName=JOB_DEFINITION_NAME)
    job_definitions = response.get('jobDefinitions', list())
    if not job_definitions:
        LOGGER.critical(f'could not find job definition \'{JOB_DEFINITION_NAME}\'')
        sys.exit(1)
    elif len(job_definitions) > 1:
        job_definition_names = [jd.get('jobDefinitionName') for jd in job_definitions]
        LOGGER.critical(f'got more than one job definition:\r{job_definition_names}')
        sys.exit(1)

    # Parse event data and get record
    record = process_event_data(event)
    data = Submission(record)

    # Get name and email from record
    if record.get('eventSource') == 'aws:s3' and 'userIdentity' in record and 'principalId' in record['userIdentity']:
        principal_id = record['userIdentity']['principalId']
        data.submitter_name, data.submitter_email = shared.get_name_email_from_principalid(principal_id)
        LOGGER.info(f'Extracted name and email from record: {data.submitter_name} <{data.submitter_email}>')
    else:
        LOGGER.warning(f'Could not extract name and email: unsuitable event type/data')

    # Get manifest submission S3 key prefix
    data.bucket_name = record['s3']['bucket']['name']
    data.manifest_key = record['s3']['object']['key']
    data.submission_prefix = os.path.dirname(manifest_key)
    LOGGER.info(f'Submission with prefix: {data.submission_prefix}')

    # Collect manifest data and then validate
    data.manifest_data = retrieve_manifest_data(data)
    data.manifest_files, data.extra_files = validate_manfiest_data(data)

    # Process each record
    # NOTE(SW): this logic ignores files that are on S3 but absent from manifest
    for manifest_file in data.manifest_files:
        process_manifest_entry(manifest_file, data)

    # If any submission sucessful, enable CloudWatch event (scheduled to run lambda every n
    # minutes). This lambda will:
    #   1. check how mnay jobs are running for a group
    #   2. if at least one running, do nothing
    #   3. if none are running, query results and send email

    # Alternatively, we just enable the above regardless to allow handling an notification of any
    # Batch failures i.e. if all fail to submit that will then be reported


def process_event_data(event):
    if 'Records' not in event:
        LOGGER.critical('no \'s3\' entry found in record')
        sys.exit(1)

    records_n = len(event['Records'])
    if records_n > 1:
        LOGGER.critical(f'Expected one manifest record but got {records_n}')
        sys.exit(1)
    [record] = event['Records']

    if 's3' not in record:
        LOGGER.critical('no \'s3\' entry found in record')
        sys.exit(1)
    record_s3 = record['s3']

    if 'bucket' not in record_s3:
        LOGGER.critical('S3 record missing bucket info')
        sys.exit(1)
    elif 'name' not in record_3['bucket']:
        LOGGER.critical('S3 bucket record missing name info')
        sys.exit(1)

    if 'object' not in record_3:
        LOGGER.critical('S3 record missing object info')
        sys.exit(1)
    elif 'key' not in record_3['object']:
        LOGGER.critical('S3 object record missing key info')
        sys.exit(1)

    if record_s3['bucket']['name'] != STAGING_BUCKET:
        LOGGER.critical(f'expected {STAGING_BUCKET} bucket but got {bucket_name}')
        sys.exit(1)
    return record


def retrieve_manifest_data(data):
    LOGGER.info(f'Getting manifest from: {data.bucket_name}/{data.manifest_key}')
    try:
        manifest_obj = S3_CLIENT.get_object(Bucket=data.bucket_name, Key=data.manifest_key)
    except botocore.exceptions.ClientError as e:
        message = f'could not retrieve manifest data from S3:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    try:
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_data = pd.read_csv(manifest_str, sep='\t', index_col='filename', encoding='utf8')
    except Exception as e:
        message = f'could not convert manifest into DataFrame:\r{e}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    return manifest_data


def validate_manifest(data):
    # Head validation
    columns_missing = MANIFEST_REQUIRED_COLUMNS.difference(data.manifest_data.columns)
    if columns_missing:
        plurality = 'column' if len(columns_missing) == 1 else 'columns'
        cmissing_str = '\r\t'.join(columns_missing)
        cfound_str = '\r\t'.join(data.manifest_data.columns)
        message_base = f'required {plurality} missing from manifest:'
        log_and_store_message(f'{message_base}\r{cmissing_str}\rGot:\r{cfound_str}', level='critical')
        notify_and_exit(data)

    # Entry validation
    # Entry count
    log_and_store_message(f'Entries in manifest: {len(data.manifest_data)}')
    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_s3 = get_s3_filenames(data.bucket_name, data.submission_prefix)
    log_and_store_file_message(message_text, files_s3)
    # Files missing from S3
    files_manifest = set(manifest_df['filename'].to_list())
    files_missing_from_s3 = files_manifest.difference(files_s3)
    message_text = f'Entries in manifest, but not on S3'
    log_and_store_file_message(message_text, files_missing_from_s3)
    # Extra files present on S3
    files_missing_from_manifest = files_s3.difference(files_manifest)
    message_text = f'Entries on S3, but not in manifest'
    log_and_store_file_message(message_text, files_missing_from_manifest)
    # Files present in manifest *and* S3
    files_matched = files_manifest.intersection(files_s3)
    message_text = f'Entries common in manifest and S3'
    log_and_store_file_message(message_text, files_matched)
    # Fail if there are extra files (other than manifest.txt) or missing files
    if 'manifest.txt' in files_missing_from_manifest:
        files_missing_from_s3.remove('manifest.txt')
    messages_error = list()
    if files_missing_from_s3:
        messages_error.append('files listed in manifest were absent from S3')
    # NOTE(SW): failing on this might be too strict; we may want to re-run validation on some
    # files in-place. Though this would probably be triggered through a different entry point.
    # Strict manifest validation could be appropriate here in that case.
    if files_missing_from_manifest:
        messages_error.append('files found on S3 absent from manifest.tsv')
    if messages_error:
        plurality = 'message' if len(manifest_error_messages) == 1 else 'messages'
        errors = '\t\r'.join(manifest_error_messages)
        message_base = f'Manifest failed validation with the following {plurality}'
        message = f'{message_base}:\r{errors}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)

    # Notify with success message
    message = f'Manifest succesfully validated, continuing with file validation'
    log_and_store_message(message)
    shared.send_notifications(
        MESSAGE_STORE,
        EMAIL_SUBJECT,
        data.submitter_name,
        data.submitter_email,
        data.submission_prefix,
    )
    # NOTE(SW): returning files that are on S3 and (1) in manifest, and (2) not in manifest. This
    # will allow flexbility in the future for any refactor if we decide to modify handling of
    # missing files
    return list(files_matched), list(files_missing_from_manifest)


def process_manifest_entry(filename, data, job_definition):
    # Construct partition key and sort key
    study_id = data.manifest_data['agha_study_id'].loc[filename]
    key_partition = f'{study_id}_{filename}'

    # Get sort key
    # NOTE(SW): currently creating new record if previous one exists. Could modify this behaviour
    # here to:
    #   - ignore and overwrite
    #   - set new keys, create new record
    #   - keep record and recompute only missing values
    #   - keep record and some specified set of values
    #   - allow user to have some choice

    # TODO: implement this function with correct looping/pagination. option sort key
    records = get_records(key_partition)
    # TODO: refactor as single function
    if records:
        records_current = [r for r in records if r['in_use']]
        assert len(records_current) == 1
        [record_current] = records_current
        file_number = record_current['file_number'] += 1
    else:
        file_number = 1
    sort_key = f'{filename}_{file_number}'

    # Compute required jobs
    # NOTE(SW): given the above behaviour of creating new records if previous exists, we will
    # always need to run all jobs. Hardcode here for now.
    tasks = ['checksum', 'validate_file_type', 'indexing']

    # Construct command for Batch job
    command = textwrap.dedent(f'''
        validate_file.py \
          --partition_key {partition_key} \
          --sort_key {sort_key} \
          --tasks {tasks}
    ''')

    # Submit Batch job
    job_name = f'agha_validation__{key_partition}__{key_sort}'
    response = client.submit_job(
        jobName=job_name,
        jobQueue=aws.BATCH_QUEUE,
        jobDefinition=JOB_DEFINITION_NAME,
        containerOverrides={'memory': 4000, 'command': ['bash', '-o', 'pipefail', '-c', command]},
    )


def get_s3_object_metadata(bucket, prefix):
    results = list()
    response = S3_CLIENT.list_objects_v2(
        Bucket='umccr-temp-dev',
        Prefix=prefix
    )
    if not (object_mdata := response['Contents']):
        message = f'could not retrieve files from S3 at s3://{bucket}{prefix}'
        log_and_store_message(message, level='critical')
        notify_and_exit(data)
    else:
        results.extend(object_mdata)
    while response['IsTruncated']:
        token = response['NextContinuationToken']
        response = S3_CLIENT.list_objects_v2(
            Bucket='umccr-temp-dev',
            Prefix=prefix,
            ContinuationToken=token
        )
        results.extend(object_mdata)
    return results


def list_s3_objects(bucket, prefix):
    object_metadata = get_s3_object_metadata(bucket, prefix)
    return [os.path.basename(md.get('Key')) for md in object_metadata]


def get_listing(prefix: str):
    # get the S3 object listing for the prefix
    files = list()
    file_batch = S3_CLIENT.list_objects_v2(
        Bucket=STAGING_BUCKET,
        Prefix=prefix
    )
    if file_batch.get('Contents'):
        files.extend(extract_filenames(file_batch['Contents']))
    while file_batch['IsTruncated']:
        token = file_batch['NextContinuationToken']
        file_batch = S3_CLIENT.list_objects_v2(
            Bucket=STAGING_BUCKET,
            Prefix=prefix,
            ContinuationToken=token
        )
        files.extend(extract_filenames(file_batch['Contents']))
    return files


def extract_filenames(listing: list):
    filenames = list()
    for item in listing:
        filenames.append(os.path.basename(item['Key']))
    return filenames


def log_and_store_file_message(message_text, files):
    # Notification only gets summary message; Lambda log gets both summary and full
    message_summary = f'{message_text}: {len(files)}'
    log_and_store_message(message_summary)
    if files:
        files_str = '\r\t'.join(files)
        LOGGER.info(f'{message_text}:\r\t{files_str})'


def log_and_store_message(message, level='info'):
    LOGGER.log(level, message)
    # Prefix message with 'ERROR' for display in notifications
    if level in {'error', 'critical'}:
        message = f'ERROR: {message}'
    MESSAGE_STORE.append(message)


def notify_and_exit(data):
    shared.send_notifications(
        MESSAGE_STORE,
        EMAIL_SUBJECT,
        data.submitter_name,
        data.submitter_email,
        data.submission_prefix,
    )
    sys.exit(1)
