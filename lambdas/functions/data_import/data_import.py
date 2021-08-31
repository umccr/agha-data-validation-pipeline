#!/usr/bin/env python3
import json
import logging
import sys


import boto3


import util


# Setup logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Get environment variables
DYNAMODB_TABLE = util.get_environment_variable('DYNAMODB_TABLE')
SLACK_NOTIFY = util.get_environment_variable('SLACK_NOTIFY')
EMAIL_NOTIFY = util.get_environment_variable('EMAIL_NOTIFY')
SLACK_HOST = util.get_environment_variable('SLACK_HOST')
SLACK_CHANNEL = util.get_environment_variable('SLACK_CHANNEL')
MANAGER_EMAIL = util.get_environment_variable('MANAGER_EMAIL')
SENDER_EMAIL = util.get_environment_variable('SENDER_EMAIL')

# Get AWS clients, resources
CLIENT_S3 = util.get_client('s3')
CLIENT_SSM = util.get_client('ssm')
RESOURCE_DYNAMODB = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(util.get_context_info(context))}')

    # Parse event data
    validate_event_data(event)
    bucket_name = event.get('bucket_name')
    if 'results_dir' in event:
        results_fps = list()
        results_dir = event['results_dir']
        file_metadata = util.get_s3_object_metadata(bucket_name, results_dir, CLIENT_S3)
        for mdata in file_metadata:
            file_key = mdata.get('Key')
            if not file_key.endswith('.json'):
                continue
            results_fps.append(file_key)
        if not results_fps:
            results_dir_full = f's3://{bucket_name}/{results_dir}'
            LOGGER.critical(f'did not find any JSON results files in \'{results_dir_full}\'')
            sys.exit(1)
    else:
        results_fps = event['results_fps']
        # Check that each provided results file exists. Files discovered when provided results_dir
        # must implicitly exist.
        results_fp_missing = list()
        for results_fp in results_fps:
            if util.get_s3_object_metadata(bucket_name, results_fp, CLIENT_S3):
                continue
            results_fp_missing.append(results_fp)
        if results_fp_missing:
            plurality = 'files' if len(results_fp_missing) > 1 else 'file'
            file_list_str = '\t\r'.join(results_fp_missing)
            LOGGER.critical(f'did not find {len(results_fp_missing)} provided {plurality}:\r\t{file_list_str}')
            sys.exit(1)

    # Pull result data
    results_data = dict()
    for results_fp in results_fps:
        response = CLIENT_S3.get_object(
            Bucket=bucket_name,
            Key=results_fp,
        )
        assert results_fp not in results_data
        # NOTE(SW): will need to handle exceptions here to cleanly log all cases below
        data = response['Body'].read()
        results_data[results_fp] = json.loads(data)
    # Check we have data for each file
    results_data_missing = [fp for fp, data in results_data.items() if not data]
    if results_data_missing:
        plurality = 'files' if len(results_data_missing) > 1 else 'file'
        file_list_str = '\t\r'.join(results_data_missing)
        LOGGER.critical(f'could not retrieve data for {len(results_fp_missing)} {plurality}:\r\t{file_list_str}')
        sys.exit(1)

    for fp, data in results_data.items():
        LOGGER.info(f'processing results file \'{fp}\'')
        record_keys = get_record_keys(data)
        if (record_keys['partition'] and record_keys['sort']) and record_exists(record_keys):
            # Update existing record

            # NOTE(SW): we're updating all fields available, may want to control in some regard

            results_str = '\r\t'.join(f'{k}: {v}' for k, v in data['validation_results'].items())
            LOGGER.info(f'found existing record for {fp}, updating with:\r\t{results_str}')
            # Get update expression string and attribute values
            update_expr_items = list()
            attr_values = dict()
            for i, k in enumerate(data['validation_results']):
                update_expr_key = f':{i}'
                assert update_expr_key not in attr_values
                update_expr_items.append(f'{k} = {update_expr_key}')
                attr_values[update_expr_key] = data['validation_results'][k]
            update_expr_items_str = ', '.join(update_expr_items)
            update_expr = f'SET {update_expr_items_str}'
            # Update record
            RESOURCE_DYNAMODB.update_item(
                Key={
                    'partition_key': record_keys['partition'],
                    'sort_key': record_keys['sort'],
                },
                UpdateExpression=update_expr,
                ExpressionAttributeValues=attr_values,
            )
        else:
            # Create new record

            # NOTE(SW): we want to apply some logic to specific fields here

            # Construct complete record from results file, copying for convenience
            record = data['existing_record'].copy()
            for k, v in data['validation_results'].items():
                record[k] = v
            # Manual set some fields
            record['active'] = True
            # Log and create record
            results_str = '\r\t'.join(f'{k}: {v}' for k, v in record.items())
            LOGGER.info(f'creating record for {fp} with:\r\t{results_str}')
            RESOURCE_DYNAMODB.put_item(Item=record)


def validate_event_data(event):
    # Require bucket name
    if 'bucket_name' not in event:
        LOGGER.critical('event data did not contain \'bucket_name\' value')
        sys.exit(1)

    # Accept either results directory for results filepaths
    if 'results_dir' not in event and 'results_fps' not in event:
        LOGGER.critical('either \'results_dir\' or \'results_fps\' must be provided')
        sys.exit(1)

    # All results filepaths *must* be json
    results_fps_not_json = [fp for fp in event.get('results_fps', list()) if not fp.endswith('json')]
    if results_fps_not_json:
        plurality = 'files' if len(results_fps_not_json) > 1 else 'file'
        files_str = '\r\t'.join(results_fps_not_json)
        msg_base = f'input files must be json, found {len(results_fps_not_json)} non-json {plurality}'
        LOGGER.critical(f'{msg_base}:\r\t{files_str}')
        sys.exit(1)


def get_record_keys(data):
    if 'existing_record' not in data:
        return False
    else:
        record_existing = data.get('existing_record')
    return {
        'partition': record_existing.get('partition_key'),
        'sort': int(record_existing.get('sort_key')),
    }


def record_exists(keys):
    LOGGER.info(f'using partition key {keys["partition"]} and sort key {keys["sort"]}')
    response = RESOURCE_DYNAMODB.get_item(
        Key={'partition_key': keys['partition'], 'sort_key': keys['sort']},
    )
    return 'Item' in response
