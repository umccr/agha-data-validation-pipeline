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

# Misc
FIELDS_DISALLOWED = {'partition_key', 'sort_key'}


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(util.get_context_info(context))}')

    # Validate event data
    validate_event_data(event)

    # Discovery results filepaths on S3 and then download file contents
    results_fps = collect_result_filepaths(event)
    results_data = get_results_data(results_fps, event['bucket_name'])

    # If the user provides specific update fields, check these exist in result data
    if event['fields'] != 'all':
        check_update_fields(results_data, event)

    # Determine how to import data into the database, then execute
    results_update, results_create = determine_update_type(
        results_data,
        event['fields'],
        event['strict_mode']
    )
    run_record_update(results_update, event['fields'])
    run_record_create(results_create)


def validate_event_data(event):
    # Check for unknown argments
    args_known = {
        'bucket_name',
        'results_dir',
        'results_fps',
        'fields',
        'strict_mode',
    }
    args_unknown = [arg for arg in event if arg not in args_known]
    if args_unknown:
        plurality = 'arguments' if len(args_unknown) > 1 else 'argument'
        args_unknown_str = '\r\t'.join(args_unknown)
        LOGGER.critical(f'got {len(args_unknown)} unknown arguments:\r\t{args_unknown_str}')
        sys.exit(1)

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

    # Prohibit some update fields, otherwise if not specified set default
    if fields := event.get('fields'):
        if not isinstance(fields, list):
            LOGGER.critical(f'the \'fields\' argument must be a list')
            sys.exit(1)
        fields_disallowed_found = [f for f in fields if f in FIELDS_DISALLOWED]
        if fields_disallowed_found:
            plurality = 'fields' if len(fields_disallowed_found) > 1 else 'field'
            fields_str = '\r\t'.join(fields_disallowed_found)
            msg_base = f'the \'fields\' argument contained disallowed {plurality}'
            LOGGER.critical(f'{msg_base}:\r\t{fields_disallowed_found}')
            sys.exit(1)
    else:
        event['fields'] = 'all'

    # Process strict mode, must be bool
    if 'strict_mode' in event:
        strict_mode_str = event['strict_mode']
        if strict_mode_str.lower() == 'true':
            event['strict_mode'] = True
        elif strict_mode_str.lower() == 'false':
            event['strict_mode'] = False
        else:
            msg = f'expected \'True\' or \'False\' for strict_mode but got {strict_mode_str}'
            LOGGER.critical(msg)
            sys.exit(1)
    else:
        event['strict_mode'] = True


def collect_result_filepaths(event):
    bucket_name = event['bucket_name']
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
            file_list_str = '\r\t'.join(results_fp_missing)
            LOGGER.critical(f'did not find {len(results_fp_missing)} provided {plurality}:\r\t{file_list_str}')
            sys.exit(1)
    return results_fps


def get_results_data(results_fps, bucket_name):
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
        file_list_str = '\r\t'.join(results_data_missing)
        LOGGER.critical(f'could not retrieve data for {len(results_data_missing)} {plurality}:\r\t{file_list_str}')
        sys.exit(1)
    return results_data


def check_update_fields(results_data, event):
    records_fields_missing = dict()
    for fp, data in results_data.items():
        fields_list = get_field_list(event['fields'], data)
        fields = set(fields_list)
        fields_missing = fields.difference(data['validation_results'])
        if fields_missing:
            assert fp not in records_fields_missing
            records_fields_missing[fp] = fields_missing
    if records_fields_missing:
        plurality = 'records' if len(records_fields_missing) > 1 else 'record'
        fp_fields_strs = [f'{fp}: {", ".join(fields)}' for fp, fields in records_fields_missing.items()]
        fp_fields_str = '\r\t'.join(fp_fields_strs)
        msg_base = f'found {len(records_fields_missing)} {plurality} missing update fields'
        LOGGER.critical(f'{msg_base}:\r\t{fp_fields_str}')
        sys.exit(1)


def determine_update_type(results_data, fields, strict_mode):
    records_no_keys = list()
    records_fields_missing = dict()
    records_update = dict()
    records_create = dict()
    for fp, data in results_data.items():
        # Get partition and sort key
        if (record_keys := get_record_keys(data)) is False:
            # No available keys
            records_no_keys.append(fp)
            continue
        # Determine whether record exists
        if (record := get_record(record_keys)):
            # Ensure that update fields exist in target record
            fields_list = get_field_list(fields, data)
            fields = set(fields_list)
            fields_missing = fields.difference(record.keys())
            if fields_missing:
                assert fp not in records_fields_missing
                records_fields_missing[fp] = fields_missing
            # Set record for update
            assert fp not in records_update
            records_update[fp] = data
        else:
            # Set record for create
            assert fp not in records_create
            records_create[fp] = data

    # Check and log issues
    if records_no_keys:
        plurality = 'records' if len(records_no_keys) > 1 else 'record'
        record_str = '\r\t'.join(records_no_keys)
        msg_base = f'found {len(records_no_keys)} {plurality} missing partition and sort keys'
        LOGGER.critical(f'{msg_base}:\r\t{record_str}')
        sys.exit(1)

    if records_fields_missing:
        plurality = 'records' if len(records_fields_missing) > 1 else 'record'
        fp_fields_strs = [f'{fp}: {", ".join(fields)}' for fp, fields in records_fields_missing.items()]
        fp_fields_str = '\r\t'.join(fp_fields_strs)
        msg_base = f'found {len(records_fields_missing)} {plurality} missing update fields'
        if strict_mode:
            LOGGER.critical(f'{msg_base}:\r\t{fp_fields_str}')
            sys.exit(1)
        else:
            LOGGER.warning(f'{msg_base}:\r\t{fp_fields_str}')

    if records_create:
        plurality = 'records' if len(records_create) > 1 else 'record'
        record_str = '\r\t'.join(records_create)
        msg_base = f'could not find existing entries for {len(records_create)} {plurality}'
        if strict_mode:
            LOGGER.critical(f'{msg_base}:\r\t{record_str}')
            sys.exit(1)
        else:
            LOGGER.warning(f'{msg_base}:\r\t{record_str}')
    return records_update, records_create


def run_record_update(results_update, fields):
    for fp, data in results_update.items():
        # Marshall data to update
        update_expr_items = list()
        attr_values = dict()
        fields_list = get_field_list(fields, data)
        for i, k in enumerate(fields_list):
            update_expr_key = f':{i}'
            assert update_expr_key not in attr_values
            update_expr_items.append(f'{k} = {update_expr_key}')
            attr_values[update_expr_key] = data['validation_results'][k]
        update_expr_items_str = ', '.join(update_expr_items)
        update_expr = f'SET {update_expr_items_str}'
        # Update record
        results_str = '\r\t'.join(f'{field}: {data["validation_results"][field]}' for field in fields_list)
        LOGGER.info(f'updating record for {fp} with:\r\t{results_str}')
        record_keys = get_record_keys(data)
        RESOURCE_DYNAMODB.update_item(
            Key={
                'partition_key': record_keys['partition'],
                'sort_key': record_keys['sort'],
            },
            UpdateExpression=update_expr,
            ExpressionAttributeValues=attr_values,
        )


def run_record_create(results_create):
    for fp, data in results_create.items():
        # NOTE: An incomplete record may still be created as we're not checking presence of all
        # fields. We should have a shared data structure that represents a record and use that to
        # ensure we have all fields, though this doesn't exist yet.
        # Get activate record for partition key, if it exists
        records_existing = get_records(data['existing_record']['partition_key'])
        records_active = [r for r in records_existing if r['active']]
        # Prepare new record
        record = data['existing_record'].copy()
        for k, v in data['validation_results'].items():
            record[k] = v
        record['active'] = True
        # Create record
        results_str = '\r\t'.join(f'{k}: {v}' for k, v in record.items())
        LOGGER.info(f'creating record for {fp} with:\r\t{results_str}')
        RESOURCE_DYNAMODB.put_item(Item=record)
        # Set other records to inactive
        for record_active in records_active:
            RESOURCE_DYNAMODB.update_item(
                Key={
                    'partition_key': record_active['partition_key'],
                    'sort_key': record_active['sort_key'],
                },
                UpdateExpression='SET active = :a',
                ExpressionAttributeValues={':a': False},
            )


def get_record_keys(data):
    if 'existing_record' not in data:
        return False
    else:
        record_existing = data.get('existing_record')
    return {
        'partition': record_existing.get('partition_key'),
        'sort': int(record_existing.get('sort_key')),
    }


def get_record(keys):
    LOGGER.info(f'using partition key {keys["partition"]} and sort key {keys["sort"]}')
    response = RESOURCE_DYNAMODB.get_item(
        Key={'partition_key': keys['partition'], 'sort_key': keys['sort']},
    )
    return response.get('Item')


# NOTE: this function is repeated from file_processor/shared.py
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


def get_field_list(user_fields, data):
    if user_fields == 'all':
        return list(data['validation_results'].keys())
    else:
        return user_fields
