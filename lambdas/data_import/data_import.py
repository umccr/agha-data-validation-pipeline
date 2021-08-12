#!/usr/bin/env python3
import json
import logging
import sys


import boto3


import shared


# Setup logging
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# Get environment variables
DYNAMODB_TABLE = shared.get_environment_variable('DYNAMODB_TABLE')
SLACK_NOTIFY = shared.get_environment_variable('SLACK_NOTIFY')
EMAIL_NOTIFY = shared.get_environment_variable('EMAIL_NOTIFY')
SLACK_HOST = shared.get_environment_variable('SLACK_HOST')
SLACK_CHANNEL = shared.get_environment_variable('SLACK_CHANNEL')
MANAGER_EMAIL = shared.get_environment_variable('MANAGER_EMAIL')
SENDER_EMAIL = shared.get_environment_variable('SENDER_EMAIL')

# Get AWS clients, resources
CLIENT_S3 = shared.get_client('s3')
CLIENT_SSM = shared.get_client('ssm')
RESOURCE_DYNAMODB = boto3.resource('dynamodb').Table(DYNAMODB_TABLE)

# Get SSM value
SLACK_WEBHOOK_ENDPOINT = shared.get_ssm_parameter(
    '/slack/webhook/endpoint',
    CLIENT_SSM,
    with_decryption=True
)


def handler(event, context):
    # Log invocation data
    LOGGER.info(f'event: {json.dumps(event)}')
    LOGGER.info(f'context: {json.dumps(shared.get_context_info(context))}')

    # Get result files
    if not (results_fps := event.get('results_fps')):
        LOGGER.critical('event data did not contain \'results_fps\' value')
        sys.exit(1)
    if not (bucket_name := event.get('bucket_name')):
        LOGGER.critical('event data did not contain \'bucket_name\' value')
        sys.exit(1)

    # Check that each provided results file exists
    results_fp_missing = list()
    for results_fp in results_fps:
        if shared.get_s3_object_metadata(bucket_name, results_fp, CLIENT_S3):
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
        # Get existing record
        partition_key = data['file_info']['partition_key']
        sort_key = int(data['file_info']['sort_key'])
        LOGGER.info(f'using partition key {partition_key} and sort key {sort_key} for {fp}')
        response = RESOURCE_DYNAMODB.get_item(
            Key={'partition_key': partition_key, 'sort_key': sort_key},
        )

        # Prepare data for logging
        if 'Item' in response:

            # NOTE(SW): we're updating all fields available, may want to control in some regard

            results_str = '\r\t'.join(f'{k}: {v}' for k, v in data['results'].items())
            LOGGER.info(f'found existing record for {fp}, updating with:\r\t{results_str}')
            # Get update expression string and attribute values
            update_expr_items = list()
            attr_values = dict()
            for i, k in enumerate(data['results']):
                update_expr_key = f':{i}'
                assert update_expr_key not in attr_values
                update_expr_items.append(f'{k} = {update_expr_key}')
                attr_values[update_expr_key] = data['results'][k]
            update_expr_items_str = ', '.join(update_expr_items)
            update_expr = f'SET {update_expr_items_str}'
            # Update record
            RESOURCE_DYNAMODB.update_item(
                Key={
                    'partition_key': partition_key,
                    'sort_key': sort_key,
                },
                UpdateExpression=update_expr,
                ExpressionAttributeValues=attr_values,
            )
        else:

            # NOTE(SW): we want to apply some logic to specific fields here. e.g. always setting
            # active to True

            # Construct complete record from results file, copying for convenience
            record = data['file_info'].copy()
            for k, v in data['results'].items():
                record[k] = v
            results_str = '\r\t'.join(f'{k}: {v}' for k, v in record.items())
            LOGGER.info(f'creating record for {fp} with:\r\t{results_str}')
            RESOURCE_DYNAMODB.put_item(Item=record)
