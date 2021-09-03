#!/usr/bin/env python3
import argparse
import functools
import sys


import boto3
import boto3.dynamodb


RECORD_STATE_CHOICE = ['not_complete', 'not_fully_validated']


def get_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--database_name', required=True, type=str,
            help='DynamoDB database name to query')
    parser.add_argument('--submission_prefix', type=str,
            help='Limit query to given submission prefix')
    parser.add_argument('--active_only', action='store_true',
            help='Limit query to active records only')
    parser.add_argument('--record_type', choices=RECORD_STATE_CHOICE, required=True,
            help=f'Query for specific record state')
    return parser.parse_args()


def main():
    # Get command line arguments
    args = get_arguments()

    # Create Boto3 DynamoDB resource and make query
    dynamodb_resource = boto3.resource('dynamodb').Table(args.database_name)
    records = get_records(
        dynamodb_resource,
        args.record_type,
        submission_prefix=args.submission_prefix,
        active_only=args.active_only
    )

    # Output record info
    fields = [
        'partition_key',
        'sort_key',
        'active',
        'fully_validated',
        'valid_checksum',
        'valid_filetype'
    ]
    print(*fields, sep='\t')
    for record in records:
        data = [record[field] for field in fields]
        print(*data, sep='\t')


def get_records(dynamodb_resource, record_type, submission_prefix=None, active_only=None):
    # Conditionally construct query expression
    exprs = list()
    if record_type == 'not_complete':
        exprs.append(boto3.dynamodb.conditions.Attr('ts_validation_job').eq('na'))
    elif record_type == 'not_fully_validated':
        exprs.append(boto3.dynamodb.conditions.Attr('fully_validated').eq('no'))
    else:
        assert False
    if submission_prefix:
        exprs.append(boto3.dynamodb.conditions.Key('partition_key').begins_with(submission_prefix))
    if active_only is True:
        exprs.append(boto3.dynamodb.conditions.Attr('active').eq(True))
    elif active_only is not False:
        assert False
    expr_complete = functools.reduce(lambda acc, new: acc & new, exprs)
    # Retrieve records
    records = list()
    response = dynamodb_resource.scan(
        FilterExpression=expr_complete
    )
    if 'Items' not in response:
        message = f'error: could not any records using partition key ({submission_prefix})'
        print(message, fh=sys.stderr)
        sys.exit(1)
    else:
        records.extend(response.get('Items'))
    while last_result_key := response.get('LastEvaluatedKey'):
        response = dynamodb_resource.scan(
            FilterExpression=expr_complete,
            ExclusiveStartKey=last_result_key,
        )
        records.extend(response.get('Items'))
    return records


if __name__ == '__main__':
    main()
