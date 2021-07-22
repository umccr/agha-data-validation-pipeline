#!/usr/bin/env python3
import os
from aws_cdk import core
import boto3

from stacks.agha_stack import AghaStack

ssm_client = boto3.client('ssm')

def get_ssm_parameter_value(name):
    return ssm_client.get_parameter(Name=name)['Parameter']['Value']

staging_bucket = get_ssm_parameter_value(name='/cdk/agha/staging_bucket')
store_bucket = get_ssm_parameter_value(name='/cdk/agha/store_bucket')
slack_host = get_ssm_parameter_value(name='/slack/webhook/host')
slack_channel = get_ssm_parameter_value(name='/slack/channel')
manager_email = get_ssm_parameter_value(name='/cdk/agha/manager_email')
sender_email = get_ssm_parameter_value(name='/cdk/agha/sender_email')

# retrieve AWS details from currently active AWS profile/credentials
aws_env = {
    'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
    'region': os.environ.get('CDK_DEFAULT_REGION')
}

agha_props = {
    'namespace': 'agha',
    'staging_bucket': staging_bucket,
    'store_bucket': store_bucket,
    'slack_host': slack_host,
    'slack_channel': slack_channel,
    'manager_email': manager_email,
    'sender_email': sender_email,
}

app = core.App()

AghaStack(
    app,
    agha_props['namespace'],
    agha_props,
    env=aws_env
)

app.synth()
