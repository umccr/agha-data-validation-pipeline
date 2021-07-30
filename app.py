#!/usr/bin/env python3
import os
from aws_cdk import core
import boto3

from stacks.agha_stack import AghaStack


# Notification settings. Enabled: 'yes'; Disabled: 'no' (or any string value not 'yes')
email_notify = 'no'
slack_notify = 'no'

# Retrieve parameters from SSM
def get_ssm_parameter_value(name):
    return ssm_client.get_parameter(Name=name)['Parameter']['Value']

ssm_client = boto3.client('ssm')
staging_bucket = get_ssm_parameter_value('/cdk/agha/staging_bucket')
store_bucket = get_ssm_parameter_value('/cdk/agha/store_bucket')
slack_host = get_ssm_parameter_value('/slack/webhook/host')
slack_channel = get_ssm_parameter_value('/cdk/agha/slack_channel')
manager_email = get_ssm_parameter_value('/cdk/agha/manager_email')
sender_email = get_ssm_parameter_value('/cdk/agha/sender_email')

# Retrieve AWS details from currently active AWS profile/credentials
aws_env = {
    'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
    'region': os.environ.get('CDK_DEFAULT_REGION')
}

agha_props = {
    'namespace': 'agha',
    # NOTE(SW): this will be a Docker image containing all necessary software tools
    'container_image': 'NOT_YET_DEFINED',
    'staging_bucket': staging_bucket,
    'store_bucket': store_bucket,
    'slack_notify': slack_notify,
    'email_notify': email_notify,
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
