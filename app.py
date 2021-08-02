#!/usr/bin/env python3
import os


from aws_cdk import core
import boto3


from stacks.agha_stack import AghaStack


CLIENT_SSM = boto3.client('ssm')


def create_stack():
    # Set variables that should only change at (re)deploy and are not shared with other stacks
    namespace = 'agha-validation'
    dynamodb_table = 'agha-validation-data'
    batch_queue_name = 'agha-validation-job-queue'
    job_definition_name = 'agha-input-validation'
    # Collect configuration from SSM parameter store. Variables are considered to be appropriate
    # for storage in SSM where they (1) might want to be changed between deploys, or (2) if they
    # are shared between multiple stacks.
    staging_bucket = get_ssm_parameter_value('/cdk/agha/staging_bucket')
    store_bucket = get_ssm_parameter_value('/cdk/agha/store_bucket')
    email_notify = get_ssm_parameter_value('/cdk/agha/email_notify')
    slack_notify = get_ssm_parameter_value('/cdk/agha/slack_notify')
    slack_host = get_ssm_parameter_value('/slack/webhook/host')
    slack_channel = get_ssm_parameter_value('/cdk/agha/slack_channel')
    manager_email = get_ssm_parameter_value('/cdk/agha/manager_email')
    sender_email = get_ssm_parameter_value('/cdk/agha/sender_email')
    # Retrieve AWS details from currently active AWS profile/credentials
    aws_env = {
        'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
        'region': os.environ.get('CDK_DEFAULT_REGION')
    }
    # Construct full set of properties for stack
    agha_props = {
        'namespace': namespace,
        # NOTE(SW): this will be a Docker image containing all necessary software tools
        'container_image': 'NOT_YET_DEFINED',
        'staging_bucket': staging_bucket,
        'store_bucket': store_bucket,
        'dynamodb_table': dynamodb_table,
        'job_definition_name': job_definition_name,
        'batch_queue_name': batch_queue_name,
        'slack_notify': slack_notify,
        'email_notify': email_notify,
        'slack_host': slack_host,
        'slack_channel': slack_channel,
        'manager_email': manager_email,
        'sender_email': sender_email,
    }
    # Create stack as configured
    app = core.App()
    AghaStack(
        app,
        agha_props['namespace'],
        agha_props,
        env=aws_env
    )
    app.synth()


def get_ssm_parameter_value(name):
    return CLIENT_SSM.get_parameter(Name=name)['Parameter']['Value']


# Entry point for stack creation
create_stack()
