#!/usr/bin/env python3
import os


from aws_cdk import core


from stacks.agha_stack import AghaStack


# Set config
namespace = 'agha-gdr-batch-dynamodb'
dynamodb_table = 'agha-gdr'
batch_queue_name = 'agha-gdr-job-queue'
job_definition_name = 'agha-gdr-input-validation'
staging_bucket = 'agha-gdr-staging'
results_bucket = 'agha-gdr-results'
store_bucket = 'agha-gdr-store'
container_image = 'scwatts/agha-validation-pipeline:0.0.1'
email_notify = 'no'
slack_notify = 'no'
manager_email = 'stephen.watts@umccr.org'
sender_email = 'services@umccr.org'
slack_host = 'hooks.slack.com'
slack_channel = '#arteria-dev'
# Retrieve AWS details from currently active AWS profile/credentials
aws_env = {
    'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
    'region': os.environ.get('CDK_DEFAULT_REGION')
}
# Construct full set of properties for stack
agha_props = {
    'namespace': namespace,
    'container_image': container_image,
    'staging_bucket': staging_bucket,
    'results_bucket': results_bucket,
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
