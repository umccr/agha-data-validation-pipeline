#!/usr/bin/env python3
import os

from aws_cdk import core

from stacks.agha_stack import AghaStack

# Retrieve AWS details from currently active AWS profile/credentials
aws_env = {
    'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
    'region': os.environ.get('CDK_DEFAULT_REGION')
}

# Construct full set of properties for stack
stack_props = {
    'namespace': 'agha-gdr-batch-dynamodb',
    'bucket_name': {
        'staging_bucket': 'agha-gdr-staging-dev',
        'results_bucket': 'agha-gdr-results-dev',
        'store_bucket': 'agha-gdr-store-dev'
    },
    'dynamodb_table': {
        "staging-bucket": 'agha-gdr-staging-bucket',
        "staging-bucket-archive": 'agha-gdr-staging-bucket-archive',
        "result-bucket": 'agha-gdr-result-bucket',
        "result-bucket-archive": 'agha-gdr-result-bucket-archive',
        "store-bucket": 'agha-gdr-store-bucket',
        "store-bucket-archive": 'agha-gdr-store-bucket-archive',
        "e-tag": 'agha-gdr-e-tag'
    },
    'autorun_validation_jobs': 'no',
    'notification': {
        'slack_notify': 'yes',
        'email_notify': 'yes',
        'slack_host': 'hooks.slack.com',
        'slack_channel': '#agha-gdr',
        'manager_email': 'sarah.casauria@mcri.edu.au',
        'sender_email': 'services@umccr.org'
    },
    'batch_environment' : {
        'job_definition_name': 'agha-gdr-input-validation',
        'batch_queue_name': 'agha-gdr-job-queue',
        'vpc_id':'vpc-6ceacc0b',
        'security_group_id' : 'sg-087b969ecd89cf993',
        'container_image': '843407916570.dkr.ecr.ap-southeast-2.amazonaws.com/agha-gdr-file-validation-dev'
    }
}
### TODO IMPORTANT:  bucket_name, sec_group, vpc, cont_image is based on dev account NOT agha ccount


# Initialise stack
app = core.App(
    context=stack_props
)

AghaStack(
    app,
    stack_props['namespace'],
    tags={
        'Stack': stack_props['namespace'],
        'Creator': f"cdk-{stack_props['namespace']}",
    },
    env=aws_env
)

app.synth()
