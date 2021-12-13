#!/usr/bin/env python3
import os

from aws_cdk import core

from stacks.codepipeline_stacks.codepipeline_stack import CodePipelineStack

# Retrieve AWS details from currently active AWS profile/credentials
aws_env = {
    'account': os.environ.get('CDK_DEFAULT_ACCOUNT'),
    'region': os.environ.get('CDK_DEFAULT_REGION')
}

# Construct full set of properties for stack
stack_props = {
    'namespace': 'agha-gdr-validation-pipeline',
    'aws_env': aws_env,
    'bucket_name': {
        'staging_bucket': 'agha-staging-dev',
        'results_bucket': 'agha-results-dev',
        'store_bucket': 'agha-store-dev'
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
        'manager_email': 'william.intan@unimelb.edu.au', # Change email to: sarah.casauria@mcri.edu.au
        'sender_email': 'services@umccr.org'
    },
    'batch_environment' : {
        'file_validation_job_definition_name': 'agha-gdr-input-validation',
        'batch_queue_name': 'agha-gdr-job-queue',
        'vpc_id':'vpc-6ceacc0b',
        'file_validation_ecr': {
            'name':'agha-gdr-validate-file',
            'tag':'0.0.1'
        },
        's3_job_definition_name':'agha-gdr-s3-manipulation',
    },
    'pipeline' : {
        'artifact_bucket_name': 'agha-validation-pipeline-artifact',
        'pipeline_name': 'agha-validation-build-pipeline',
        'repository_name': 'agha-data-validation-pipeline',
        'branch_name':'dev'
    }
}


### TODO: VERY IMPORTANT:  bucket_name, vpc, container_image, branch_name is based on dev account NOT agha account


# Initialise stack
app = core.App(
    context=stack_props
)

CodePipelineStack(
    app,
    "AGHAValidationCodePipeline",
    stack_name="agha-cdk-codepipeline",
    tags={
        "Stack": "agha-cdk-codepipeline",
        "Creator": "cdk-codepipeline"
    },
    env=aws_env
)

app.synth()
