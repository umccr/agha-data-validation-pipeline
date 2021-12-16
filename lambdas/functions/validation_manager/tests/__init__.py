# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["BATCH_QUEUE_NAME"] = 'agha-gdr-pipeline-job-queue'
os.environ["DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket-archive'
os.environ["DYNAMODB_RESULT_TABLE_NAME"] = 'agha-gdr-result-bucket'
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket'
os.environ["JOB_DEFINITION_ARN"] = 'arn:aws:batch:ap-southeast-2:843407916570:job-definition/agha-gdr-validate-file:1'
os.environ["NOTIFICATION_LAMBDA_ARN"] = 'arn:aws:lambda:ap-southeast-2:843407916570:function:agha-gdr-validation-pipeline-notification'
os.environ["RESULT_BUCKET"] = 'agha-results-dev'
os.environ["STORE_BUCKET"] = 'agha-store-dev'
os.environ["STAGING_BUCKET"] = 'agha-staging-dev'

