# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["STAGING_BUCKET"] = 'agha-gdr-staging-2.0'
os.environ["VALIDATION_MANAGER_LAMBDA_ARN"] = 'arn:aws:lambda:ap-southeast-2:602836945884:function:agha-gdr-validation-pipeline-validation-manager'
os.environ["NOTIFICATION_LAMBDA_ARN"] = 'arn:aws:lambda:ap-southeast-2:602836945884:function:agha-gdr-validation-pipeline-notification'
os.environ["DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket-archive'
os.environ["DYNAMODB_ETAG_TABLE_NAME"] = 'agha-gdr-e-tag'
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket'