# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["DYNAMODB_ARCHIVE_RESULT_TABLE_NAME"] = "agha-gdr-result-bucket-archive"
os.environ["DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"] = "agha-gdr-staging-bucket-archive"
os.environ["DYNAMODB_ARCHIVE_STORE_TABLE_NAME"] = "agha-gdr-store-bucket-archive"
os.environ["DYNAMODB_ETAG_TABLE_NAME"] = "agha-gdr-e-tag"
os.environ["DYNAMODB_RESULT_TABLE_NAME"] = "agha-gdr-result-bucket"
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = "agha-gdr-staging-bucket"
os.environ["DYNAMODB_STORE_TABLE_NAME"] = "agha-gdr-store-bucket"
os.environ["STAGING_BUCKET"] = "agha-staging-dev"
os.environ["RESULT_BUCKET"] = "agha-results-dev"
os.environ["STORE_BUCKET"] = "agha-store-dev"
os.environ[
    "BATCH_NOTIFICATION_LAMBDA"
] = "arn:aws:lambda:ap-southeast-2:602836945884:function:agha-gdr-validation-pipeline-batch-notification"
