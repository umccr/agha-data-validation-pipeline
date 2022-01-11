# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["DYNAMODB_ARCHIVE_RESULT_TABLE_NAME"] = 'agha-gdr-result-bucket-archive'
os.environ["DYNAMODB_ARCHIVE_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket-archive'
os.environ["DYNAMODB_ARCHIVE_STORE_TABLE_NAME"] = 'agha-gdr-store-bucket-archive'
os.environ["DYNAMODB_ETAG_TABLE_NAME"] = 'agha-gdr-e-tag'
os.environ["DYNAMODB_RESULT_TABLE_NAME"] = 'agha-gdr-result-bucket'
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket'
os.environ["DYNAMODB_STORE_TABLE_NAME"] = 'agha-gdr-store-bucket'
os.environ["STAGING_BUCKET"] = 'agha-gdr-staging-2.0'
os.environ["RESULTS_BUCKET"] = 'agha-gdr-results-2.0'
os.environ["STORE_BUCKET"] = 'agha-gdr-store-2.0'
os.environ["S3_JOB_DEFINITION_ARN"] = 'arn:aws:batch:ap-southeast-2:843407916570:job-definition/agha-gdr-s3-manipulation:11'
os.environ["BATCH_QUEUE_NAME"] = '{"small": "agha-gdr-pipeline-job-queue-small", "medium": "agha-gdr-pipeline-job-queue-medium", "large": "agha-gdr-pipeline-job-queue-large", "xlarge": "agha-gdr-pipeline-job-queue-xlarge"}'
