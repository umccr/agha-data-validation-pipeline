# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)

os.environ["DYNAMODB_STORE_TABLE_NAME"] = "agha-gdr-store-bucket"
os.environ["STORE_BUCKET"] = "agha-gdr-store-2.0"
os.environ["S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME"] = "agha-gdr-s3-data-sharing"
os.environ[
    "S3_DATA_SHARING_BATCH_QUEUE_NAME"
] = "arn:aws:batch:ap-southeast-2:602836945884:job-queue/agha-gdr-s3-data-sharing"
os.environ[
    "S3_DATA_SHARING_JOB_DEFINITION_ARN"
] = "arn:aws:batch:ap-southeast-2:602836945884:job-definition/agha-gdr-s3-data-sharing:2"
