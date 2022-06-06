# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)

os.environ["DYNAMODB_RESULT_TABLE_NAME"] = 'agha-gdr-result-bucket'
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = 'agha-gdr-staging-bucket'
os.environ["STAGING_BUCKET"] = 'agha-gdr-staging-2.0'
os.environ["RESULTS_BUCKET"] = 'agha-gdr-results-2.0'
os.environ["S3_JOB_DEFINITION_ARN"] = 'arn:aws:batch:ap-southeast-2:843407916570:job-definition/agha-gdr-s3-manipulation:11'
os.environ["JOB_DEFINITION_ARN"] = 'arn:aws:batch:ap-southeast-2:602836945884:job-definition/agha-gdr-validate-file:15'
