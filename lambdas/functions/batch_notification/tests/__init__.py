# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)

os.environ["DYNAMODB_RESULT_TABLE_NAME"] = "agha-gdr-result-bucket"
os.environ["DYNAMODB_STAGING_TABLE_NAME"] = "agha-gdr-staging-bucket"
os.environ["DYNAMODB_STORE_TABLE_NAME"] = "agha-gdr-store-bucket"
os.environ["STAGING_BUCKET"] = "agha-gdr-staging-2.0"
os.environ["RESULTS_BUCKET"] = "agha-gdr-results-2.0"
os.environ["STORE_BUCKET"] = "agha-gdr-store-2.0"
os.environ[
    "REPORT_LAMBDA_ARN"
] = "arn:aws:lambda:ap-southeast-2:602836945884:function:agha-gdr-validation-pipeline-report"
