# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["STAGING_BUCKET"] = 'STAGING_BUCKET'
os.environ["MANIFEST_PROCESSOR_LAMBDA_ARN"] = 'MANIFEST_PROCESSOR_LAMBDA'
os.environ["FOLDER_LOCK_LAMBDA_ARN"] = 'FOLDER_LOCK_LAMBDA'
os.environ["S3_RECORDER_LAMBDA_ARN"] = 'S3_RECORDER_LAMBDA'