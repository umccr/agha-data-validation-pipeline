# Add util directory to python path
import os
import sys
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["FOLDER_LOCK_LAMBDA_ARN"] = 'arn:aws:lambda:ap-southeast-2:602836945884:function:agha_folder_lock_lambda'
os.environ["STAGING_BUCKET"] = 'agha-gdr-staging-2.0'
