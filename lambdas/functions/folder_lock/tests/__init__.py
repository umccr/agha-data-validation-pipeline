# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["STAGING_BUCKET"] = "agha-gdr-staging-2.0"
os.environ["AWS_ACCOUNT_NUMBER"] = "123456789"
os.environ[
    "FOLDER_LOCK_EXCEPTION_ROLE_ID"
] = '["AIDACKCEVSQ6C2EXAMPLE", "AROADBQP57FF2AEXAMPLE"]'
