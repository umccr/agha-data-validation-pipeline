# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "layers", "util"
)
sys.path.append(SOURCE_PATH)

os.environ["DYNAMODB_STORE_TABLE_NAME"] = 'agha-gdr-store-bucket'
os.environ["RESULTS_BUCKET"] = 'agha-gdr-results-2.0'
