# Add util directory to python path
import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "layers", "util")
sys.path.append(SOURCE_PATH)


# Setting up environment variable
os.environ["STAGING_BUCKET"] = "agha-gdr-staging-2.0"
os.environ["STORE_BUCKET"] = "agha-gdr-store-2.0"
