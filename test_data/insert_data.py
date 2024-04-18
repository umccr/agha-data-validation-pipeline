import os
import sys

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "lambdas", "layers", "util")
sys.path.append(SOURCE_PATH)

from test_data import (
    DATA_RESULT,
    STATUS_RESULT,
    STORE_TYPE_FILE,
    STAGING_TYPE_FILE,
    STAGING_TYPE_MANIFEST,
    STORE_TYPE_MANIFEST,
    ETAG_RECORDS,
)
from util import dynamodb

DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = "agha-gdr-result-bucket-archive"
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = "agha-gdr-staging-bucket-archive"
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = "agha-gdr-store-bucket-archive"
DYNAMODB_ETAG_TABLE_NAME = "agha-gdr-e-tag"
DYNAMODB_RESULT_TABLE_NAME = "agha-gdr-result-bucket"
DYNAMODB_STAGING_TABLE_NAME = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE_NAME = "agha-gdr-store-bucket"


def insert_data():

    # The key to use local dynamodb
    os.environ["AWS_ACCESS_KEY_ID"] = "local"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
    os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
    os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    # Insert to store table
    store_table_list = STORE_TYPE_MANIFEST + STORE_TYPE_FILE
    dynamodb.batch_write_objects(
        table_name=DYNAMODB_STORE_TABLE_NAME, object_list=store_table_list
    )
    dynamodb.batch_write_objects_archive(
        table_name=DYNAMODB_ARCHIVE_STORE_TABLE_NAME,
        object_list=store_table_list,
        archive_log="Create/Update",
    )

    # Insert to Staging table
    staging_table_list = STAGING_TYPE_FILE + STAGING_TYPE_MANIFEST
    dynamodb.batch_write_objects(
        table_name=DYNAMODB_STAGING_TABLE_NAME, object_list=staging_table_list
    )
    dynamodb.batch_write_objects_archive(
        table_name=DYNAMODB_ARCHIVE_STAGING_TABLE_NAME,
        object_list=staging_table_list,
        archive_log="Create/Update",
    )

    # Insert to Result table
    result_table_list = DATA_RESULT + STATUS_RESULT
    dynamodb.batch_write_objects(
        table_name=DYNAMODB_RESULT_TABLE_NAME, object_list=result_table_list
    )
    dynamodb.batch_write_objects_archive(
        table_name=DYNAMODB_ARCHIVE_RESULT_TABLE_NAME,
        object_list=result_table_list,
        archive_log="Create/Update",
    )

    # Insert to E-Tag
    dynamodb.batch_write_objects(
        table_name=DYNAMODB_ETAG_TABLE_NAME, object_list=ETAG_RECORDS
    )


if __name__ == "__main__":
    insert_data()
