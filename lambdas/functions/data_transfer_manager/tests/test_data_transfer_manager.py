"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/data_transfer_manager

Run python test command:
cmd: python -m unittest tests.test_data_transfer_manager.DataTransferManagerUnitTestCase

"""

import os
import unittest
from unittest import mock

from data_transfer_manager import handler


def create_manifest_record_payload():
    return {
        "flagship_code": "AC",
        "submission": "20210531_162251",
    }


class DataTransferManagerUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = "local"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    def test_manifest_processor(self):
        event_payload = create_manifest_record_payload()

        res = handler(event_payload, {})

        print(res)


if __name__ == "__main__":
    unittest.main()
