"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/cleanup_manager

Run python test command:
cmd: python -m unittest tests.test_cleanup_manager.CleanupManagerUnitTestCase

"""

import os
import unittest
from unittest import mock

from cleanup_manager import handler


def create_cleanup_manager_payload():
    return {"submission_directory": "ICCon/2022-03-21/"}


class CleanupManagerUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = "local"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    def test_cleanup_manager(self):
        event_payload = create_cleanup_manager_payload()

        res = handler(event_payload, {})

        print(res)


if __name__ == "__main__":
    unittest.main()
