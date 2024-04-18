"""
To run the testcase

Change directory
cmd from root directory: cd lambdas/functions/report

Run python test command:
cmd: python -m unittest tests.test_report.ReportUnitTestCase

"""

import os
import unittest
from unittest import mock

from report import handler


def create_report_payload():
    return {
        "report_type": "file_transfer_check",
        "payload": {"submission_prefix": "AC/20210923_151606/"},
    }


class ReportUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = "local"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    def test_manifest_processor(self):
        event_payload = create_report_payload()

        res = handler(event_payload, {})

        print(res)


if __name__ == "__main__":
    unittest.main()
