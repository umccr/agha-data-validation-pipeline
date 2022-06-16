"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/gdr_s3_data_sharing

Run python test command:
cmd: python -m unittest tests.test_gdr_s3_data_sharing.GDRS3DataSharingUnitTestCase

"""

import os
import unittest
from unittest import mock

from gdr_s3_data_sharing import handler


class GDRS3DataSharingUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = 'local'
        os.environ["AWS_SECRET_ACCESS_KEY"] = 'local'
        os.environ["AWS_DEFAULT_REGION"] = 'ap-southeast-2'
        os.environ['AWS_ENDPOINT'] = 'http://localhost:4566'

    def test_gdr_s3_data_sharing(self):
        event_payload = make_mock_data()
        res = handler(event_payload, {})

        print(res)


if __name__ == '__main__':
    unittest.main()


def make_mock_data():
    return {
        "destination_s3_arn": '',
        "destination_s3_key_prefix": '',
        "source_s3_key_list": ['ABCDE/121212/filename.fastq.gz']
    }
