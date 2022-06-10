"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/batch_notification

Run python test command:
cmd: python -m unittest tests.test_batch_notification.BatchNotificationUnitTestCase

"""

import os
import unittest
from unittest import mock

from batch_notification import handler, EventType


class BatchNotificationUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = 'local'
        os.environ["AWS_SECRET_ACCESS_KEY"] = 'local'
        os.environ["AWS_DEFAULT_REGION"] = 'ap-southeast-2'
        os.environ['AWS_ENDPOINT'] = 'http://localhost:4566'

    def test_batch_notification(self):
        event_payload = make_mock_data(event=EventType.STORE_FILE_UPLOAD, s3_key='MM_VCGS/20220526_203949/abcde.fastq')
        res = handler(event_payload, {})

        print(res)


if __name__ == '__main__':
    unittest.main()


def make_mock_data(event: EventType, s3_key: str):
    return {
        "event_type": event.value,
        "s3_key": s3_key
    }
