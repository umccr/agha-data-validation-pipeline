"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/notification

Run python test command:
cmd: python -m unittest tests.test_notification.NotificationTestCase

"""

import os
import unittest
from unittest import mock

from notification import handler

def create_manifest_record_payload():
    return {
    "messages":[
        ""
    ],
    "subject":"[AGHA service] Submission received",
    "submitter_info":{
        "name":"",
        "email":"",
        "submission_prefix":""
    }
}



class NotificationTestCase(unittest.TestCase):

    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = 'local'
        os.environ["AWS_SECRET_ACCESS_KEY"] = 'local'
        os.environ["AWS_DEFAULT_REGION"] = 'ap-southeast-2'
        os.environ['AWS_ENDPOINT'] = 'http://localhost:4566'

    def test_notification(self):
        event_payload = create_manifest_record_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()