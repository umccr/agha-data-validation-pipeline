"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/manifest_processor

Run python test command:
cmd: python -m unittest tests.test_file_manifest_processor.ManifestProcessorUnitTestCase

"""

import os
import unittest
from unittest import mock

from manifest_processor import handler


def create_manifest_record_payload():
    # return {
    #     "Records": [
    #         {
    #             "eventVersion": "2.1",
    #             "eventSource": "aws:s3",
    #             "awsRegion": "ap-southeast-2",
    #             "eventTime": "2021-12-15T23:04:31.056Z",
    #             "eventName": "ObjectCreated:Copy",
    #             "userIdentity": {
    #                 "principalId": "XXX"
    #             },
    #             "requestParameters": {
    #                 "sourceIPAddress": "111.220.182.3"
    #             },
    #             "responseElements": {
    #                 "x-amz-request-id": "",
    #                 "x-amz-id-2": ""
    #             },
    #             "s3": {
    #                 "s3SchemaVersion": "1.0",
    #                 "configurationId": "",
    #                 "bucket": {
    #                     "name": "a bucket",
    #                     "ownerIdentity": {
    #                         "principalId": ""
    #                     },
    #                     "arn": "somebucket"
    #                 },
    #                 "object": {
    #                     "key": "XXX",
    #                     "size": 16292,
    #                     "eTag": "abcde",
    #                     "sequencer": "asdfg"
    #                 }
    #             }
    #         }
    #     ]
    # }
    return {
        "bucket_name": "agha-gdr-staging-2.0",
        "manifest_fp": f"NMD/2021-06-16/manifest.txt",
        "email_report_to": "william.intan@unimelb.edu.au",
        "skip_send_notification": True
    }


class ManifestProcessorUnitTestCase(unittest.TestCase):

    def test_manifest_processor(self):
        event_payload = create_manifest_record_payload()

        handler(event_payload, {})


if __name__ == '__main__':
    unittest.main()
