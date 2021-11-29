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
    return {
        "Records": [
            {
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "awsRegion": "ap-southeast-2",
                "eventTime": "2021-11-29T05:13:49.487Z",
                "eventName": "ObjectCreated:Put",
                "userIdentity": {
                    "principalId": "AWS:AROA4IXYHPYNJHCREIMFQ:william.intan@umccr.org"
                },
                "requestParameters": {
                    "sourceIPAddress": "203.12.12.109"
                },
                "responseElements": {
                    "x-amz-request-id": "V0CVPFDSCCGY5DFV",
                    "x-amz-id-2": "+t5TGAN3ot3f1IdU8+qrwJard15pcpXTOhQowefJbKMROWWnpwmkJ6nkHROKCWulwIsP4CsFv6abYLWerVSBDteQDMWbN0EX"
                },
                "s3": {
                    "s3SchemaVersion": "1.0",
                    "configurationId": "Create or Delete",
                    "bucket": {
                        "name": "agha-staging-dev",
                        "ownerIdentity": {
                            "principalId": "A1VCVQDCTO40LG"
                        },
                        "arn": "arn:aws:s3:::agha-staging-dev"
                    },
                    "object": {
                        "key": "ACG/20210722_090101/manifest.txt",
                        "size": 219,
                        "eTag": "00516a3d4d409dd3681e4a8f32002f2a",
                        "sequencer": "0061A4618D70301F0E"
                    }
                }
            }
        ]
    }



class ManifestProcessorUnitTestCase(unittest.TestCase):



    def test_manifest_processor(self):
        event_payload = create_manifest_record_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()