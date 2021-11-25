import json
import os
import sys
import unittest
from unittest import mock

from s3_event_router import handler, call_lambda


def create_event_payload(event_name="", bucket_name="", s3_key=""):
    return {
        "Records": [
            {
                "eventName": event_name,
                "s3": {
                    "bucket": {
                        "name": bucket_name,
                        "ownerIdentity": {
                            "principalId": "Amazon-customer-ID-of-the-bucket-owner"
                        },
                        "arn": "bucket-ARN"
                    },
                    "object": {
                        "key": s3_key,
                        "size": "object-size",
                        "eTag": "object eTag",
                        "versionId": "object version if bucket is versioning-enabled, otherwise null",
                        "sequencer": "a string representation of a hexadecimal value used to determine event sequence"
                    }
                }
            }
        ]
    }



def get_lambda_called(lambda_called, *argv):

    print("Lambda Called: ",lambda_called)

    os.environ['LAMBDA_CALLED'] = lambda_called
    return lambda_called


class S3EventRouterUnitTestCase(unittest.TestCase):


    @mock.patch('s3_event_router.call_lambda', new=get_lambda_called, create=True)
    def test_s3_recorder_lambda(self):

        # Eye ball check if lambda is called
        print("\nS3_RECORDER_LAMBDA lambda is expected here:")

        event_payload = create_event_payload(event_name="ObjectCreated", bucket_name=os.environ.get("STAGING_BUCKET"),
                                             s3_key='non Manifest')
        handler(event_payload, {})
        assert os.environ.get('S3_RECORDER_LAMBDA_ARN') == os.environ.get('LAMBDA_CALLED'), \
            "S3 recoder lambda is expected"

    @mock.patch('s3_event_router.call_lambda', new=get_lambda_called)
    def test_s3_file_processor_lambda(self):

        # Eye ball check if lambda is called
        print("\nFOLDER_LOCK and FILE_PROCESSOR lambda is expected here:")

        event_payload = create_event_payload(event_name="ObjectCreated", bucket_name=os.environ.get("STAGING_BUCKET"),
                                             s3_key='manifest.txt')
        handler(event_payload, {})




if __name__ == '__main__':
    unittest.main()