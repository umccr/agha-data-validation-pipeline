"""
TO run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/s3_event_router

Run python test command:
cmd: python -m unittest tests.test_s3_event_router.S3EventRouterUnitTestCase

"""

import os
import unittest
import json
from unittest import mock

from s3_event_router import handler, call_lambda


def ordered(obj):
    # To order JSON ordering for comparison
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


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
                        "arn": "bucket-ARN",
                    },
                    "object": {
                        "key": s3_key,
                        "size": "object-size",
                        "eTag": "object eTag",
                        "versionId": "object version if bucket is versioning-enabled, otherwise null",
                        "sequencer": "a string representation of a hexadecimal value used to determine event sequence",
                    },
                },
            }
        ]
    }


def get_lambda_called(lambda_called, *argv):
    print("Lambda Called: ", lambda_called)

    os.environ["LAMBDA_CALLED"] = lambda_called
    return lambda_called


def get_folder_lock_lambda_payload(lambda_arn, payload):
    if lambda_arn == os.environ["FOLDER_LOCK_LAMBDA_ARN"]:
        raise Exception(json.dumps(payload))
    else:
        pass


class S3EventRouterUnitTestCase(unittest.TestCase):
    @mock.patch("s3_event_router.call_lambda", new=get_lambda_called, create=True)
    def test_s3_recorder_lambda(self):
        # Eye ball check if lambda is called
        print("\nS3_RECORDER_LAMBDA lambda is expected here:")

        event_payload = create_event_payload(
            event_name="ObjectCreated",
            bucket_name=os.environ.get("STAGING_BUCKET"),
            s3_key="non Manifest",
        )
        handler(event_payload, {})
        assert os.environ.get("S3_RECORDER_LAMBDA_ARN") == os.environ.get(
            "LAMBDA_CALLED"
        ), "S3 recoder lambda is expected"

    @mock.patch("s3_event_router.call_lambda", new=get_lambda_called)
    def test_s3_manifest_processor_lambda(self):
        # Eye ball check if lambda is called
        print("\nFOLDER_LOCK and MANIFEST_PROCESSOR lambda is expected here:")

        event_payload = create_event_payload(
            event_name="ObjectCreated",
            bucket_name=os.environ.get("STAGING_BUCKET"),
            s3_key="manifest.txt",
        )
        handler(event_payload, {})

    @mock.patch("s3_event_router.call_lambda", new=get_folder_lock_lambda_payload)
    def test_folder_lock_trigger_payload(self):
        """Testing the payload given to folder_lock lambda"""
        key = "flagship/date/manifest.txt"
        expected_payload = {"task": "FOLDER_LOCK", "submission_prefix": key}

        event_payload = create_event_payload(
            event_name="ObjectCreated",
            bucket_name=os.environ.get("STAGING_BUCKET"),
            s3_key=key,
        )
        with self.assertRaises(Exception) as context:
            handler(event_payload, {})
        exception_raise = json.loads(str(context.exception))
        self.assertTrue(ordered(expected_payload) == ordered(exception_raise))
        return


if __name__ == "__main__":
    unittest.main()
