"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/manifest_processor

Run python test command:
cmd: python -m unittest tests.test_file_manifest_processor.ManifestProcessorUnitTestCase

"""

import os
import unittest
import json
from unittest import mock

from manifest_processor import handler
from util import dynamodb
import pandas as pd


def ordered(obj):
    # To order JSON ordering for comparison
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


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
        "skip_send_notification": True,
    }


MOCK_DATA_FILE = {
    "filename": "filename.fastq.gz",
    "date_modified": "2022_02_22",
    "partition_key": "TYPE:FILE",
    "bucket_name": "agha-gdr-staging-2.0",
    "sort_key": "FlagShip/20220222/filename.fastq.gz",
    "size_in_bytes": "222",
    "etag": "3d274bd263f1e0dcf0950a5ba24e676c",
    "filetype": "FASTQ",
    "s3_key": "FlagShip/20220222/filename.fastq.gz",
}
MOCK_DATA_MANIFEST_FILE = {
    "filename": "filename.fastq.gz",
    "is_in_manifest": "True",
    "date_modified": "2022_02_22",
    "validation_status": "PASS",
    "partition_key": "TYPE:MANIFEST",
    "submission": "FlagShip/20220222/",
    "sort_key": "FlagShip/20220222/filename.fastq.gz",
    "agha_study_id": "A0000001",
    "filetype": "FASTQ",
    "flagship": "FlagShip",
    "provided_checksum": "b484664271be4fcf3551bd1f19f94017",
}

MOCK_MANIFEST_DATA = pd.json_normalize(
    {
        "checksum": "280acef56edbf4b39a75622c822f0e5c",
        "filename": "filename.fastq.gz",
        "agha_study_id": "A0000002",
    }
)

MOCK_BAD_MANIFEST_DATA = pd.json_normalize(
    {
        "checksum": "280acef56edbf4b39a75622c822f0e5c",
        "filename": "filename.fastq.gz",
        "agha_study_id": "Unknown",
    }
)  # Unknown study_id

MOCK_S3_METADATA_LIST = [
    {
        "Key": "FlagShip/filename.fastq.gz",
        "LastModified": "datetime(2015, 1, 1)",
        "ETag": "string",
        "Size": 123,
        "StorageClass": "STANDARD",
        "Owner": {"DisplayName": "string", "ID": "string"},
    },
]


def get_folder_lock_lambda_payload(lambda_arn, payload):
    if lambda_arn == os.environ["FOLDER_LOCK_LAMBDA_ARN"]:
        raise Exception(json.dumps(payload))
    else:
        pass


class ManifestProcessorUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = "local"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    @mock.patch(
        "manifest_processor.s3.get_s3_object_metadata",
        mock.MagicMock(return_value=MOCK_S3_METADATA_LIST),
    )
    @mock.patch(
        "manifest_processor.submission_data.retrieve_manifest_data",
        mock.MagicMock(return_value=MOCK_MANIFEST_DATA),
    )
    def test_manifest_reupload(self):
        # Insert mock data
        dynamodb.batch_write_objects(
            "agha-gdr-staging-bucket", [MOCK_DATA_FILE, MOCK_DATA_MANIFEST_FILE]
        )

        event_payload = {
            "bucket_name": "agha-gdr-staging-2.0",
            "manifest_fp": f"FlagShip/20220222/manifest.txt",
            "email_report_to": "example@email.com",
            "skip_send_notification": True,
        }

        handler(event_payload, {})

        new_manifest_record = dynamodb.get_item_from_exact_pk_and_sk(
            "agha-gdr-staging-bucket",
            "TYPE:MANIFEST",
            "FlagShip/20220222/filename.fastq.gz",
        )
        self.assertEqual(new_manifest_record["Items"][0]["agha_study_id"], "A0000002")

    @mock.patch(
        "manifest_processor.s3.get_s3_object_metadata",
        mock.MagicMock(return_value=MOCK_S3_METADATA_LIST),
    )
    @mock.patch(
        "manifest_processor.submission_data.retrieve_manifest_data",
        mock.MagicMock(return_value=MOCK_BAD_MANIFEST_DATA),
    )
    @mock.patch(
        "manifest_processor.util.call_lambda", new=get_folder_lock_lambda_payload
    )
    def test_folder_unlock_lambda_with_bad_manifest(self):
        """Will test if folder_lock lambda will be triggered for any failure."""
        key = "FlagShip/20220222/manifest.txt"
        expected_payload = {"task": "FOLDER_UNLOCK", "submission_prefix": key}
        event_payload = {
            "bucket_name": "agha-gdr-staging-2.0",
            "manifest_fp": key,
            "email_report_to": "example@email.com",
            "skip_send_notification": True,
        }

        # Insert mock data
        dynamodb.batch_write_objects(
            "agha-gdr-staging-bucket", [MOCK_DATA_FILE, MOCK_DATA_MANIFEST_FILE]
        )

        with self.assertRaises(Exception) as context:
            handler(event_payload, {})
        exception_raise = json.loads(str(context.exception))
        self.assertTrue(ordered(expected_payload) == ordered(exception_raise))
        return


if __name__ == "__main__":
    unittest.main()
