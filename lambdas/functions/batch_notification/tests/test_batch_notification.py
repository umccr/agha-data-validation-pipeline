"""
To run the testcase

Change directory the s3_event_router
cmd from root directory: cd lambdas/functions/batch_notification

Run python test command:
cmd: python -m unittest tests.test_batch_notification.BatchNotificationUnitTestCase

"""
import os
import unittest
import json
from unittest import mock
from util import dynamodb
from batch_notification import handler, EventType

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

MOCK_DATA_FILE = {
    "filename": "filename.fastq.gz",
    "date_modified": "2022_02_22",
    "partition_key": "TYPE:FILE",
    "bucket_name": "agha-gdr-store-2.0",
    "sort_key": "FlagShip/20220222/filename.fastq.gz",
    "size_in_bytes": "222",
    "etag": "3d274bd263f1e0dcf0950a5ba24e676c",
    "filetype": "FASTQ",
    "s3_key": "FlagShip/20220222/filename.fastq.gz",
}
MOCK_MANIFEST_TXT_DATA_FILE = {
    "filename": "manifest.txt",
    "date_modified": "2022_02_22",
    "partition_key": "TYPE:FILE",
    "bucket_name": "agha-gdr-store-2.0",
    "sort_key": "FlagShip/20220222/manifest.txt",
    "size_in_bytes": "222",
    "etag": "3d274bd263f1e0dcf0950a5ba24e676c",
    "filetype": "MANIFEST",
    "s3_key": "FlagShip/20220222/filename.fastq.gz",
}

MOCK_MANIFEST_ORIG_DATA_FILE = {
    "filename": "manifest.orig",
    "date_modified": "2022_02_22",
    "partition_key": "TYPE:FILE",
    "bucket_name": "agha-gdr-store-2.0",
    "sort_key": "FlagShip/20220222/manifest.orig",
    "size_in_bytes": "222",
    "etag": "3d274bd263f1e0dcf0950a5ba24e676c",
    "filetype": "MANIFEST",
    "s3_key": "FlagShip/20220222/filename.fastq.gz",
}
MOCK_README_DATA_FILE = {
    "filename": "README.txt",
    "date_modified": "2022_02_22",
    "partition_key": "TYPE:FILE",
    "bucket_name": "agha-gdr-staging-2.0",
    "sort_key": "FlagShip/20220222/README.txt",
    "size_in_bytes": "222",
    "etag": "3d274bd263f1e0dcf0950a5ba24e676c",
    "filetype": "UNKNOWN",
    "s3_key": "FlagShip/20220222/README.txt",
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


def make_mock_data(event: EventType, s3_key: str):
    return {"event_type": event.value, "s3_key": s3_key}


def raise_error_lambda_payload(lambda_arn, payload):
    # Raise exception to identify function called
    raise Exception(json.dumps(payload))


def raise_slack_message(heading: str, title: str, message: str):
    # Raise exception to identify function called
    raise Exception("slack notified")


def ordered(obj):
    # To order JSON ordering for comparison
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


class BatchNotificationUnitTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # The key to use local dynamodb
        os.environ["AWS_ACCESS_KEY_ID"] = "local"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "local"
        os.environ["AWS_DEFAULT_REGION"] = "ap-southeast-2"
        os.environ["AWS_ENDPOINT"] = "http://localhost:4566"

    @mock.patch(
        "batch_notification.send_slack_notification", mock.MagicMock(return_value=None)
    )
    @mock.patch(
        "batch_notification.batch.run_batch_check", mock.MagicMock(return_value=[])
    )
    @mock.patch(
        "batch_notification.batch.run_status_result_check",
        mock.MagicMock(return_value=[]),
    )
    @mock.patch("batch_notification.util.call_lambda", new=raise_error_lambda_payload)
    def test_validation_lambda_invoked(self):
        key = "AGHA/20222222/test.fastq.gz"
        expected_exception = json.loads(
            """{"flagship_code": "AGHA", "submission": "20222222", "run_all": true}"""
        )

        payload = make_mock_data(EventType.VALIDATION_RESULT_UPLOAD, key)

        # Running to handler with exception raised
        with self.assertRaises(Exception) as context:
            handler(payload, {})
        exception_raise = json.loads(str(context.exception))

        self.assertTrue(ordered(expected_exception) == ordered(exception_raise))
        return

    @mock.patch("batch_notification.send_slack_notification", new=raise_slack_message)
    @mock.patch(
        "batch_notification.batch.run_batch_check",
        mock.MagicMock(return_value=["Uncomplete Check"]),
    )
    @mock.patch(
        "batch_notification.batch.run_status_result_check",
        mock.MagicMock(return_value=[]),
    )
    @mock.patch("batch_notification.util.call_lambda", new=raise_error_lambda_payload)
    def test_validation_lambda_terminated(self):
        key = "AGHA/20222222/test.fastq.gz"
        payload = make_mock_data(EventType.VALIDATION_RESULT_UPLOAD, key)

        res = handler(payload, {})
        self.assertTrue(res is None)

        return

    @mock.patch("batch_notification.send_slack_notification", new=raise_slack_message)
    @mock.patch(
        "batch_notification.batch.run_batch_check", mock.MagicMock(return_value=[])
    )
    @mock.patch(
        "batch_notification.batch.run_status_result_check",
        mock.MagicMock(return_value=["TEST_FAILED"]),
    )
    @mock.patch("batch_notification.util.call_lambda", new=raise_error_lambda_payload)
    def test_validation_fail_lambda_notification(self):
        key = "AGHA/20222222/test.fastq.gz"
        expected_exception = "slack notified"
        payload = make_mock_data(EventType.VALIDATION_RESULT_UPLOAD, key)

        # Running to handler with exception raised
        with self.assertRaises(Exception) as context:
            handler(payload, {})
        exception_raise = str(context.exception)

        self.assertTrue(expected_exception == exception_raise)
        return

    @mock.patch(
        "batch_notification.send_slack_notification", mock.MagicMock(return_value=None)
    )
    @mock.patch("batch_notification.util.call_lambda", new=raise_error_lambda_payload)
    @mock.patch(
        "batch_notification.batch.run_manifest_orig_store_check",
        mock.MagicMock(return_value=[]),
    )
    def test_cleanup_lambda_invoked(self):
        key = "FlagShip/20220222/filename.fastq.gz"
        expected_exception = json.loads(
            """{"submission_directory": "FlagShip/20220222/"}"""
        )

        # Inserting testing data
        dynamodb.batch_write_objects(
            "agha-gdr-store-bucket",
            [
                MOCK_DATA_FILE,
                MOCK_DATA_MANIFEST_FILE,
                MOCK_MANIFEST_TXT_DATA_FILE,
                MOCK_MANIFEST_ORIG_DATA_FILE,
            ],
        )

        payload = make_mock_data(EventType.STORE_FILE_UPLOAD, key)

        # Running to handler with exception raised
        with self.assertRaises(Exception) as context:
            handler(payload, {})
        exception_raise = json.loads(str(context.exception))

        # Check assertion
        self.assertTrue(ordered(expected_exception) == ordered(exception_raise))
        return

    @mock.patch(
        "batch_notification.send_slack_notification", mock.MagicMock(return_value=None)
    )
    @mock.patch("batch_notification.util.call_lambda", new=raise_error_lambda_payload)
    @mock.patch(
        "batch_notification.batch.run_manifest_orig_store_check",
        mock.MagicMock(return_value=[]),
    )
    def test_cleanup_store_terminated(self):
        key = "FlagShip/20220222/filename.fastq.gz"

        # Inserting testing data
        dynamodb.batch_write_objects("agha-gdr-staging-bucket", [MOCK_README_DATA_FILE])

        payload = make_mock_data(EventType.STORE_FILE_UPLOAD, key)

        # Running to handler with no lambda triggered raised
        res = handler(payload, {})

        # Check assertion
        self.assertTrue(res is None)
        return

    def tearDown(self):
        deletion_list = [
            {"partition_key": i["partition_key"], "sort_key": i["sort_key"]}
            for i in [
                MOCK_DATA_FILE,
                MOCK_DATA_MANIFEST_FILE,
                MOCK_MANIFEST_TXT_DATA_FILE,
                MOCK_MANIFEST_ORIG_DATA_FILE,
                MOCK_README_DATA_FILE,
            ]
        ]
        dynamodb.batch_delete_from_dictionary("agha-gdr-staging-bucket", deletion_list)


if __name__ == "__main__":
    unittest.main()
