"""
To run the testcase

Change directory the s3_event_router
cmd from root directory: cd lambdas/layers/util

Run python test command:
cmd: python -m unittest util.tests.test_s3.TestS3Layer

"""

import unittest
from unittest import mock
from util import s3


class TestS3Layer(unittest.TestCase):
    def setUp(self) -> None:
        pass

    def test_get_s3_location(self):
        expected_region_test = "ap-southeast-2"

        # Mock function
        s3.S3_CLIENT.get_bucket_location = mock.MagicMock(
            return_value={"LocationConstraint": expected_region_test}
        )

        response_region = s3.get_s3_location("")
        self.assertEqual(
            expected_region_test, response_region, "Does not match with expected region"
        )

    def test_create_s3_cp_command_from_s3_uri(self):
        source_uri = "s3://source/puppy.jpg"
        target_uri = "s3://target/puppy.jpg"
        filename = "puppy.jpg"

        expected_result = (
            f"s3 sync s3://source/ s3://target/ --exclude='*' --include='*/{filename}'"
        )

        response_func = s3.create_s3_sync_command_from_s3_uri(source_uri, target_uri)
        self.assertEqual(
            expected_result, response_func, "Does not match with expected command"
        )

    def test_create_s3_uri_from_bucket_name_and_key(self):
        expected_uri = "s3://mybucket/puppy.jpg"

        response_func = s3.create_s3_uri_from_bucket_name_and_key(
            "mybucket", "puppy.jpg"
        )

        self.assertEqual(
            expected_uri, response_func, "Does not match with expected uri"
        )

    def tearDown(self):
        pass


if __name__ == "__main__":
    unittest.main()
