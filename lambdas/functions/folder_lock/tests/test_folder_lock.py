"""
To run the testcase

Change directory to folder_lock
cmd from root directory: cd lambdas/functions/folder_lock

Run python test command:
cmd: python -m unittest tests.test_folder_lock.FolderLockUnitTestCase

"""
import json
import os
import unittest
from unittest import mock

from folder_lock import (
    handler,
    TaskType,
    find_folder_lock_statement,
    create_resource_arn_from_submission_prefix,
)
from tests.test_data import EXISTING_KEY, EXISTING_BUCKET_POLICY


def raise_new_bucket_policy(Bucket, Policy: str):
    raise Exception(Policy)


def simulate_get_bucket_policy():
    return {"Policy": json.dumps(EXISTING_BUCKET_POLICY)}


class FolderLockUnitTestCase(unittest.TestCase):
    @mock.patch("folder_lock.s3.put_bucket_policy", new=raise_new_bucket_policy)
    @mock.patch(
        "folder_lock.s3.get_bucket_policy",
        mock.MagicMock(return_value=simulate_get_bucket_policy()),
    )
    def test_locking_folder(self):
        """
        Testing to lock with existing policy statement
        """
        new_key = "NEW/2022-02-02/FileName.fastq.gz"
        new_resource_arn = create_resource_arn_from_submission_prefix(new_key)
        payload = create_event(TaskType.FOLDER_LOCK, new_key)

        with self.assertRaises(Exception) as context:
            handler(payload, {})
        exception_raise = json.loads(str(context.exception))
        policy_statement = find_folder_lock_statement(exception_raise)

        # Check if new key in resource
        new_resource_statement = policy_statement.get("Resource")

        self.assertTrue(new_resource_arn in new_resource_statement)

    @mock.patch("folder_lock.s3.put_bucket_policy", new=raise_new_bucket_policy)
    @mock.patch(
        "folder_lock.s3.get_bucket_policy",
        mock.MagicMock(return_value=simulate_get_bucket_policy()),
    )
    def test_deleting_folder(self):
        """Testing if deleting prefix from statement is OK"""

        new_resource_arn = create_resource_arn_from_submission_prefix(EXISTING_KEY)
        payload = create_event(TaskType.FOLDER_UNLOCK, EXISTING_KEY)

        with self.assertRaises(Exception) as context:
            handler(payload, {})
        exception_raise = json.loads(str(context.exception))
        policy_statement = find_folder_lock_statement(exception_raise)

        # Check if new key in resource
        new_resource_statement = policy_statement.get("Resource")
        self.assertTrue(new_resource_arn not in new_resource_statement)


if __name__ == "__main__":
    unittest.main()


def create_event(task_type: TaskType, submission_prefix: str):
    return {"submission_prefix": submission_prefix, "task": task_type.value}
