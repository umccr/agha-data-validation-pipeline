"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/data_transfer_manager

Run python test command:
cmd: python -m unittest tests.test_data_transfer_manager.DataTransferManagerUnitTestCase

"""

import os
import unittest
from unittest import mock

from data_transfer_manager import handler

def create_manifest_record_payload():
    return     {
        "flagship_code": "ACG",
        "submission": "20210722_090101"
    }



class DataTransferManagerUnitTestCase(unittest.TestCase):

    def test_manifest_processor(self):
        event_payload = create_manifest_record_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()