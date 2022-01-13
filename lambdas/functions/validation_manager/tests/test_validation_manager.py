"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/validation_manager

Run python test command:
cmd: python -m unittest tests.test_validation_manager.ValidationManagerUnitTest

"""

import unittest

from validation_manager import handler

def create_payload():
    return {
    "manifest_fp": "BM/2019-09-05/manifest.txt",
    "manifest_dynamodb_key_prefix":"BM/2019-09-05/"
}



class ValidationManagerUnitTest(unittest.TestCase):

    def test_lambda(self):
        event_payload = create_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()