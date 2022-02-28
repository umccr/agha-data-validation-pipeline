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
            "manifest_fp": f"NMD/2021-06-16/manifest.txt",
            "manifest_dynamodb_key_prefix": 'NMD/2021-06-16/',
            "skip_update_dynamodb":"true",
        }



class ValidationManagerUnitTest(unittest.TestCase):

    def test_lambda(self):
        event_payload = create_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()