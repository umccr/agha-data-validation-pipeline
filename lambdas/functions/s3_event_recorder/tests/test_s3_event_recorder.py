"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/s3_event_recorder

Run python test command:
cmd: python -m unittest tests.test_s3_event_recorder.S3EventRecorderUnitTest

"""

import unittest

from s3_event_recorder import handler

def create_payload():
    return {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "ap-southeast-2",
            "eventTime": "2021-12-16T11:03:25.663Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
                "principalId": ""
            },
            "requestParameters": {
                "sourceIPAddress": ""
            },
            "responseElements": {
                "x-amz-request-id": "",
                "x-amz-id-2": ""
            },
            "s3": {
                "s3SchemaVersion": "1.0",
                "configurationId": "",
                "bucket": {
                    "name": "agha-results-dev",
                    "ownerIdentity": {
                        "principalId": ""
                    },
                    "arn": "arn:aws:s3:::agha-results-dev"
                },
                "object": {
                    "key": "ACG/20210722_090101/SBJ00592-somatic-PASS.vcf.gz__results.json",
                    "size": 236,
                    "eTag": "caa865c523195e816f244c5893558926",
                    "sequencer": ""
                }
            }
        }
    ]
}



class S3EventRecorderUnitTest(unittest.TestCase):

    def test_lambda(self):
        event_payload = create_payload()

        handler(event_payload,{})




if __name__ == '__main__':
    unittest.main()