"""
To run the testcase

Change directory the the s3_event_router
cmd from root directory: cd lambdas/functions/batch_notification

Run python test command:
cmd: python -m unittest tests.test_batch_notification.BatchNotificationUnitTestCase

"""

import os
import unittest
from unittest import mock

from batch_notification import handler

S3_MOVE_JOB_ARN = 'arn:aws:batch:ap-southeast-2:602836945884:job-definition/agha-gdr-s3-manipulation:15'
VALIDATE_FILE_JOB_ARN = 'arn:aws:batch:ap-southeast-2:602836945884:job-definition/agha-gdr-validate-file:15'


class BatchNotificationUnitTestCase(unittest.TestCase):

    def test_batch_notification(self):
        event_payload = make_mock_data(
            status='SUCCEEDED',
            job_arn=S3_MOVE_JOB_ARN,
            command=['--s3_key',
                     'ABCDE/123_hello_world.fastq.gz'])
        res = handler(event_payload, {})

        print(res)


if __name__ == '__main__':
    unittest.main()


def make_mock_data(status: str, job_arn: str, command: [str]):
    return {
        "version": "0",
        "id": "c8f9c4b5-76e5-d76a-f980-7011e206042b",
        "detail-type": "Batch Job State Change",
        "source": "aws.batch",
        "account": "123456789012",
        "time": "2022-01-11T23:36:40Z",
        "region": "us-east-1",
        "resources": [
            "arn:aws:batch:us-east-1:123456789012:job/4c7599ae-0a82-49aa-ba5a-4727fcce14a8"
        ],
        "detail": {
            "jobArn": job_arn,
            "jobName": 'job_name',
            "jobId": "4c7599ae-0a82-49aa-ba5a-4727fcce14a8",
            "jobQueue": "arn:aws:batch:us-east-1:123456789012:job-queue/PexjEHappyPathCanary2JobQueue",
            "status": status,
            "attempts": [],
            "createdAt": 1641944200058,
            "retryStrategy": {
                "attempts": 2,
                "evaluateOnExit": []
            },
            "dependsOn": [],
            "jobDefinition": "arn:aws:batch:us-east-1:123456789012:job-definition/first-run-job-definition:1",
            "parameters": {},
            "container": {
                "image": "137112412989.dkr.ecr.us-east-1.amazonaws.com/amazonlinux:latest",
                "command": command,
                "volumes": [],
                "environment": [],
                "mountPoints": [],
                "ulimits": [],
                "networkInterfaces": [],
                "resourceRequirements": [
                    {
                        "value": "2",
                        "type": "VCPU"
                    }, {
                        "value": "256",
                        "type": "MEMORY"
                    }
                ],
                "secrets": []
            },
            "tags": {
                "resourceArn": "arn:aws:batch:us-east-1:123456789012:job/4c7599ae-0a82-49aa-ba5a-4727fcce14a8"
            },
            "propagateTags": False
            ,
            "platformCapabilities": []
        }
    }
