"""
To run the testcase

Change directory
cmd from root directory: cd lambdas/functions/report

Run python test command:
cmd: python -m unittest tests.test_report.ReportUnitTestCase

"""

import os
import unittest
from unittest import mock

from report import handler

def create_report_payload():
    return {
        "check_type": "file_transfer",
        "submission_prefix": "AC/20210531_162251/"
    }



class ReportUnitTestCase(unittest.TestCase):

    def test_manifest_processor(self):
        event_payload = create_report_payload()

        res = handler(event_payload,{})

        print(res)




if __name__ == '__main__':
    unittest.main()