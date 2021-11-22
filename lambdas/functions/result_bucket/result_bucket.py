from lambdas.layers.util import util

import util

class ResultFileRecord:
    """
    - partition_key: Most probably the s3 of the original file
    - sort_key: Prefix will be 'FILE' followed by check_type and the filename seperated by a colon
            (e.g. Result of index check file would be stored as 'FILE:INDEX:some_file.bai')
    - s3_key: The s3_key for the result file
    
    Other field are self explanotary taken from S3 metadata
    """
    def __init__(self):
        self.partition_key=""
        self.sort_key=""
        self.etag=""
        self.size_in_bytes=0
        self.filename=""
        self.s3_key=""
        self.flagship=""
        self.date_modified=util.get_datetimestamp()

class ResultDataRecord:
    """
    partition_key: Most probably the s3 of the original file
    sort_key: Prefix will be 'DATA' followed by check_type seperated by colon
            (e.g. Result of checksum check would be stored as 'DATA:CHECKSUM')
    value: The value of the test run (e.g checksum result will be '058116692da5f14d74a32f1db3195c30')

    """
    def __init__(self):
        self.partition_key=""
        self.sort_key=""
        self.date_modified=util.get_datetimestamp()
        self.value=""

class ResultStatusRecord:
    """
    partition_key: Most probably the s3 of the original file
    sort_key: Prefix will be 'DATA' followed by check_type seperated by colon
            (e.g. Result of checksum check would be stored as 'DATA:CHECKSUM')
    value: The status result for the tests (e.g 'SUCCESS', 'FAILURE')

    """
    def __init__(self):
        self.partition_key=""
        self.sort_key=""
        self.date_modified= util.get_datetimestamp()
        self.value=""
