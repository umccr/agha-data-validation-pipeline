STORE_TYPE_FILE = [
    {
        "filename": "19W000000.bam",
        "date_modified": "20220105_051419",
        "partition_key": "TYPE:FILE",
        "bucket_name": "agha-gdr-store-2.0",
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "size_in_bytes": "60600149772",
        "etag": "6bcf86bed8807b8e78f0fc6e0a53079d-7225",
        "filetype": "BAM",
        "s3_key": "Cardiac/2001-02-25/19W000000.bam"
    },
    {
        "filename": "19W000000.bam.bai",
        "date_modified": "20220105_051030",
        "partition_key": "TYPE:FILE",
        "bucket_name": "agha-gdr-store-2.0",
        "sort_key": "Cardiac/2001-02-25/19W000000.bam.bai",
        "size_in_bytes": "9471736",
        "etag": "6bcf86bed8807b8e78f0fc6e0a53079d-2",
        "filetype": "BAM_INDEX",
        "s3_key": "Cardiac/2001-02-25/19W000000.bam.bai"
    }
]

STORE_TYPE_MANIFEST = [
    {
        "filename": "19W000000.bam",
        "is_in_manifest": "True",
        "date_modified": "20220105_050833",
        "validation_status": "PASS",
        "partition_key": "TYPE:MANIFEST",
        "submission": "Cardiac/2001-02-25",
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "agha_study_id": "A0000001",
        "filetype": "BAM",
        "flagship": "Cardiac",
        "provided_checksum": "55a4d844031c5f0db9a1d17365e89f15"
    },
    {
        "filename": "19W000000.bam.bai",
        "is_in_manifest": "True",
        "date_modified": "20220105_050833",
        "validation_status": "PASS",
        "partition_key": "TYPE:MANIFEST",
        "submission": "Cardiac/2001-02-25",
        "sort_key": "Cardiac/2001-02-25/19W000000.bam.bai",
        "agha_study_id": "A0000001",
        "filetype": "BAM_INDEX",
        "flagship": "Cardiac",
        "provided_checksum": "55a4d844031c5f0db9a1d17365e89f15"
    }
]

ETAG_RECORDS = [
    {
        "sort_key": "BUCKET:agha-gdr-store-2.0:S3_KEY:Cardiac/2001-02-25/19W000000.bam",
        "etag": "e1833f461b33fd997993b25d857ac4c8-7225",
        "partition_key": "e1833f461b33fd997993b25d857ac4c8-7225",
        "s3_key": "Cardiac/2001-02-25/19W000000.bam",
        "bucket_name": "agha-gdr-store-2.0"
    },
    {
        "sort_key": "BUCKET:agha-gdr-store-2.0:S3_KEY:Cardiac/2001-02-25/19W000000.bam.bai",
        "etag": "bcab3e5a609262e1f0495f502b30ecb6-2",
        "partition_key": "bcab3e5a609262e1f0495f502b30ecb6-2",
        "s3_key": "Cardiac/2001-02-25/19W000000.bam.bai",
        "bucket_name": "agha-gdr-store-2.0"
    }
]

STATUS_RESULT = [
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": "PASS",
        "date_modified": "20211231_083030",
        "partition_key": "STATUS:CREATE_INDEX"
    },
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": "PASS",
        "date_modified": "20211231_083030",
        "partition_key": "STATUS:CHECKSUM_VALIDATION"
    },
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": "PASS",
        "date_modified": "20211231_083030",
        "partition_key": "STATUS:FILE_VALIDATION"
    }
]

DATA_RESULT= [
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": "4cd9755362784faa7b94a0c643f39460",
        "date_modified": "20211231_083030",
        "partition_key": "DATA:CHECKSUM_VALIDATION"
    },
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": "BAM",
        "date_modified": "20211231_083030",
        "partition_key": "DATA:FILE_VALIDATION"
    },
    {
        "sort_key": "Cardiac/2001-02-25/19W000000.bam",
        "value": {
            "s3_key": "Cardiac/2001-02-25/19W000000.bam.bai",
            "checksum": "55a4d844031c5f0db9a1d17365e89f15",
            "bucket_name": "agha-gdr-store-2.0"
        },
        "date_modified": "20211231_083030",
        "partition_key": "DATA:CREATE_INDEX"
    }
]
STAGING_TYPE_FILE = [
    {
        "filename": "19W111111.bam",
        "date_modified": "20220105_051419",
        "partition_key": "TYPE:FILE",
        "bucket_name": "agha-gdr-staging-2.0",
        "sort_key": "Cardiac/2007-10-31/19W111111.bam",
        "size_in_bytes": "60600149772",
        "etag": "6bcf86bed8807b8e78f0fc6e0a53079d-7225",
        "filetype": "BAM",
        "s3_key": "Cardiac/2007-10-31/19W111111.bam"
    },
    {
        "filename": "19W111111.bam.bai",
        "date_modified": "20220105_051030",
        "partition_key": "TYPE:FILE",
        "bucket_name": "agha-gdr-staging-2.0",
        "sort_key": "Cardiac/2007-10-31/19W111111.bam.bai",
        "size_in_bytes": "9471736",
        "etag": "6bcf86bed8807b8e78f0fc6e0a53079d-2",
        "filetype": "BAM_INDEX",
        "s3_key": "Cardiac/2007-10-31/19W111111.bam.bai"
    }
]

STAGING_TYPE_MANIFEST = [
    {
        "filename": "19W111111.bam",
        "is_in_manifest": "True",
        "date_modified": "20220105_050833",
        "validation_status": "PASS",
        "partition_key": "TYPE:MANIFEST",
        "submission": "Cardiac/2007-10-31",
        "sort_key": "Cardiac/2007-10-31/19W111111.bam",
        "agha_study_id": "A0000002",
        "filetype": "BAM",
        "flagship": "Cardiac",
        "provided_checksum": "702242d3703818ddefe6bf7da2bed757"
    },
    {
        "filename": "19W111111.bam.bai",
        "is_in_manifest": "True",
        "date_modified": "20220105_050833",
        "validation_status": "PASS",
        "partition_key": "TYPE:MANIFEST",
        "submission": "Cardiac/2007-10-31",
        "sort_key": "Cardiac/2007-10-31/19W111111.bam.bai",
        "agha_study_id": "A0000002",
        "filetype": "BAM_INDEX",
        "flagship": "Cardiac",
        "provided_checksum": "702242d3703818ddefe6bf7da2bed757"
    }
]
