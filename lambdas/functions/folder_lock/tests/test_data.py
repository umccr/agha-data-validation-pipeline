EXISTING_KEY = "FlagShip/2022-02-02/FileName.fastq.gz"

EXISTING_BUCKET_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "FolderLock",
            "Effect": "Deny",
            "Principal": "*",
            "Action": ["s3:PutObject", "s3:DeleteObject"],
            "Resource": [
                "arn:aws:s3:::agha-gdr-staging-2.0/TEST/1970-01-01/*",
                "arn:aws:s3:::agha-gdr-staging-2.0/FlagShip/2022-02-02/*",
            ],
            "Condition": {
                "StringNotLike": {
                    "aws:userId": [
                        "AROADBQP57FF2AEXAMPLE:*",
                        "AIDACKCEVSQ6C2EXAMPLE:*",
                        "1234567890",
                    ]
                }
            },
        }
    ],
}
