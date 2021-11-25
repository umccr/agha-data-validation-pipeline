from aws_cdk import (
    aws_dynamodb as dynamodb,
    core
)


class DynamoDBStack(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        dynamodb_table = self.node.try_get_context("dynamodb_table")

        ################################################################################
        # Table for staging bucket dynamodb table
        # Partition Key: S3 key

        self.dynamodb_staging_bucket = dynamodb.Table(
            self,
            'DynamoDBTableStagingBucket',
            table_name=dynamodb_table['staging-bucket'],
            partition_key=dynamodb.Attribute(
                name='s3_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='flagship',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Archived Table for staging bucket dynamodb table
        # Partition Key: S3 key
        # Sort Key: date_modified

        self.dynamodb_staging_bucket_archive = dynamodb.Table(
            self,
            'DynamoDBTableStagingBucketArchive',
            table_name=dynamodb_table['staging-bucket-archive'],
            partition_key=dynamodb.Attribute(
                name='s3_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='date_modified',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Table for result bucket dynamodb table
        # Partition Key: S3 key
        # Sort Key: Combination of output, validation type
        #           (e.g. Output may be file and the type validation is checksum. 
        #            sort key would be FILE:CHECKSUM:LOG )

        self.dynamodb_result_bucket = dynamodb.Table(
            self,
            'DynamoDBTableResultBucket',
            table_name=dynamodb_table['result-bucket'],
            partition_key=dynamodb.Attribute(
                name='partition_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='sort_key',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Archived Table for result bucket dynamodb table
        # Partition Key: S3 key with timestamp
        # Sort Key: Combination of output, validation type
        #           (e.g. Output may be file and the type validation is checksum. 
        #            sort key would be FILE:CHECKSUM:LOG )

        self.dynamodb_result_bucket_archive = dynamodb.Table(
            self,
            'DynamoDBTableResultBucketArchive',
            table_name=dynamodb_table['result-bucket-archive'],
            partition_key=dynamodb.Attribute(
                name='partition_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='sort_key',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Table for store bucket dynamodb table
        # Partition Key: S3 key

        self.dynamodb_store_bucket = dynamodb.Table(
            self,
            'DynamoDBTableStoreBucket',
            table_name=dynamodb_table['store-bucket'],
            partition_key=dynamodb.Attribute(
                name='s3_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='flagship',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Archived Table for store bucket dynamodb table
        # Partition Key: S3 key
        # Sort Key: date_modified

        self.dynamodb_store_bucket_archive = dynamodb.Table(
            self,
            'DynamoDBTableStoreBucketArchive',
            table_name=dynamodb_table['store-bucket-archive'],
            partition_key=dynamodb.Attribute(
                name='s3_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='date_modified',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # eTag Table for staging bucket dynamodb table
        # Partition Key: etag
        # Sort Key: Will Store a combination of bucket_name and s3_key (see example below)
        #       (e.g. "BUCKET:{bucket_name}:S3_KEY:{s3_key}")

        self.dynamodb_e_tag = dynamodb.Table(
            self,
            'DynamoDBTableETag',
            table_name=dynamodb_table['e-tag'],
            partition_key=dynamodb.Attribute(
                name='etag',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='sort_key',
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=core.RemovalPolicy.RETAIN,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )
