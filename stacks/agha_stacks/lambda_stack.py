from aws_cdk import (
    aws_lambda as lambda_,
    aws_iam as iam,
    core
)


class LambdaStack(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Grab stack properties
        namespace = self.node.try_get_context("namespace")
        bucket_name = self.node.try_get_context("bucket_name")
        notification = self.node.try_get_context("notification")
        dynamodb_table = self.node.try_get_context("dynamodb_table")

        ################################################################################
        # Lambda layers

        runtime_layer = lambda_.LayerVersion(
            self,
            'RuntimeLambdaLayer',
            code=lambda_.Code.from_asset(
                'lambdas/layers/runtime/python38-runtime.zip'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            description='A runtime layer for python 3.8'
        )

        util_layer = lambda_.LayerVersion(
            self,
            'UtilLambdaLayer',
            code=lambda_.Code.from_asset(
                'lambdas/layers/util/python38-util.zip'),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
            description='A shared utility layer for python 3.8'
        )

        ################################################################################
        # Folder lock Lambda

        folder_lock_lambda_role = iam.Role(
            self,
            'FolderLockLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole')
            ]
        )

        folder_lock_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    's3:GetBucketPolicy',
                    's3:PutBucketPolicy',
                    's3:DeleteBucketPolicy'
                ],
                resources=[f'arn:aws:s3:::{bucket_name["staging_bucket"]}']
            )
        )

        self.folder_lock_lambda = lambda_.Function(
            self,
            'FolderLockLambda',
            function_name=f'{namespace}_folder_lock_lambda',
            handler='folder_lock.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset('lambdas/functions/folder_lock/'),
            environment={
                'STAGING_BUCKET': bucket_name['staging_bucket']
            },
            role=folder_lock_lambda_role
        )

        ################################################################################
        # Notification Lambda

        notification_lambda_role = iam.Role(
            self,
            'NotificationLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole')
            ]
        )

        notification_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'ses:SendEmail',
                    'ses:SendRawEmail',
                ],
                # NOTE(SW): resources related to identities i.e. email addresses. We could
                # construct ARNs using the manager and sender email defined in props.
                resources=['*']
            )
        )

        self.notification_lambda = lambda_.Function(
            self,
            'NotificationLambda',
            function_name=f"{namespace}_notification_lambda",
            handler='notification.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(20),
            code=lambda_.Code.from_asset('lambdas/functions/notification/'),
            environment={
                'SLACK_NOTIFY': notification["slack_notify"],
                'EMAIL_NOTIFY': notification["email_notify"],
                'SLACK_HOST': notification["slack_host"],
                'SLACK_CHANNEL': notification["slack_channel"],
                'MANAGER_EMAIL': notification["manager_email"],
                'SENDER_EMAIL': notification["sender_email"]
            },
            role=notification_lambda_role,
            layers=[
                util_layer,
            ]
        )

        ################################################################################
        # Validation Lambda

        validation_lambda_role = iam.Role(
            self,
            'ValidationLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'IAMReadOnlyAccess')
            ]
        )

        self.validation_lambda = lambda_.Function(
            self,
            'ValidationLambda',
            function_name=f"{namespace}_validation_lambda",
            handler='file_processor.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset('lambdas/functions/validation'),
            environment={
                # Lambda ARN
                'FOLDER_LOCK_LAMBDA_ARN': self.folder_lock_lambda.function_arn,
                'NOTIFICATION_LAMBDA_ARN': self.notification_lambda.function_arn,
                # Table
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME': dynamodb_table["staging-bucket-archive"]
            },
            role=validation_lambda_role,
            layers=[
                util_layer,
                runtime_layer
            ]
        )
        ################################################################################
        # S3 event recorder Lambda

        s3_event_recorder_lambda_role = iam.Role(
            self,
            'S3EventRecorderLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess')
            ]
        )

        self.s3_event_recorder_lambda = lambda_.Function(
            self,
            'S3EventRecorderLambda',
            function_name=f"{namespace}_s3_event_recorder_lambda",
            handler='s3_event_recorder.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset(
                'lambdas/functions/s3_event_recorder'),
            environment={
                # Bucket
                'STAGING_BUCKET': bucket_name["staging_bucket"],
                'STORE_BUCKET': bucket_name["store_bucket"],
                # Table
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME': dynamodb_table["staging-bucket-archive"]
            },
            role=s3_event_recorder_lambda_role,
            layers=[
                util_layer,
            ]
        )

        ################################################################################
        # S3 event router Lambda

        s3_event_router_lambda_role = iam.Role(
            self,
            'S3EventRouterLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole')
            ]
        )

        s3_event_router_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:InvokeFunction"
                ],
                resources=[
                    self.folder_lock_lambda.function_arn,
                    self.validation_lambda.function_arn,
                    self.s3_event_recorder_lambda.function_arn
                ]
            )
        )

        self.s3_event_router_lambda = lambda_.Function(
            self,
            'S3EventRouterLambda',
            function_name=f"{namespace}_s3_event_router_lambda",
            handler='s3_event_router.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(20),
            code=lambda_.Code.from_asset('lambdas/functions/s3_event_router'),
            environment={
                'STAGING_BUCKET': bucket_name["staging_bucket"],
                'VALIDATION_LAMBDA_ARN': self.validation_lambda.function_arn,
                'FOLDER_LOCK_LAMBDA_ARN': self.folder_lock_lambda.function_arn,
                'S3_RECORDER_LAMBDA_ARN': self.s3_event_recorder_lambda.function_arn
            },
            role=s3_event_router_lambda_role
        )
