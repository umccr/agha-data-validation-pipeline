from aws_cdk import (
    aws_lambda as lambda_,
    aws_s3_notifications as s3notification,
    aws_iam as iam,
    aws_s3 as s3,
    core
)


class LambdaStack(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, batch, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Grab stack properties
        namespace = self.node.try_get_context("namespace")
        bucket_name = self.node.try_get_context("bucket_name")
        notification = self.node.try_get_context("notification")
        dynamodb_table = self.node.try_get_context("dynamodb_table")
        batch_environment = self.node.try_get_context("batch_environment")
        autorun_validation_jobs = self.node.try_get_context("autorun_validation_jobs")


        ################################################################################
        # S3 bucket
        staging_bucket = s3.Bucket.from_bucket_name(
            self,
            "StagingBucket",
            bucket_name=bucket_name['staging_bucket']
        )
        result_bucket = s3.Bucket.from_bucket_name(
            self,
            "ResultBucket",
            bucket_name=bucket_name['results_bucket']
        )
        store_bucket = s3.Bucket.from_bucket_name(
            self,
            "StoreBucket",
            bucket_name=bucket_name['store_bucket']
        )

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
            function_name=f'{namespace}-folder-lock',
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
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSSMReadOnlyAccess')
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
            function_name=f"{namespace}-notification",
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
        # File Validation Lambda (Trigger Batch)

        validation_manager_lambda_role = iam.Role(
            self,
            'ValidationManagerLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'IAMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess')
            ]
        )

        validation_manager_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'batch:SubmitJob'
                ],
                resources=[
                    batch.batch_job_queue.job_queue_arn,
                    batch.batch_job_definition.job_definition_arn,
                ]
            )
        )

        self.validation_manager_lambda = lambda_.Function(
            self,
            'ValidationManagerLambda',
            function_name=f"{namespace}-validation-manager",
            handler='validation_manager.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset('lambdas/functions/validation_manager'),
            environment={
                # Lambda ARN
                'NOTIFICATION_LAMBDA_ARN': self.notification_lambda.function_arn,
                # Table
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME': dynamodb_table["staging-bucket-archive"],
                'DYNAMODB_RESULT_TABLE_NAME': dynamodb_table["result-bucket"],
                # Batch
                'BATCH_QUEUE_NAME': batch_environment['batch_queue_name'],
                'JOB_DEFINITION_ARN': batch.batch_job_definition.job_definition_arn,
                # Buckets
                'RESULTS_BUCKET': bucket_name['results_bucket'],
                'STAGING_BUCKET': bucket_name['staging_bucket']
            },
            role=validation_manager_lambda_role,
            layers=[
                util_layer,
                runtime_layer
            ]
        )

        ################################################################################
        # Manifest Processor Lambda

        manifest_processor_lambda_role = iam.Role(
            self,
            'FileProcessorLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'IAMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess')
            ]
        )

        manifest_processor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'lambda:InvokeFunction'
                ],
                resources=[
                    self.notification_lambda.function_arn
                ]
            )
        )

        self.manifest_processor_lambda = lambda_.Function(
            self,
            'FileProcessorLambda',
            function_name=f"{namespace}-manifest-processor",
            handler='manifest_processor.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset('lambdas/functions/manifest_processor'),
            environment={
                # Lambda ARN
                'NOTIFICATION_LAMBDA_ARN': self.notification_lambda.function_arn,
                'VALIDATION_MANAGER_LAMBDA_ARN': self.validation_manager_lambda.function_arn,
                # Table
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME': dynamodb_table["staging-bucket-archive"],
                'DYNAMODB_ETAG_TABLE_NAME': dynamodb_table["e-tag"],
                # Bucket
                'STAGING_BUCKET': bucket_name['staging_bucket'],
                # AUTORUN
                "AUTORUN_VALIDATION_JOBS": autorun_validation_jobs
            },
            role=manifest_processor_lambda_role,
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
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess')
            ]
        )

        self.s3_event_recorder_lambda = lambda_.Function(
            self,
            'S3EventRecorderLambda',
            function_name=f"{namespace}-s3-event-recorder",
            handler='s3_event_recorder.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset(
                'lambdas/functions/s3_event_recorder'),
            environment={
                # Bucket
                'STAGING_BUCKET': bucket_name["staging_bucket"],
                'STORE_BUCKET': bucket_name["store_bucket"],
                'RESULT_BUCKET': bucket_name["results_bucket"],
                # Table
                'DYNAMODB_RESULT_TABLE_NAME': dynamodb_table["result-bucket"],
                'DYNAMODB_ARCHIVE_RESULT_TABLE_NAME': dynamodb_table["result-bucket-archive"],
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_ARCHIVE_STAGING_TABLE_NAME': dynamodb_table["staging-bucket-archive"],
                'DYNAMODB_STORE_TABLE_NAME': dynamodb_table["store-bucket"],
                'DYNAMODB_ARCHIVE_STORE_TABLE_NAME': dynamodb_table["store-bucket-archive"],
                'DYNAMODB_ETAG_TABLE_NAME': dynamodb_table["e-tag"]
            },
            role=s3_event_recorder_lambda_role,
            layers=[
                util_layer,
                runtime_layer
            ]
        )

        # add bucket notification to lambda
        result_bucket.add_object_created_notification(
            s3notification.LambdaDestination(self.s3_event_recorder_lambda)
        )
        store_bucket.add_object_created_notification(
            s3notification.LambdaDestination(self.s3_event_recorder_lambda)
        )
        store_bucket.add_object_removed_notification(
            s3notification.LambdaDestination(self.s3_event_recorder_lambda)
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
                    self.manifest_processor_lambda.function_arn,
                    self.s3_event_recorder_lambda.function_arn
                ]
            )
        )

        self.s3_event_router_lambda = lambda_.Function(
            self,
            'S3EventRouterLambda',
            function_name=f"{namespace}-s3-event-router",
            handler='s3_event_router.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(20),
            code=lambda_.Code.from_asset('lambdas/functions/s3_event_router'),
            environment={
                'STAGING_BUCKET': bucket_name["staging_bucket"],
                'MANIFEST_PROCESSOR_LAMBDA_ARN': self.manifest_processor_lambda.function_arn,
                'FOLDER_LOCK_LAMBDA_ARN': self.folder_lock_lambda.function_arn,
                'S3_RECORDER_LAMBDA_ARN': self.s3_event_recorder_lambda.function_arn
            },
            role=s3_event_router_lambda_role
        )

        # Bucket event emmit
        staging_bucket.add_object_created_notification(
            s3notification.LambdaDestination(self.s3_event_router_lambda)
        )
        staging_bucket.add_object_removed_notification(
            s3notification.LambdaDestination(self.s3_event_router_lambda)
        )

        ################################################################################
        # File Validation Lambda (Trigger Batch)

        data_transfer_manager_lambda_role = iam.Role(
            self,
            'DataTransferManagerLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'IAMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBFullAccess')
            ]
        )

        data_transfer_manager_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    's3:GetBucketPolicy',
                    's3:PutBucketPolicy',
                    's3:DeleteBucketPolicy'
                ],
                resources=[f'arn:aws:s3:::{bucket_name["staging_bucket"]}']
            )
        )

        data_transfer_manager_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'batch:SubmitJob'
                ],
                resources=[
                    batch.batch_job_queue.job_queue_arn,
                    batch.batch_s3_job_definition.job_definition_arn,
                ]
            )
        )
        self.data_transfer_manager_lambda = lambda_.Function(
            self,
            'DataTransferManagerLambda',
            function_name=f"{namespace}-data-transfer-manager",
            handler='data_transfer_manager.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lambda_.Code.from_asset('lambdas/functions/data_transfer_manager'),
            environment={
                # Batch
                'BATCH_QUEUE_NAME': batch_environment['batch_queue_name'],
                'S3_JOB_DEFINITION_ARN': batch.batch_s3_job_definition.job_definition_arn,
                # Buckets
                'STORE_BUCKET': bucket_name['store_bucket'],
                'RESULTS_BUCKET': bucket_name['results_bucket'],
                'STAGING_BUCKET': bucket_name['staging_bucket'],
                # Dynamodb
                'DYNAMODB_RESULT_TABLE_NAME': dynamodb_table["result-bucket"],
                'DYNAMODB_STORE_TABLE_NAME': dynamodb_table["store-bucket"],
                'DYNAMODB_STAGING_TABLE_NAME': dynamodb_table["staging-bucket"],
                'DYNAMODB_STORE_ARCHIVE_TABLE_NAME': dynamodb_table["store-bucket-archive"]

            },
            role=data_transfer_manager_lambda_role,
            layers=[
                util_layer,
                runtime_layer
            ]
        )
