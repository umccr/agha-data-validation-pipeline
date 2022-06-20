from aws_cdk import (
    core,
    aws_batch as batch,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lambda_,
)


class S3DataSharing(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, batch_stack, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # stack properties
        namespace = self.node.try_get_context("namespace")
        dynamodb_table = self.node.try_get_context("dynamodb_table")
        bucket_name = self.node.try_get_context("bucket_name")

        ####################################################
        # Allow read-only access for our own S3 and
        # put-access on destination bucket

        self.s3_data_sharing_task_role = iam.Role(
            self,
            'S3DataSharingTaskRole',
            role_name="agha-gdr-s3-data-sharing",
            assumed_by=iam.ServicePrincipal('ecs-tasks.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2ContainerServiceforEC2Role'),
            ],
            inline_policies={
                "gdr-s3-read-store-bucket-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            effect=iam.Effect.ALLOW,
                            actions=[
                                "s3:Get*",
                                "s3:List*",
                                "s3-object-lambda:Get*",
                                "s3-object-lambda:List*"
                            ],
                            resources=[
                                f"arn:aws:s3:::{bucket_name['store_bucket']}",
                                f"arn:aws:s3:::{bucket_name['store_bucket']}/*"
                            ]
                        )
                    ]
                )
            }
        )

        self.gdr_s3_sharing_policy = iam.Policy(
            self,
            'S3GDRPutSharingPolicy',
            statements=[iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:*"],
                not_resources=[
                    f"arn:aws:s3:::{bucket_name['store_bucket']}",
                    f"arn:aws:s3:::{bucket_name['store_bucket']}/*",
                    f"arn:aws:s3:::{bucket_name['staging_bucket']}",
                    f"arn:aws:s3:::{bucket_name['staging_bucket']}/*",
                    f"arn:aws:s3:::{bucket_name['results_bucket']}",
                    f"arn:aws:s3:::{bucket_name['results_bucket']}/*"
                ]
            )],
            policy_name="gdr-s3-sharing-put-bucket-policy",
            roles=[self.s3_data_sharing_task_role]
        )

        self.gdr_s3_sharing_job_queue = batch.JobQueue(
            self,
            f"GdrS3SharingJobQueue",
            job_queue_name="agha-gdr-s3-data-sharing",
            compute_environments=[
                batch.JobQueueComputeEnvironment(
                    compute_environment=batch_stack.batch_compute_environment['small'],
                    order=1
                )
            ]
        )

        ################################################################################################################

        # Batch job definition
        self.gdr_s3_sharing_job_definition = batch.JobDefinition(
            self,
            'S3DataSharingJobDefinition',
            job_definition_name="agha-gdr-s3-data-sharing",
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_registry('amazon/aws-cli:latest'),
                command=['true'],
                memory_limit_mib=200,
                vcpus=1,
                execution_role=self.s3_data_sharing_task_role
            ),
            retry_attempts=2
        )

        # ################################################################################################################
        # # Create lambda to submit move

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

        # Defining roles for lambda
        gdr_s3_data_sharing_lambda_role = iam.Role(
            self,
            'GDRSharingLambdaRolerLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonDynamoDBReadOnlyAccess'),
            ]
        )

        # Give access to execute job from lambda
        gdr_s3_data_sharing_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'batch:SubmitJob'
                ],
                resources=[
                    self.gdr_s3_sharing_job_queue.job_queue_arn,
                    self.gdr_s3_sharing_job_definition.job_definition_arn
                ]
            )
        )

        # We could change policy via lambda to upload stuff on s3 destination bucket
        gdr_s3_data_sharing_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "iam:AttachRolePolicy",
                    "iam:CreatePolicy"
                ],
                resources=[
                    self.s3_data_sharing_task_role.role_arn,
                ]
            )
        )

        self.validation_manager_lambda = lambda_.Function(
            self,
            'S3DataSharingLambda',
            function_name=f"{namespace}-s3-data-sharing",
            handler='gdr_s3_data_sharing.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(300),
            retry_attempts=0,
            code=lambda_.Code.from_asset('lambdas/functions/gdr_s3_data_sharing'),
            environment={
                # Table
                'DYNAMODB_STORE_TABLE_NAME': dynamodb_table["store-bucket"],
                # Batch
                'S3_DATA_SHARING_BATCH_QUEUE_NAME': self.gdr_s3_sharing_job_queue.job_queue_arn,
                'S3_DATA_SHARING_JOB_DEFINITION_ARN': self.gdr_s3_sharing_job_definition.job_definition_arn,
                # Buckets
                'STORE_BUCKET': bucket_name['store_bucket'],
            },
            role=gdr_s3_data_sharing_lambda_role,
            memory_size=1769,
            layers=[
                util_layer,
                runtime_layer
            ]
        )
