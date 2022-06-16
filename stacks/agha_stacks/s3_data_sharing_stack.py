from aws_cdk import (
    core,
    aws_batch as batch,
    aws_ec2 as ec2,
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
        batch_environment = self.node.try_get_context("batch_environment")
        bucket_name = self.node.try_get_context("bucket_name")

        ################################################################################
        # Batch

        vpc = ec2.Vpc.from_lookup(
            self,
            'MainVPC',
            vpc_id=batch_environment['vpc_id']
        )

        # Only small instances (s3 cli should not need much)
        micro_instance_type = [
            'm3.medium',
            'm4.large',
            'm5.large',
            'c3.large',
            'c4.large',
            'c5.large',
        ]

        ####################################################
        # Allow read-only access for our own S3 and
        # put-access on destination bucket

        self.s3_data_sharing_instance_role = iam.Role(
            self,
            'S3DataSharingIntanceRole',
            role_name="agha-gdr-s3-data-sharing",
            assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
            inline_policies={
                "s3-data-sharing-policy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "s3:GetObject",
                                "s3:PutObject",
                                "s3:PutObjectAcl",
                                "s3:GetBucketLocation"
                            ],
                            resources=[
                                f"arn:aws:s3:::{bucket_name['staging_bucket']}/TEST/*"
                            ]
                        ),
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

        block_device_mappings = [
            ec2.CfnLaunchTemplate.BlockDeviceMappingProperty(
                device_name='/dev/xvda',
                ebs=ec2.CfnLaunchTemplate.EbsProperty(
                    encrypted=True,
                    volume_size=8,
                    volume_type='gp3'
                )
            ),
        ]

        batch_launch_template = ec2.CfnLaunchTemplate(
            self,
            f"S3DataS3SharingBatchLaunchTemplate",
            launch_template_name=f"agha-validation-launch-template-s3-data-sharing",
            launch_template_data=ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
                block_device_mappings=block_device_mappings,
            ),
        )

        batch_launch_template_spec = batch.LaunchTemplateSpecification(
            launch_template_name=batch_launch_template.launch_template_name,
            version='$Latest',
        )

        batch_compute_environment = batch.ComputeEnvironment(
            self,
            f"BatchComputeEnvironmentS3DataSharing",
            compute_environment_name="BatchComputeEnvironmentS3DataSharing",
            compute_resources=batch.ComputeResources(
                vpc=vpc,
                allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                desiredv_cpus=0,
                instance_role=batch_stack.batch_instance_profile.attr_arn,
                instance_types=[ec2.InstanceType(it) for it in micro_instance_type],
                launch_template=batch_launch_template_spec,
                maxv_cpus=16,
                security_groups=[batch_stack.batch_security_group],
                spot_fleet_role=self.s3_data_sharing_instance_role,
                type=batch.ComputeResourceType.SPOT,
            )
        )

        self.gdr_s3_sharing_job_queue = batch.JobQueue(
            self,
            f"BatchJobQueueMicro",
            job_queue_name="agha-gdr-s3-data-sharing",
            compute_environments=[
                batch.JobQueueComputeEnvironment(
                    compute_environment=batch_compute_environment,
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
                    self.s3_data_sharing_instance_role.role_arn,
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
                # Batch ec3 instance role
                'S3_DATA_SHARING_BATCH_INSTANCE_ROLE_NAME': self.s3_data_sharing_instance_role.role_name,
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
