from aws_cdk import (
    aws_batch as batch,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lmbda,
    aws_s3 as s3,
    core
)


class AghaStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, props: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        ################################################################################
        # S3 Buckets
        # NOTE: CDK does currently not support event notification setup on imported/existing S3 buckets.
        #       The S3 event notification setup will have to be handled in Terraform as long as TF is controlling
        #       the S3 buckets.

        staging_bucket = s3.Bucket.from_bucket_name(
            self,
            id="GdrStagingBucket",
            bucket_name=props['staging_bucket']
        )
        store_bucket = s3.Bucket.from_bucket_name(
            self,
            id="GdrStoreBucket",
            bucket_name=props['store_bucket']
        )

        ################################################################################
        # DynamoDB

        dynamodb_table = dynamodb.Table(
            self,
            'DynamoDBTable',
            table_name='agha-file-validation',
            partition_key=dynamodb.Attribute(
                name='partition_key',
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name='sort_key',
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Batch

        vpc = ec2.Vpc.from_lookup(
            self,
            'MainVPC',
            tags={'Name': 'main-vpc', 'Stack': 'networking'},
        )

        machine_image = ec2.MachineImage.latest_amazon_linux(
            cpu_type=ec2.AmazonLinuxCpuType.X86_64,
            edition=ec2.AmazonLinuxEdition.STANDARD,
            generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2,
            storage=ec2.AmazonLinuxStorage.GENERAL_PURPOSE,
            virtualization=ec2.AmazonLinuxVirt.HVM,
        )

        # NOTE(SW): may want to restrict as ro with write perms to specific directory for
        # emergency results write.
        # Would the following work or conflict?
        # AWS managed policy:
        #   iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3ReadOnlyAccess'),
        # Add policy actions:
        #   actions=['s3:PutBucketPolicy'],
        #   resources=[f'arn:aws:s3:::{staging_bucket.bucket_name}/{results_json_dir}']
        batch_instance_role = iam.Role(
            self,
            'BatchInstanceRole',
            assumed_by=iam.ServicePrincipal('ec2.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSSMManagedInstanceCore'),
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2ContainerServiceforEC2Role'),
            ]
        )

        batch_instance_profile = iam.CfnInstanceProfile(
            self,
            'BatchInstanceProfile',
            roles=[batch_instance_role.role_name],
            instance_profile_name='BatchInstanceProfile',
        )

        batch_spot_fleet_role = iam.Role(
            self,
            'BatchSpotFleetRole',
            assumed_by=iam.ServicePrincipal('spotfleet.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2ContainerServiceforEC2Role'),
            ]
        )

        # NOTE(SW): this should be generalised and added as a config option
        batch_security_group = ec2.SecurityGroup.from_security_group_id(
            self,
            'SecruityGroupOutBoundOnly',
            'sg-0e4269cd9c7c1765a',
        )

        block_device_mappings = [
            ec2.CfnLaunchTemplate.BlockDeviceMappingProperty(
                device_name='/dev/xvdcz',
                ebs=ec2.CfnLaunchTemplate.EbsProperty(
                    encrypted=True,
                    volume_size=100,
                    volume_type='gp2'
                )
            ),
        ]

        batch_launch_template = ec2.CfnLaunchTemplate(
            self,
            'BatchLaunchTemplate',
            launch_template_name='agha-launch-template',
            launch_template_data=ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
                block_device_mappings=block_device_mappings,
            ),
        )

        batch_launch_template_spec = batch.LaunchTemplateSpecification(
            launch_template_name=batch_launch_template.launch_template_name,
        )

        batch_compute_environment = batch.ComputeEnvironment(
            self,
            'BatchComputeEnvironment',
            compute_environment_name='agha-file-validation-compute-environment',
            compute_resources=batch.ComputeResources(
                vpc=vpc,
                allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                desiredv_cpus=0,
                image=machine_image,
                instance_role=batch_instance_profile.ref,
                launch_template=batch_launch_template_spec,
                maxv_cpus=16,
                security_groups=[batch_security_group],
                spot_fleet_role=batch_spot_fleet_role,
                type=batch.ComputeResourceType.SPOT,
            )
        )

        job_queue = batch.JobQueue(
            self,
            'BatchJobQueue',
            job_queue_name=props['job_definition_name'],
            compute_environments=[
                batch.JobQueueComputeEnvironment(
                    compute_environment=batch_compute_environment,
                    order=1
                )
            ]
        )

        batch_job_definition = batch.JobDefinition(
            self,
            'BatchJobDefinition',
            job_definition_name=props['job_definition_name'],
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_registry(name=props['container_image']),
                command=['True'],
                memory_limit_mib=1000,
                vcpus=1,
            ),
        )

        ################################################################################
        # Lambda general

        runtime_layer = lmbda.LayerVersion(
            self,
            "RuntimeLambdaLayer",
            code=lmbda.Code.from_asset("lambdas/layers/runtime/python38-runtime.zip"),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_8],
            description="A runtime layer for python 3.8"
        )

        shared_layer = lmbda.LayerVersion(
            self,
            "SharedLambdaLayer",
            code=lmbda.Code.from_asset("lambdas/layers/shared/python38-shared.zip"),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_8],
            description="A shared layer for python 3.8"
        )

        ################################################################################
        # Manifest processor Lambda

        manifest_processor_lambda_role = iam.Role(
            self,
            'ManifestProcessorLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3ReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('IAMReadOnlyAccess')
            ]
        )
        manifest_processor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    # Email notification via SES
                    'ses:SendEmail',
                    'ses:SendRawEmail',
                    # Entry read/create
                    'dynamodb:Query',
                    'dynamodb:PutItem',
                    'dynamodb:UpdateItem'
                ],
                resources=['*']
            )
        )

        manifest_processor_lambda = lmbda.Function(
            self,
            'ManifestProcessorLambda',
            function_name=f"{props['namespace']}_manifest_processor_lambda",
            handler='manifest_processor.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(60),
            code=lmbda.Code.from_asset('lambdas/manifest_processor'),
            environment={
                'STAGING_BUCKET': staging_bucket.bucket_name,
                'DYNAMODB_TABLE': props['dynamodb_table'],
                'JOB_DEFINITION_NAME': props['job_definition_name'],
                'BATCH_QUEUE_NAME': props['batch_queue_name'],
                'SLACK_NOTIFY': props['slack_notify'],
                'EMAIL_NOTIFY': props['email_notify'],
                'SLACK_HOST': props['slack_host'],
                'SLACK_CHANNEL': props['slack_channel'],
                'MANAGER_EMAIL': props['manager_email'],
                'SENDER_EMAIL': props['sender_email'],
            },
            role=manifest_processor_lambda_role,
            layers=[
                runtime_layer,
                shared_layer,
            ]
        )

        ################################################################################
        # Folder lock Lambda

        folder_lock_lambda_role = iam.Role(
            self,
            'FolderLockLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
            ]
        )
        folder_lock_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetBucketPolicy",
                    "s3:PutBucketPolicy",
                    "s3:DeleteBucketPolicy"
                ],
                resources=[f"arn:aws:s3:::{staging_bucket.bucket_name}"]
            )
        )

        folder_lock_lambda = lmbda.Function(
            self,
            'FolderLockLambda',
            function_name=f"{props['namespace']}_folder_lock_lambda",
            handler='folder_lock.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lmbda.Code.from_asset('lambdas/folder_lock'),
            environment={
                'STAGING_BUCKET': staging_bucket.bucket_name
            },
            role=folder_lock_lambda_role
        )

        ################################################################################
        # S3 event router Lambda

        s3_event_router_lambda_role = iam.Role(
            self,
            'S3EventRouterLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole')
            ]
        )
        s3_event_router_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "lambda:InvokeFunction"
                ],
                resources=[
                    folder_lock_lambda.function_arn,
                    manifest_processor_lambda.function_arn,
                ]
            )
        )

        s3_event_router_lambda = lmbda.Function(
            self,
            'S3EventRouterLambda',
            function_name=f"{props['namespace']}_s3_event_router_lambda",
            handler='s3_event_router.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(20),
            code=lmbda.Code.from_asset('lambdas/s3_event_router'),
            environment={
                'STAGING_BUCKET': staging_bucket.bucket_name,
                'VALIDATION_LAMBDA_ARN': manifest_processor_lambda.function_arn,
                'FOLDER_LOCK_LAMBDA_ARN': folder_lock_lambda.function_arn,
            },
            role=s3_event_router_lambda_role
        )

        ################################################################################
        # SNS topic
        # Not needed, as we can directly route S3 events to Lambda.
        # May be useful to filter out unwanted events in the future.

        # sns_topic = sns.Topic(
        #     self,
        #     id="AghaS3EventTopic",
        #     topic_name="AghaS3EventTopic",
        #     display_name="AghaS3EventTopic"
        # )
        # sns_topic.add_subscription(subscription=sns_subs.LambdaSubscription(fn=s3_event_router_lambda))

