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
                type=dynamodb.AttributeType.NUMBER,
            ),
            # NOTE(SW): set to retain in production
            removal_policy=core.RemovalPolicy.DESTROY,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST
        )

        ################################################################################
        # Batch

        vpc = ec2.Vpc.from_lookup(
            self,
            'MainVPC',
            tags={'Name': 'main-vpc', 'Stack': 'networking'},
        )

        # NOTE(SW): may want to restrict as ro with write perms to specific directory for
        # results write.
        # Would the following work or conflict?
        # AWS managed policy:
        #   iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3ReadOnlyAccess'),
        # Add policy actions:
        #   actions=['s3:PutBucketPolicy'],
        #   resources=[f'arn:aws:s3:::{props["staging_bucket"]}/{results_json_dir}']
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
        batch_instance_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'dynamodb:GetItem',
                ],
                resources=[dynamodb_table.table_arn]
            )
        )

        batch_instance_profile = iam.CfnInstanceProfile(
            self,
            'BatchInstanceProfile',
            roles=[batch_instance_role.role_name],
            instance_profile_name='agha-batch-instance-profile',
        )

        batch_spot_fleet_role = iam.Role(
            self,
            'BatchSpotFleetRole',
            assumed_by=iam.ServicePrincipal('spotfleet.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2SpotFleetTaggingRole'),
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
                device_name='/dev/xvda',
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
            version='$Latest',
        )

        batch_compute_environment = batch.ComputeEnvironment(
            self,
            'BatchComputeEnvironment',
            compute_environment_name='agha-file-validation-compute-environment',
            compute_resources=batch.ComputeResources(
                vpc=vpc,
                allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                desiredv_cpus=0,
                instance_role=batch_instance_profile.attr_arn,
                launch_template=batch_launch_template_spec,
                maxv_cpus=16,
                security_groups=[batch_security_group],
                spot_fleet_role=batch_spot_fleet_role,
                type=batch.ComputeResourceType.SPOT,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE),
            )
        )

        batch_job_queue = batch.JobQueue(
            self,
            'BatchJobQueue',
            job_queue_name=props['batch_queue_name'],
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
                command=['true'],
                memory_limit_mib=1000,
                vcpus=1,
            ),
        )

        ################################################################################
        # Lambda layers

        runtime_layer = lmbda.LayerVersion(
            self,
            'RuntimeLambdaLayer',
            code=lmbda.Code.from_asset('lambdas/layers/runtime/python38-runtime.zip'),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_8],
            description='A runtime layer for python 3.8'
        )

        shared_layer = lmbda.LayerVersion(
            self,
            'SharedLambdaLayer',
            code=lmbda.Code.from_asset('lambdas/layers/shared/python38-shared.zip'),
            compatible_runtimes=[lmbda.Runtime.PYTHON_3_8],
            description='A shared layer for python 3.8'
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
                    's3:GetBucketPolicy',
                    's3:PutBucketPolicy',
                    's3:DeleteBucketPolicy'
                ],
                resources=[f'arn:aws:s3:::{props["staging_bucket"]}']
            )
        )

        folder_lock_lambda = lmbda.Function(
            self,
            'FolderLockLambda',
            function_name=f'{props["namespace"]}_folder_lock_lambda',
            handler='folder_lock.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(10),
            code=lmbda.Code.from_asset('lambdas/folder_lock'),
            environment={
                'STAGING_BUCKET': props['staging_bucket']
            },
            role=folder_lock_lambda_role
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
                    'ses:SendEmail',
                    'ses:SendRawEmail',
                ],
                # NOTE(SW): resources related to identities i.e. email addresses. We could
                # construct ARNs using the manager and sender email defined in props.
                resources=['*']
            )
        )

        manifest_processor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'dynamodb:Query',
                    'dynamodb:PutItem',
                    'dynamodb:UpdateItem',
                ],
                resources=[dynamodb_table.table_arn]
            )
        )

        manifest_processor_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'batch:SubmitJob'
                ],
                resources=[
                    batch_job_queue.job_queue_arn,
                    batch_job_definition.job_definition_arn,
                ]
            )
        )

        manifest_processor_lambda = lmbda.Function(
            self,
            'ManifestProcessorLambda',
            function_name=f'{props["namespace"]}_manifest_processor_lambda',
            handler='manifest_processor.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(60),
            code=lmbda.Code.from_asset('lambdas/manifest_processor'),
            environment={
                'STAGING_BUCKET': props['staging_bucket'],
                'RESULTS_BUCKET': props['results_bucket'],
                'DYNAMODB_TABLE': props['dynamodb_table'],
                'JOB_DEFINITION_ARN': batch_job_definition.job_definition_arn,
                'FOLDER_LOCK_LAMBDA_ARN': folder_lock_lambda.function_arn,
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
        # Data import lambda

        data_import_lambda_role = iam.Role(
            self,
            'DataImportLambdaRole',
            assumed_by=iam.ServicePrincipal('lambda.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSSMReadOnlyAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3ReadOnlyAccess'),
            ]
        )

        data_import_lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'dynamodb:GetItem',
                    'dynamodb:PutItem',
                    'dynamodb:UpdateItem',
                ],
                resources=[dynamodb_table.table_arn]
            )
        )

        data_import_lambda = lmbda.Function(
            self,
            'DataImportLambda',
            function_name=f'{props["namespace"]}_data_import_lambda',
            handler='data_import.handler',
            runtime=lmbda.Runtime.PYTHON_3_8,
            timeout=core.Duration.seconds(60),
            code=lmbda.Code.from_asset('lambdas/data_import'),
            environment={
                'DYNAMODB_TABLE': props['dynamodb_table'],
                'SLACK_NOTIFY': props['slack_notify'],
                'EMAIL_NOTIFY': props['email_notify'],
                'SLACK_HOST': props['slack_host'],
                'SLACK_CHANNEL': props['slack_channel'],
                'MANAGER_EMAIL': props['manager_email'],
                'SENDER_EMAIL': props['sender_email'],
            },
            role=data_import_lambda_role,
        )
