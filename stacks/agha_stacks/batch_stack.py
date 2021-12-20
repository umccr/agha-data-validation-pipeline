from aws_cdk import (
    core,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_ecr as ecr
)


class BatchStack(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # Batch properties
        batch_environment = self.node.try_get_context("batch_environment")

        ################################################################################
        # Batch

        vpc = ec2.Vpc.from_lookup(
            self,
            'MainVPC',
            vpc_id=batch_environment['vpc_id'],
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
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonDynamoDBFullAccess')
            ]
        )

        batch_instance_profile = iam.CfnInstanceProfile(
            self,
            'BatchInstanceProfile',
            roles=[batch_instance_role.role_name],
            instance_profile_name='agha-batch-instance-profile-2.0',
        )

        batch_spot_fleet_role = iam.Role(
            self,
            'BatchSpotFleetRole',
            assumed_by=iam.ServicePrincipal('spotfleet.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2SpotFleetTaggingRole'),
            ]
        )

        batch_security_group = ec2.SecurityGroup(
            self,
            'SecruityGroupOutBoundOnly',
            vpc=vpc,
            description="Defined outbound only traffic for AGHA validation pipeline batch job",
            allow_all_outbound=True,
            security_group_name="AGHA validation pipeline"
        )

        block_device_mappings = [
            ec2.CfnLaunchTemplate.BlockDeviceMappingProperty(
                device_name='/dev/xvda',
                ebs=ec2.CfnLaunchTemplate.EbsProperty(
                    encrypted=True,
                    volume_size=500,
                    volume_type='gp2'
                )
            ),
        ]

        batch_launch_template = ec2.CfnLaunchTemplate(
            self,
            'BatchLaunchTemplate',
            launch_template_name='agha-validation-launch-template',
            launch_template_data=ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
                block_device_mappings=block_device_mappings,
            ),
        )

        batch_launch_template_spec = batch.LaunchTemplateSpecification(
            launch_template_name=batch_launch_template.launch_template_name,
            version='$Latest',
        )

        instance_types = [
            'r4.large',
            'r5.large',
            'r5a.large',
            'r5d.large',
            'r5n.large',
        ]

        batch_compute_environment = batch.ComputeEnvironment(
            self,
            'BatchComputeEnvironment',
            compute_environment_name=batch_environment['compute_environment_name'],
            compute_resources=batch.ComputeResources(
                vpc=vpc,
                allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                desiredv_cpus=0,
                instance_role=batch_instance_profile.attr_arn,
                instance_types=[ec2.InstanceType(it) for it in instance_types],
                launch_template=batch_launch_template_spec,
                maxv_cpus=64,
                security_groups=[batch_security_group],
                spot_fleet_role=batch_spot_fleet_role,
                type=batch.ComputeResourceType.SPOT,
            )
        )

        self.batch_job_queue = batch.JobQueue(
            self,
            'BatchJobQueue',
            job_queue_name=batch_environment['batch_queue_name'],
            compute_environments=[
                batch.JobQueueComputeEnvironment(
                    compute_environment=batch_compute_environment,
                    order=1
                )
            ]
        )

        self.batch_job_definition = batch.JobDefinition(
            self,
            'BatchJobDefinition',
            job_definition_name=batch_environment['file_validation_job_definition_name'],
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_ecr_repository(
                    repository=ecr.Repository.from_repository_name(
                        self,
                        "FileValidationRepository",
                        repository_name=batch_environment["file_validation_ecr"]["name"]
                    ),
                    tag=batch_environment["file_validation_ecr"]["tag"]
                ),
                command=['true'],
                memory_limit_mib=1000,
                vcpus=1,
            ),
        )


        ################################################################################
        # Batch for move_s3_object

        self.batch_s3_job_definition = batch.JobDefinition(
            self,
            'S3BatchJobDefinition',
            job_definition_name=batch_environment['s3_job_definition_name'],
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_registry('amazon/aws-cli:latest'),
                command=['true'],
                memory_limit_mib=1000,
                vcpus=1,
            ),
        )
