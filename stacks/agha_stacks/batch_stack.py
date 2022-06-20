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
            vpc_id=batch_environment['vpc_id']
        )

        # NOTE(SW): may want to restrict as ro with write perms to specific directory for
        # results write.
        # Would the following work or conflict?
        # AWS managed policy:
        #   iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3ReadOnlyAccess'),
        # Add policy actions:
        #   actions=['s3:PutBucketPolicy'],
        #   resources=[f'arn:aws:s3:::{props["staging_bucket"]}/{results_json_dir}']

        instance_types_2_vcpu = [
            'm4.large',
            'm5.large'
        ]

        compute_environment_spec_list = [
            {
                'type': 'small',
                'queue_name': batch_environment['batch_queue_name']['small'],
                'compute_environment_name': batch_environment['compute_environment_name']['small'],
                'instance_type': instance_types_2_vcpu,
                'ebs_storage_size': 60
            },
            {
                'type': 'medium',
                'queue_name': batch_environment['batch_queue_name']['medium'],
                'compute_environment_name': batch_environment['compute_environment_name']['medium'],
                'instance_type': instance_types_2_vcpu,
                'ebs_storage_size': 250
            },
            {
                'type': 'large',
                'queue_name': batch_environment['batch_queue_name']['large'],
                'compute_environment_name': batch_environment['compute_environment_name']['large'],
                'instance_type': instance_types_2_vcpu,
                'ebs_storage_size': 350
            },
            {
                'type': 'xlarge',
                'queue_name': batch_environment['batch_queue_name']['xlarge'],
                'compute_environment_name': batch_environment['compute_environment_name']['xlarge'],
                'instance_type': instance_types_2_vcpu,
                'ebs_storage_size': 500
            },
        ]

        self.batch_instance_role = iam.Role(
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

        self.batch_instance_profile = iam.CfnInstanceProfile(
            self,
            'BatchInstanceProfile',
            roles=[self.batch_instance_role.role_name],
            instance_profile_name='agha-batch-instance-profile-2.0',
        )

        self.batch_spot_fleet_role = iam.Role(
            self,
            'BatchSpotFleetRole',
            assumed_by=iam.ServicePrincipal('spotfleet.amazonaws.com'),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AmazonEC2SpotFleetTaggingRole'),
            ]
        )

        self.batch_security_group = ec2.SecurityGroup(
            self,
            'SecruityGroupOutBoundOnly',
            vpc=vpc,
            description="Defined outbound only traffic for AGHA validation pipeline batch job",
            allow_all_outbound=True,
            security_group_name="AGHA validation pipeline"
        )

        batch_queue_dict = {}
        batch_compute_environment_dict = {}

        # Defining ebs storage
        for compute_environment_spec in compute_environment_spec_list:
            block_device_mappings = [
                ec2.CfnLaunchTemplate.BlockDeviceMappingProperty(
                    device_name='/dev/xvda',
                    ebs=ec2.CfnLaunchTemplate.EbsProperty(
                        encrypted=True,
                        volume_size=compute_environment_spec['ebs_storage_size'],
                        volume_type='gp2'
                    )
                ),
            ]

            batch_launch_template = ec2.CfnLaunchTemplate(
                self,
                f"BatchLaunchTemplate_{compute_environment_spec['type']}",
                launch_template_name=f"agha-validation-launch-template{compute_environment_spec['type']}",
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
                f"BatchComputeEnvironment_{compute_environment_spec['compute_environment_name']}",
                compute_environment_name=compute_environment_spec['compute_environment_name'],
                compute_resources=batch.ComputeResources(
                    vpc=vpc,
                    allocation_strategy=batch.AllocationStrategy.SPOT_CAPACITY_OPTIMIZED,
                    desiredv_cpus=0,
                    instance_role=self.batch_instance_profile.attr_arn,
                    instance_types=[ec2.InstanceType(it) for it in compute_environment_spec['instance_type']],
                    launch_template=batch_launch_template_spec,
                    maxv_cpus=32,
                    security_groups=[self.batch_security_group],
                    spot_fleet_role=self.batch_spot_fleet_role,
                    type=batch.ComputeResourceType.SPOT,
                )
            )

            batch_job_queue = batch.JobQueue(
                self,
                f"BatchJobQueue_{compute_environment_spec['queue_name']}",
                job_queue_name=compute_environment_spec['queue_name'],
                compute_environments=[
                    batch.JobQueueComputeEnvironment(
                        compute_environment=batch_compute_environment,
                        order=1
                    )
                ]
            )

            batch_queue_dict[compute_environment_spec['queue_name']] = batch_job_queue
            batch_compute_environment_dict[compute_environment_spec['type']] = batch_compute_environment

        self.batch_job_queue = batch_queue_dict
        self.batch_compute_environment = batch_compute_environment_dict

        ################################################################################################################
        # Definition for batch job

        # Batch job for file validation
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
                memory_limit_mib=4000,
                vcpus=2,
            ),
            retry_attempts=2
        )

        # Batch job definition for s3 mv
        self.batch_s3_job_definition = batch.JobDefinition(
            self,
            'S3BatchJobDefinition',
            job_definition_name=batch_environment['s3_job_definition_name'],
            container=batch.JobDefinitionContainer(
                image=ecs.ContainerImage.from_registry('amazon/aws-cli:latest'),
                command=['true'],
                memory_limit_mib=2000,
                vcpus=1,
            ),
            retry_attempts=2
        )
