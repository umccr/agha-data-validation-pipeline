from aws_cdk import (
    aws_codepipeline as codepipeline,
    core as cdk,
    aws_s3 as s3,
    aws_ssm as ssm,
    aws_codepipeline_actions as codepipeline_actions,
    aws_codebuild as codebuild,
    aws_ecr as ecr,
    pipelines as pipelines,
)
from stacks.agha_stacks.agha_stack import AghaStack

class AGHAValidationPipelineStage(cdk.Stage):
    def __init__(self, scope: cdk.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        namespace = self.node.try_get_context("namespace")
        aws_env = self.node.try_get_context("aws_env")

        # Create stack defined on stacks folder
        AghaStack(
            self,
            namespace,
            tags={
                'Stack': namespace,
                'Creator': f"cdk-codepipeline-{namespace}",
            },
            env=aws_env
        )


class CodePipelineStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        pipeline_props = self.node.try_get_context("pipeline")
        batch_environment = self.node.try_get_context("batch_environment")

        ################################################################################
        # Basic Pipeline construct

        # Create artifact
        github_source_output = codepipeline.Artifact()
        ecr_source_output = codepipeline.Artifact()

        # Create S3 bucket for artifacts
        pipeline_artifact_bucket = s3.Bucket(
            self,
            "PipelineArtifactBucket",
            bucket_name = pipeline_props["artifact_bucket_name"],
            auto_delete_objects = True,
            removal_policy = cdk.RemovalPolicy.DESTROY,
            block_public_access= s3.BlockPublicAccess.BLOCK_ALL
        )

        # Create a pipeline for agha self-mutate
        agha_validation_build_pipeline = codepipeline.Pipeline(
            self,
            "AGHAValidationAppCodePipeline",
            artifact_bucket=pipeline_artifact_bucket,
            restart_execution_on_update=True,
            cross_account_keys=False,
            pipeline_name=pipeline_props["pipeline_name"],
        )

        ################################################################################
        # Add Source Stage to pipeline

        codestar_arn = ssm.StringParameter.from_string_parameter_attributes(self, "codestarArn",
            parameter_name="codestar_github_arn"
        ).string_value

        github_source_action = codepipeline_actions.CodeStarConnectionsSourceAction(
            action_name="GitHub_Source",
            owner='umccr',
            repo=pipeline_props['repository_name'],
            connection_arn=codestar_arn,
            branch=pipeline_props['branch_name'],
            output=github_source_output
        )

        ecr_source_action = codepipeline_actions.EcrSourceAction(
            action_name="ECR_Source",
            output=ecr_source_output,
            repository=ecr.Repository.from_repository_name(
                self,
                "FileValidationRepository",
                repository_name=batch_environment["file_validation_ecr"]["name"],
            ),
            image_tag=batch_environment["file_validation_ecr"]["tag"]
        )

        agha_validation_build_pipeline.add_stage(
            stage_name='GitHub_Source_Stage',
            actions=[github_source_action, ecr_source_action],
        )

        ################################################################################
        # CDK self mutating pipeline

        code_pipeline_file_set = pipelines.CodePipelineFileSet.from_artifact(github_source_output)

        self_mutate_pipeline = pipelines.CodePipeline(
            self,
            "CodePipeline",
            code_pipeline=agha_validation_build_pipeline,
            synth=pipelines.ShellStep(
                "CDKShellScript",
                input=code_pipeline_file_set,
                commands=[
                    "for dir in $(find ./lambdas/layers/ -maxdepth 1 -mindepth 1 -type d);"
                    "do /bin/bash ./build_lambda_layers.sh ${dir}; done",

                    # CDK lint test
                    "cdk synth",
                    "mkdir ./cfnnag_output",
                    "for template in $(find ./cdk.out -type f -maxdepth 2 -name '*.template.json');"
                    "do cp $template ./cfnnag_output; done",
                    "cfn_nag_scan --input-path ./cfnnag_output",

                ],
                install_commands=[
                    "npm install -g aws-cdk",
                    "gem install cfn-nag",
                    "pip install -r requirements.txt",
                    "docker -v"
                ],
                primary_output_directory="cdk.out"
            ),
            code_build_defaults=pipelines.CodeBuildOptions(
                build_environment=codebuild.BuildEnvironment(
                    build_image=codebuild.LinuxBuildImage.STANDARD_5_0,
                    privileged=True
                )
            )
        )

        # Deploy infrastructure
        self_mutate_pipeline.add_stage(
            stage=AGHAValidationPipelineStage(
                self,
                "AGHAValidationPipelineStage"
            )
        )



