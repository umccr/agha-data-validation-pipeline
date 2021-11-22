from aws_cdk import (
    aws_batch as batch,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lmbda,
    aws_s3 as s3,
    core,
    aws_ssm as ssm
)

from .agha_stacks.dynamodb_stack import DynamoDBStack
from .agha_stacks.lambda_stack import LambdaStack
from .agha_stacks.batch_stack import BatchStack

class AghaStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # DynamoDB
        dynamodb_table = DynamoDBStack(
            self,
            'DynamoDBStack',
        )

        # Batch
        batch = BatchStack(
            self,
            'BatchStack',
        )
        
        # Lambdas
        lambdas = LambdaStack(
            self,
            'LambdaStack',
            batch=batch
        )


        