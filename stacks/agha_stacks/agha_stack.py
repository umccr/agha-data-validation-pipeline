from aws_cdk import (
    core
)

from stacks.agha_stacks.dynamodb_stack import DynamoDBStack
from stacks.agha_stacks.lambda_stack import LambdaStack
from stacks.agha_stacks.batch_stack import BatchStack

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


        