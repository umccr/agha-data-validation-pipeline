from aws_cdk import (
    core
)

from stacks.agha_stacks.lambda_stack import LambdaStack
from stacks.agha_stacks.batch_stack import BatchStack
from stacks.agha_stacks.s3_data_sharing_stack import S3DataSharing

class AghaStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

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

        # S3 Data Sharing
        data_sharing_stack = S3DataSharing(
            self,
            'DataSharingStack',
            batch_stack=batch
        )

        