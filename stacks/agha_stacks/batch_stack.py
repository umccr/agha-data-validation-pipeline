from aws_cdk import (
    aws_lambda as lambda_,
    core
)


class BatchStack(core.NestedStack):

    def __init__(self, scope: core.Construct, id: str, props: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)


        