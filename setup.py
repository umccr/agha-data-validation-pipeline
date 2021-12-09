import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="agha",
    version="0.0.1",

    description="An CDK Python app to setup AGHA resources",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="author",

    package_dir={"": "stacks"},
    packages=setuptools.find_packages(where="stacks"),

    install_requires=[
        "aws-cdk.aws_codepipeline",
        "aws-cdk.aws_codepipeline_actions",
        "aws-cdk.aws_batch",
        "aws-cdk.aws_dynamodb",
        "aws-cdk.aws_ec2",
        "aws-cdk.aws_ecs",
        "aws-cdk.aws_iam",
        "aws-cdk.aws_lambda",
        "aws-cdk.aws_s3",
        "aws-cdk.aws_s3_notifications",
        "aws-cdk.aws_sns",
        "aws-cdk.aws_sns_subscriptions",
        "aws-cdk.aws_sqs",
        "aws-cdk.core",
        "aws-cdk.pipelines",
        "boto3"
    ],


    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
