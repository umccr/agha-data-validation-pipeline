#!/usr/bin/env python3
import setuptools


setuptools.setup(
    name="util",
    version="0.0.1",
    description="shared utility code for AGHA Lambda functions",
    author="Stephen Watts",
    license="GPLv3",
    packages=setuptools.find_packages(),
    install_requires=[
        "boto3",
        "pytz",
        "pandas",
    ],
)
