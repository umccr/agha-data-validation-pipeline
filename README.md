
# Welcome to your CDK Python project!

- [Welcome to your CDK Python project!](#welcome-to-your-cdk-python-project)
  - [Useful commands](#useful-commands)
  - [Stacks](#stacks)
    - [agha_stack](#agha_stack)

This is a CDK project for AGHA.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the .env
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .env
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .env/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .env\Scripts\activate.bat
```

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

## Stacks

### agha_stack
This stack contains a Lambda to run AGHA submission validations comparing the submitted `manifest.txt` file to the content of the corresponding S3 "folder".

#### Lambda layer requirement
This stack deploys Lambda layers to provide runtime code to the Lambda function. Consequently, the Lambda layers must be
built prior to stack deployment. This is done by running `build_lambda_layers.sh` on each Lambda layer directory in
`lambda/layers/`:
```bash
for dir in $(find ./lambdas/layers/ -maxdepth 1 -mindepth 1 -type d); do
  ./build_lambda_layers.sh ${dir};
done
```

#### Batch Docker image
A Dockerfile is provided to build an image that contains all necessary software for Batch job execution. And so, deployment
of this stacks additionally involves building the image and uploading it to a repository.

Configure
```bash
NAME=agha-validation-pipeline
VERSION=0.0.1
URI_LOCAL="${NAME}:${VERSION}"
# Docker Hub
HUB_PROVIDER_URL=docker.io/scwatts
HUB_URI_REMOTE="${HUB_PROVIDER_URL}/${NAME}:${VERSION}"
```

Build
```bash
docker build -t "${NAME}" -f assets/Dockerfile .
```

Upload
```bash
# Tag image with remote Docker Hub URI
docker tag "${NAME}" "${HUB_URI_REMOTE}"

# Configure Docker with DH credentials and upload
docker login
docker push "${HUB_URI_REMOTE}"

# Remove unencrypted credentials
rm /Users/stephen/.docker/config.json
```
