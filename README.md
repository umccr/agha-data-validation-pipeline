# AGHA data validation stack
This stack is used to handle and validate data received as part of the AGHA GDR project. The primary functions are:
1. take receipt of genomic files in a staging area
2. validate genomic files, and generate indexes if required
3. store file validation results in a database
4. move genomic files to a data store along with indexes _[planned]_
5. generate validation reports _[planned]_

## Table of contents
* [Schematic](#schematic)
* [Prerequisites](#prerequisites)
* [Deployment](#deployment)
* [Usage](#usage)
* [Lambda arguments](#lambda-arguments)
* [Accessory scripts](#accessory-scripts)

## Schematic
<p align="center"><img src="images/schematic.png" width="80%"></p>

## Prerequisites
It is assumed that the necessary VPC, security groups, and S3 buckets are appropriately deployed and configured in the target
AWS account.

## Deployment
The stack has some software requirements for deploy:
* AWS CDK Toolkit (available through Homebrew or npm)
* Docker
* Python3

### Create virtual environment
```bash
python3 -m venv .venv/
pip install -r requirements.txt
```

### Configure
The cdk application will mostly configure the infrastructure as shown in the picture. Just make sure constants props 
defined in app.py are correct.

### Deploy stack
The stack contain a pipeline will take source code from the repository and self-update the pipeline when new 
code commit is detected to this repository. Initialize setup of pipeline is necessary and only be done once.

```bash
cdk deploy
```
_Make sure to setup aws profile by `export AWS_PROFILE` or `--porfile=${PROFIE_NAME}`_


## Usage
### Automatic triggering (manifest file uploaded)
Incoming data should be uploaded under an accepted flagship and date/time stamp directory e.g. `./CARDIAC/20210825/`. The
`manifest.txt` file is expected to be uploaded last to enable downstream validation process when it is enabeld.
This Lambda function when manifest is uploaded:
* Add bucket policy to read-only
* Validate manifest data structure (e.g. correct format, no black data)
* Create records in DynamoDB for file properties along with manifest data for ease of access
* Run validate validate file batch job (if automation is enabled)

The validation results, logs, and indexes are uploaded to the results S3 bucket using a key prefix matching the input
`manifest.txt` and with run directory e.g. `./CARDIAC/20210825/<rundate>_<runtime>_<uid>/`.

### Manual triggering

#### Manual file validation
Files can be resubmitted for validation manually via the File Process Lambda function. Through the manual entry point, the
Lambda function will accept either a list of file paths to process or a manifest file path. When providing a manifest file
path, behaviour is intended to mirror automatic process of the S3 event with the option to specific a list of files to
include or exclude. Conversely, using file paths is for cases where you want to generate indices, checksums, or validate
file type for files that may not be associated with a project, and doing so requires you to also set an output prefix.

The required set of tasks will be automatically determined but this can be overridden with the `tasks` option. When a
DynamoDB record already exists for an input file, the default behaviour is to create a new record. This change by changed to
update the existing record with `record_mode` however, only a partial update of fields is performed and probably needs more
work to be useful.

We currently assume that all input data is in the staging bucket, and output data is always written to the results bucket.

#### Invoke: manifest path
```bash
aws lambda invoke \
    --function-name agha-gdr-validation-pipeline_validation_manager_lambda \
    --cli-binary-format raw-in-base64-out \
    --payload '{
      "manifest_fp": "cardiac/20210711_170230/manifest.txt",
      "include_fns": [
        "19W001053.bam",
        "19W001053.individual.norm.vcf.gz",
        "19W001056.bam",
        "19W001056.individual.norm.vcf.gz"
      ]
    }' \
    response.json
```

#### Invoke: file paths
```bash
aws lambda invoke \
    --function-name agha-gdr-validation-pipeline_validation_manager_lambda \
    --cli-binary-format raw-in-base64-out \
    --payload '{
      "filepaths": [
        "cardiac/20210711_170230/20210824_051333_0755318/19W001053.bam__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001053.individual.norm.vcf.gz__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001056.bam__results.json",
        "cardiac/20210711_170230/20210824_051333_0755318/19W001056.individual.norm.vcf.gz__results.json"
      ],
      "output_prefix": "cardiac/20210711_170230/20210824_manual_run/"
    }' \
    response.json
```
Payload Arguments:
| Argument          | Description                                                                                                    |
| ---               | ---                                                                                                            |
| `manifest_fp`     | S3 key for an input manifest file. Cannot be used with `filepaths` or `output_prefix`.                         |
| `filepaths`       | List of S3 keys of input files. Requires `output_prefix`, cannot be used with `manifest_fp`.                   |
| `output_prefix`   | Output S3 key prefix when using `filepaths`, cannot be used with `manifest_fp`.                                |
| `include_fns`     | List of *filenames* to include for validation from `manifest_fp`, all other files excluded.                    |
| `exclude_fns`     | List of *filenames* to exclude for validation form `manifest_fp`, all other file included.                     |
| `record_mode`     | Operation used add result data to database: can be either `create` or `update` [_default:_ `create`].          |
| `email_address`   | Email of submitter (_optional_).                                                                               |
| `email_name`      | Name of submitter (_optional_).                                                                                |
| `tasks`           | List of tasks to run (_optional_). Tasks: `checksum`, `validate_filetype`, `create_index`.                     |
| `strict_mode`     | Run in strict mode: fail on unexpected files, flagship, study ID. Choices `True`, `False` [_default:_ `True`]. |


#### Data Transfer Manager lambda
From a given flagship code and submission directory lambda would be able to trigger batch job to move from staging
bucket to store bucket. 

```bash
aws lambda invoke \
    --function-name agha-gdr-validation-pipeline_data_transfer_manager_lambda \
    --cli-binary-format raw-in-base64-out \
    --payload '{
        submission: "13023_3432423",
        flagship_code: "ACG"
    }' \
    response.json
```
## More detail
For time being, more detail can be seen at 
[AGHA validation pipeline update Pull Request](https://github.com/umccr/agha-data-validation-pipeline/pull/2)




## Accessory scripts
### `query_database.py`
The script provides functionality to perform useful database queries. Query space can be limited by submission name or S3
prefix.

#### Record query
| Query type                            | Description                                          |
| ---                                   | ---                                                  |
| `no_task_run`                         | No tasks run                                         |
| `any_task_run`                        | ≥1 tasks run                                         |
| `tasks_incompleted`                   | ≥1 runnable tasks incomplete                         |
| `tasks_completed`                     | All runnable tasks complete                          |
| `tasks_completed_not_fully_validated` | All tasks run (incl. non-runnable) with ≥1 failed    |
| `fully_validated`                     | All tasks successfully complete (incl. non-runnable) |

#### File query
| Query type   | Description                                                |
| ---          | ---                                                        |
| `has_record` | Files *with* any database record                           |
| `no_record`  | Files *without* any database record                        |

### `dump_database.py`
Simply dump the given DynamoDB table to disk.
