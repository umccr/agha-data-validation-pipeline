# Generate Presign URL

This script is to be used to create pre-signed url based on the below parameter. Pre-signed url will live for 7 days.

###### Parameter

Parameter passed as an arguments in calling the python scripts. Parameter are as follows.

- `--study-ids`: `str` - agha_study_id to be queried. Space seperated for multiple ids.
- `--flagship`: `str` - The flagship preferred code. [Check on the FlagShip class](../../lambdas/layers/util/util/agha.py#L9)
- `--filetype`: `str` - The filetype that the script will find. Space seperated for multiple ids. (Options: VCF, BAM, FASTQ, CRAM). Default: all filetypes.
- `--dryrun`: `bool` - only print the s3 key associated with the above query.
- `--release-id`: `str` - A unique ID to identify the sharing request. This ID will be inserted as a custom parameter in the presignedUrl for access tracking.

To execute the script, just call main.py and include the parameter.

```
python3 main.py [PARAMETER HERE]
```

NOTE: Make sure use correct AWS credentials that valid up to 7 days.

###### Command example for the following parameter

study_id    : A00001, A00002, A00003
flagship    : GI
filetype    : BAM, FASTQ
release_id  : USER007

```
python3 main.py --study-ids A00001 A00002 A00003 --flagship GI --filetype BAM FASTQ --release-id USER007
```

## The setup before executing the script

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/generate_presign_url
    ```
3. Setup an virtual environment and install packages
   ```
   python3 -mvenv .venv
   source .venv/bin/activate  # This might be different for non-unix shell
   pip install -r requirements.txt
   ```
4. Setup AWS_PROFILE. This aws profile that is valid for up to 7 days.
   ```
   export AWS_PROFILE=agha
   ```

   or

   ```
   aws configure
   ```
