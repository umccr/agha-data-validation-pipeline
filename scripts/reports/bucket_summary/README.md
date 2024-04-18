# Generate bucket summary script

This script will generate summary statistic for content inside the AGHA GDR store bucket.

Parameter needed for the script to run:
- `--consent` - Specify if statistic is only based on consented data

Example of executing the script

```bash
python main.py --consent # Remove '--consent' flag to generate based on all files in the bucket
```

Data will be generated at the same directory of this file with a post fix with the datestamp.

Example of file produced from the script.

```

General statistic report for 'agha-gdr-store-2.0' bucket at date 20220408.


################################################################################
Statistic Entire Bucket
################################################################################
        File count per file type.

        Total number of BAM: 10
        Total number of BAM_INDEX: 10
        Total number of FASTQ: 10
        Total number of MANIFEST: 10
        Total number of UNSUPPORTED: 10
        Total number of VCF: 10
        Total number of VCF_INDEX: 10

    ================================================================================

        Total file size in bytes per file type.

        Total file size of BAM (in bytes): 10000
        Total file size of BAM_INDEX (in bytes): 10000
        Total file size of FASTQ (in bytes): 10000
        Total file size of MANIFEST (in bytes): 10000
        Total file size of UNSUPPORTED (in bytes): 10000
        Total file size of VCF (in bytes): 10000
        Total file size of VCF_INDEX (in bytes): 10000


################################################################################
Checking FlagShip XXX (xxx)
################################################################################
        File count per file type.

        Total number of BAM: 10
        Total number of BAM_INDEX: 10
        Total number of FASTQ: 10
        Total number of MANIFEST: 10
        Total number of UNSUPPORTED: 10
        Total number of VCF: 10
        Total number of VCF_INDEX: 10

    ================================================================================

        Total file size in bytes per file type.

        Total file size of BAM (in bytes): 10000
        Total file size of BAM_INDEX (in bytes): 10000
        Total file size of FASTQ (in bytes): 10000
        Total file size of MANIFEST (in bytes): 10000
        Total file size of UNSUPPORTED (in bytes): 10000
        Total file size of VCF (in bytes): 10000
        Total file size of VCF_INDEX (in bytes): 10000



```


## The setup before executing the script

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/reports/bucket_summary
    ```
3. Setup an virtual environment and install packages
   ```
   python3 -mvenv .venv
   source .venv/bin/activate  # This might be different for non-unix shell
   pip install -r requirements.txt
   ```
4. Setup AWS_PROFILE. This aws profile must have the right permission to tag S3 object and update Dynamodb
   ```
   export AWS_PROFILE=agha
   ```

   or

   ```
   aws configure
   ```
