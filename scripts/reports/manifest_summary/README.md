# Generate manifest summary script

This script will generate **manifest** summary inside the AGHA GDR store bucket. It will generate 2 files:
- `{key}_MANIFEST_{generated_date}.txt` - A brief statistic about study_id and files associated with it.
- `{key}_FILECOUNT_{generated_date}.tsv` - A tsv format file with study_id and filetype counts.

Parameter needed for the script to run:
- `--sort-key-prefix` - Matching s3key prefix with the current in store bucket.

Example of executing the script:

```bash
python main.py --sort-key-prefix NMD/
```

Data will be generated at the same directory of this file with a post fix with the datestamp.

Example of file produced from the script.

MANIFEST report file:
```
Manifest report for 'agha-gdr-store-2.0' bucket with 'NMD/2022-02-22/' prefix on 20220222_000000.

Study Id Count: 1
Study Id list: ["A0000001"]

Details
################################################
A0000001

    Number of files per types:

    TOTAL: 2
    FASTQ: 2

    Filename and checksums:

                filename                provided_checksum
    FILENAME_R1.fastq.gz c6779ec2960296ed9a04f08d67f64422
    FILENAME_R2.fastq.gz c6779ec2960296ed9a04f08d67f64423
```

FILECOUNT report file:
```
agha_study_id	TOTAL	BAM	BAM_INDEX	FASTQ	VCF	VCF_INDEX	CRAM	CRAM_INDEX
A0000001	1	0	0	1	0	0	0	0
A0000002	2	1	1	0	0	0	0	0
A0000003	2	0	0	2	0	0	0	0
```

## The setup before executing the script

Prerequisite:
- `aws-cli` available with least version 2
- `python3` available with at least version 3.8

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/reports/manifest_summary
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
