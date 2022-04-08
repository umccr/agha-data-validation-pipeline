# Consent Tagging Script

This script is to be used for any new consent request. This script will:
- Add tagging to S3 object `Consent`:`True` 
- Update Dynamodb regards on Consent status of the object

Parameter needed to run this script.
- `--study-ids`: `str` - agha_study_id to be queried. Space seperated for multiple ids.
- `--flagship`: `str` - The flagship preferred code. [Check on the FlagShip class](../../lambdas/layers/util/util/agha.py#L9)
- `--action`: `str` - Action to add/remove consent (Options: ADD_CONSENT, REMOVE_CONSENT).
- `--dryrun`: `bool` - to print the affected files on s3 without modifying anything. Add `--dryrun` to set this to True

The value could be populated at `get_argument()` (`main.py` Line 55) 

Alternatively, could execute script from custom function. See the `parse_from_excel_by_pandas()` at main.py.
This reads data from excel workbook and call the script based on the Excel workbook.

To execute the script, just call main.py and include the parameter.

```
python3 main.py [PARAMETER HERE]
```

NOTE: Make sure use correct AWS credentials that have the permission to modify S3 object tag and update Dynamodb tables.

###### Command example for the following parameter

study_id : A00001, A00002, A00003  
flagship : GI  
action   : ADD_CONSENT
```
python3 main.py --study-ids A00001 A00002 A00003 --flagship GI --filetype BAM FASTQ
```


## The setup before executing the script

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/consent_tagging
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
