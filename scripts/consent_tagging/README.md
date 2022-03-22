# Consent Tagging Script

This script is to be used for any new consent request. This script will:
- Add tagging to S3 object `Consent`:`True` 
- Update Dynamodb regards on Consent status of the object

Parameter needed to run this script.
- `agha_study_id_list`: [`str`] - list of study_id
- `flagship`: `str` - the flagship associated with the study_id
- `dry_run`: `bool` - to print the affected files on s3 without modifying anything.  

The value could be populated at `get_argument()` (`main.py` Line 38) 

Alternatively, could execute script from custom function. See the `parse_from_excel_by_pandas()` at main.py.
This reads data from excel workbook and call the script based on the Excel workbook.

After done filling the information, execute the script with:
```
python3 main.py
```

NOTE: Make sure AWS_PROFILE is set before running the script.