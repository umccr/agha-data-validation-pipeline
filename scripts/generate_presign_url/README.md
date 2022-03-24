# Generate Presign URL

This script is to be used to create pre-signed url based on the below parameter. Pre-signed url will live for 7 days.

Parameter needed to run this script.
- `agha_study_id_list`: [`str`] - list of study_id
- `flagship`: str - the flagship preferred code. [Check on the FlagShip class](../../lambdas/layers/util/util/agha.py#L9)
- `filetype_list`: str - Enum class for the action type (Options: VCF, BAM, FASTQ, CRAM). Default: all filetypes. [Check on the FileType class](../../lambdas/layers/util/util/agha.py#L65)
- `dry_run`: `bool` - only print the sort key associated with the above query

The value could be populated at `get_argument()` ([main.py](main.py#L40-L50) Line 40) 

After done filling the information, execute the script with:
```
python3 main.py
```

NOTE: Make sure use correct AWS_PROFILE and have the permission to live for 7 days.
