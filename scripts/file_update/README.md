# File Update Script

This script is to be used when there are file data change request. This change could include:
- Data moved to a different flagship
- Data moved to a different submission directory
- AGHA Study ID change for the file
- Filename change

To execute the script you would need to update the necessary value at 'execute.sh' file. At the top of the script
there is a 'TODO' section needed for the change.
- BUCKET_LOCATION - enter the bucket name
- PARTITION_KEY - partition key for the change (most probably 'TYPE:MANIFEST')
- SOURCE_SORT_KEY - current s3_key
- TARGET_SORT_KEY - desired s3_key
- OVERRIDE_VALUE - this is where you would fill in any changes to manifest file. PLEASE make sure it is in the correct
json string format. E.g. if you would change agha_study_id, enter '{"agha_study_id":"abcde"}' as the value
- UPDATE_MANIFEST_TXT - to indicate if you would want to update the manifest.txt file from dynamodb after the change

Additionally, please make sure AWS CLI and AWS_PROFILE is set properly.
Profile must have the permission to modify `S3` and `DynamoDb`.

After done filling the information, execute the script with:
```
./execute.sh
```

___
### Explanation of each file

  
#### script.sh
This file is the backbone of the script and will run the following order:
1. Move manifest record file from current s3_key location to desired s3_key location. This includes updated information from the `override_value` arguments.
2. Move the file of the current to desired s3_key
3. When `UPDATE_MANIFEST_TXT` is set to `true`. This will update manifest file in both s3_key submission prefix.
4. This will move `__results.json` and `__log.txt` respectively at the results bucket if necessary.

#### manifest_txt_update.py
This will update manifest file at the submission prefix from s3_key argument given.

Args:
- s3_key


#### move_manifest_record.py
This will move the dynamodb manifest record from old to new file. 

Args:
- bucket_location
- partition_key
- old_sort_key
- new_sort_key 
- value_override

#### move_and_modify_result_file.py
This will move `__results.json` and `__log.txt` to the new location.
The script will modify the content of `__results.json` file to reflect the changes desired and re-upload the file to the desired file.

Args:
- source_s3_key
- target_s3_key