# Handle duplicate script

The main goal is to remove any duplicate within flagship

To execute the script you would need to update the necessary value at 'execute.sh' file. At the top of the script
there is a 'TODO' section needed for the change.
- BUCKET_LOCATION - enter the bucket name
- FLAGSHIP - Flagship Code in the s3 bucket
- UPDATE_MANIFEST_TXT - to indicate if you would want to update the manifest.txt file from dynamodb after the change

Additionally, please make sure AWS CLI and AWS_PROFILE is set properly.
Profile must have the permission to modify `S3` and `DynamoDb`.

After done filling the information, execute the script with:
```
./execute.sh
```