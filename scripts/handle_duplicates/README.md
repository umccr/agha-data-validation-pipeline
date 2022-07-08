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

## The setup before executing the script

1. Clone this repository

   ```
    git clone https://github.com/umccr/agha-data-validation-pipeline.git
   ```

2. Go to this directory

    ```
    cd scripts/handle_duplicates
    ```
3. Setup an virtual environment and install packages
   ```
   python3 -mvenv .venv
   source .venv/bin/activate  # This might be different for non-unix shell
   pip install -r requirements.txt
   ```
4. Setup AWS_PROFILE.
   ```
   export AWS_PROFILE=agha
   ```

   or

   ```
   aws configure
   ```
