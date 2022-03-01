#!/usr/bin/env bash

######################################################################################################
# TODO: Fill this this section for the migration
# TODO: Please also export your AWS credentials to AWS_PROFILE
export BUCKET_LOCATION="agha-gdr-store-2.0"
export PARTITION_KEY="TYPE:MANIFEST"
export TARGET_SORT_KEY="ABC/0123456789/SOME-FILE.vcf.gz"
export SOURCE_SORT_KEY="XYZ/987654321/SOME-FILE.vcf.gz"
export OVERRIDE_VALUE='{"agha_study_id":"abcde"}'  # Make sure it is a JSON string object

# Update manifest.txt file in submission level by toggling 'UPDATE_MANIFEST_TXT' variable
# Ideally you would only trigger this for the last move file in the submission

export UPDATE_MANIFEST_TXT=true  # Boolean value (lower case): true, false

######################################################################################################

# Setting environment variables
export DYNAMODB_ARCHIVE_RESULT_TABLE_NAME='agha-gdr-result-bucket-archive'
export DYNAMODB_ARCHIVE_STAGING_TABLE_NAME='agha-gdr-staging-bucket-archive'
export DYNAMODB_ARCHIVE_STORE_TABLE_NAME='agha-gdr-store-bucket-archive'
export DYNAMODB_ETAG_TABLE_NAME='agha-gdr-e-tag'
export DYNAMODB_RESULT_TABLE_NAME='agha-gdr-result-bucket'
export DYNAMODB_STAGING_TABLE_NAME='agha-gdr-staging-bucket'
export DYNAMODB_STORE_TABLE_NAME='agha-gdr-store-bucket'
export STAGING_BUCKET='agha-gdr-staging-2.0'
export RESULT_BUCKET='agha-gdr-results-2.0'
export STORE_BUCKET='agha-gdr-store-2.0'

######################################################################################################

# AWS CLI existence check
command -v aws >/dev/null 2>&1 || {
  echo >&2 "AWS CLI COMMAND NOT FOUND. ABORTING..."
  exit 1
}

# AWS Creds check
aws sts get-caller-identity >/dev/null 2>&1 || {
  echo >&2 "UNABLE TO LOCATE CREDENTIALS. YOUR AWS LOGIN SESSION HAVE EXPIRED. PLEASE LOGIN. ABORTING..."
  exit 1
}

# Move dynamodb manifest
python move_manifest_record.py \
  --bucket_location ${BUCKET_LOCATION} \
  --partition_key ${PARTITION_KEY} \
  --old_sort_key ${SOURCE_SORT_KEY} \
  --new_sort_key ${TARGET_SORT_KEY} \
  --value_override ${OVERRIDE_VALUE} \
  2>&1 || {
    echo >&2 "FAIL DYNAMODB UPDATE. ABORTING..."
    exit 1
}

# Move the file object
echo "Moving original file"
s3_uri_source="s3://${BUCKET_LOCATION}/${SOURCE_SORT_KEY}"
s3_uri_target="s3://${BUCKET_LOCATION}/${TARGET_SORT_KEY}"
aws s3 mv "${s3_uri_source}" "${s3_uri_target}"

 Update manifest.txt file for each submission
if [ "$UPDATE_MANIFEST_TXT" == true ] && [ "$BUCKET_LOCATION" == $STORE_BUCKET ]; then
  echo "Updating manifest.txt file (in source and target submission)"
  python manifest_txt_update.py --s3_key_object $SOURCE_SORT_KEY
  python manifest_txt_update.py --s3_key_object $TARGET_SORT_KEY

fi

# Move '__results.json' location
echo "Moving results json file with modification"
python move_and_modify_results_file.py \
  --source_s3_key "${SOURCE_SORT_KEY}" \
  --target_s3_key "${TARGET_SORT_KEY}" \
  2>&1 || {
    echo >&2 "No results file found. Terminating ... "
    exit 1
}

echo "Script Execute Successfully"


