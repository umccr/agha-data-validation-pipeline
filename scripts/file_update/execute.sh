#!/usr/bin/env bash

######################################################################################################
# TODO: Fill this this section for the migration
# TODO: Please also export your AWS credentials to AWS_PROFILE
export BUCKET_LOCATION="${1}"
export PARTITION_KEY="${2}"
export SOURCE_SORT_KEY="${3}"
export TARGET_SORT_KEY="${4}"
export OVERRIDE_VALUE="${5}"

# Update manifest.txt file in submission level by toggling 'UPDATE_MANIFEST_TXT' variable
# Ideally you would only trigger this for the last move file in the submission

export UPDATE_MANIFEST_TXT="${6}" # Boolean value (lower case): true, false

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
# Printing choices made

echo "Printing selection made"
echo "BUCKET_LOCATION: ${BUCKET_LOCATION}"
echo "PARTITION_KEY: ${PARTITION_KEY}"
echo "SOURCE_SORT_KEY: ${SOURCE_SORT_KEY}"
echo "TARGET_SORT_KEY: ${TARGET_SORT_KEY}"
echo "OVERRIDE_VALUE: ${OVERRIDE_VALUE}"
echo "UPDATE_MANIFEST_TXT: ${UPDATE_MANIFEST_TXT}"


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
  --bucket_location "${BUCKET_LOCATION}" \
  --partition_key "${PARTITION_KEY}" \
  --old_sort_key "${SOURCE_SORT_KEY}" \
  --new_sort_key "${TARGET_SORT_KEY}" \
  --value_override "${OVERRIDE_VALUE}" \
  2>&1 || {
  echo >&2 "FAIL DYNAMODB UPDATE. ABORTING..."
  exit 1
}

# Move the file object
if [ "$SOURCE_SORT_KEY" != "$TARGET_SORT_KEY" ]; then
  echo "Moving Files to a different location"
  s3_uri_source="s3://${BUCKET_LOCATION}/${SOURCE_SORT_KEY}"
  s3_uri_target="s3://${BUCKET_LOCATION}/${TARGET_SORT_KEY}"
  aws s3 mv "${s3_uri_source}" "${s3_uri_target}"
else
  echo "Source and target destination are the same, skipping file move"
fi

# Update manifest.txt file for each submission
if [ "$UPDATE_MANIFEST_TXT" == true ] && [ "$BUCKET_LOCATION" == $STORE_BUCKET ]; then
  echo "Updating manifest.txt file (in source and target submission)"
  python manifest_txt_update.py --s3_key_object "$SOURCE_SORT_KEY"
  python manifest_txt_update.py --s3_key_object "$TARGET_SORT_KEY"

fi

# Move '__results.json' location
if [ "$SOURCE_SORT_KEY" != "$TARGET_SORT_KEY" ]; then
  echo "Moving results json file with modification"
  python move_and_modify_results_file.py \
    --source_s3_key "${SOURCE_SORT_KEY}" \
    --target_s3_key "${TARGET_SORT_KEY}" \
    2>&1 || {
    echo >&2 "No results file found. Terminating ... "
    exit 1
  }
else
  echo "Source and target destination are the same, skipping result file move"
fi

echo "Script Execute Successfully"
