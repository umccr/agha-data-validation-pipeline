#!/usr/bin/env bash

######################################################################################################
# TODO: Fill this this section for the migration
# TODO: Please also export your AWS credentials to AWS_PROFILE
export BUCKET_NAME="agha-gdr-store-2.0"
export FLAGSHIP="EE"

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
python find_and_delete_duplicates.py --bucket_name ${BUCKET_NAME} --flagship ${FLAGSHIP}

echo "Script Execute Successfully"


