#!/usr/bin/env python3
import json
import logging
import os
import re
import io

import boto3
import pandas as pd

import util
import notification
from util import get_resource, botocore
from util.s3 import get_s3_filename

MANIFEST_REQUIRED_COLUMNS = {'filename', 'checksum', 'agha_study_id'}
# Manifest field validation related
AGHA_ID_RE = re.compile('^A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?$|^unknown$')
MD5_RE = re.compile('^[0-9a-f]{32}$')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class SubmissionData:

    def __init__(
            self, 
            bucket_name='',
            manifest_key='',
            submission_prefix=''
        ):

        # Event data
        self.bucket_name = bucket_name
        self.manifest_key = manifest_key
        self.submission_prefix = submission_prefix

        # Data from current S3 bucket
        self.file_metadata = list()

        # The content of manifest.txt
        self.manifest_data = pd.DataFrame()

        # Validation result from the manifest
        self.filename_accepted = list()
        self.files_rejected = list()
        self.files_extra = list()
        self.output_prefix = str()
    



def retrieve_manifest_data(bucket_name:str, manifest_key: str):

    client_s3 = get_resource('s3')

    logger.info(
        f'Getting manifest from: {bucket_name}/{manifest_key}')

    try:
        manifest_obj = client_s3.get_object(
            Bucket=bucket_name, Key=manifest_key)
    except botocore.exceptions.ClientError as e:
        raise ValueError(f'could not retrieve manifest data from S3:\r{e}')

    try:
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_data = pd.read_csv(manifest_str, sep='\t', encoding='utf8')
        manifest_data.fillna(value='not provided', inplace=True)
    except Exception as e:
        raise ValueError(f'could not convert manifest into DataFrame:\r{e}')

    return manifest_data


def validate_manifest(data:SubmissionData, strict_mode:bool=True, notify:bool=True):
    
    # Check manifest columns
    columns_present = set(data.manifest_data.columns.tolist())
    columns_missing = MANIFEST_REQUIRED_COLUMNS.difference(columns_present)
    
    
    if columns_missing:
        plurality = 'column' if len(columns_missing) == 1 else 'columns'
        cmissing_str = '\r\t'.join(columns_missing)
        cfound_str = '\r\t'.join(columns_present)
        message_base = f'required {plurality} missing from manifest:'
        notification.log_and_store_message(
            f'{message_base}\r\t{cmissing_str}\rGot:\r\t{cfound_str}', level='critical')
        notification.notify_and_exit()

    # File discovery
    # Entry count
    notification.log_and_store_message(f'Entries in manifest: {len(data.manifest_data)}')
    
    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_s3 = {get_s3_filename(md) for md in data.file_metadata}
    notification.log_and_store_file_message(message_text, files_s3)
    
    # Files missing from S3
    files_manifest = set(data.manifest_data['filename'].to_list())
    files_missing_from_s3 = files_manifest.difference(files_s3)
    message_text = f'Entries in manifest, but not on S3'
    notification.log_and_store_file_message(message_text, files_missing_from_s3)
    
    # Extra files present on S3
    files_missing_from_manifest = files_s3.difference(files_manifest)
    message_text = f'Entries on S3, but not in manifest'
    notification.log_and_store_file_message(message_text, files_missing_from_manifest)
    
    # Files present in manifest *and* S3
    files_matched = files_manifest.intersection(files_s3)
    message_text = f'Entries matched in manifest and S3'
    notification.log_and_store_file_message(message_text, files_matched)
    
    # Matched files that are accepted. Here files such as indices are filtered.
    files_matched_prohibited = list()
    files_matched_accepted = list()
    for filename in files_matched:
        if any(filename.endswith(fext) for fext in util.FEXT_ACCEPTED):
            files_matched_accepted.append(filename)
        else:
            files_matched_prohibited.append(filename)
    message_text = f'Matched entries excluded on file extension'
    notification.log_and_store_file_message(message_text, files_matched_prohibited)
    message_text = f'Matched entries eligible for validation'
    notification.log_and_store_file_message(message_text, files_matched_accepted)
    
    # Record error messages for extra files (other than manifest.txt) or missing files
    if 'manifest.txt' in files_missing_from_manifest:
        files_missing_from_manifest.remove('manifest.txt')
    messages_error = list()
    if files_missing_from_s3:
        messages_error.append('files listed in manifest were absent from S3')
    if files_missing_from_manifest:
        messages_error.append('files found on S3 absent from manifest.tsv')

    # Field validation
    for row in data.manifest_data.itertuples():
        # Study ID
        if not AGHA_ID_RE.match(row.agha_study_id):
            message = f'got malformed AGHA study ID for {row.filename} ({row.agha_study_id})'
            messages_error.append(message)
        # Checksum
        if not MD5_RE.match(row.checksum):
            message = f'got malformed MD5 checksum for {row.filename} ({row.checksum})'
            messages_error.append(message)

    # Check for error messages, exit in strict mode otherwise just emit warnings
    if messages_error:
        plurality = 'message' if len(messages_error) == 1 else 'messages'
        errors = '\r\t'.join(messages_error)
        message_base = f'Manifest failed validation with the following {plurality}'
        message = f'{message_base}:\r\t{errors}'
        if strict_mode:
            notification.log_and_store_message(message, level='critical')
            if notify:
                notification.notify_and_exit()
            else:
                raise ValueError()
        else:
            notification.log_and_store_message(message, level='warning')

    # Notify with success message
    message = f'Manifest successfully validated, continuing with file validation'
    notification.log_and_store_message(message)
    if notify:
        notification.send_notifications()
    # NOTE(SW): returning files that are on S3 and (1) in manifest, and (2) not in manifest. This
    # will allow flexbility in the future for any refactor if we decide to modify handling of
    # missing files
    return files_matched_accepted, list(files_missing_from_manifest)


def find_study_id_from_manifest_df_and_filename(manifest_df, filename):
    file_info = manifest_df.loc[manifest_df['filename'] == filename].iloc[0]
    return file_info['agha_study_id']

def find_checksum_from_manifest_df_and_filename(manifest_df, filename):
    file_info = manifest_df.loc[manifest_df['filename'] == filename].iloc[0]
    return file_info['checksum']