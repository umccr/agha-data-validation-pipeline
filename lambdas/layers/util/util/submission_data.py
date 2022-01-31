#!/usr/bin/env python3
import logging
import os
import re
import io
import json
import sys

import botocore
import pandas as pd

import util
from util import notification, s3, agha

MANIFEST_REQUIRED_COLUMNS = {'filename', 'checksum', 'agha_study_id'}
# Manifest field validation related
AGHA_ID_RE = re.compile('^A\d{7,8}(?:_mat|_pat|_R1|_R2|_R3)?$|^unknown$')
MD5_RE = re.compile('^[0-9a-f]{32}$')

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


########################################################################################################################
# Data placeholder for submitted data

class SubmissionData:

    def __init__(
            self,
            bucket_name='',
            manifest_s3_key='',
            submission_prefix=''
    ):
        # Event data
        self.bucket_name = bucket_name
        self.manifest_s3_key = manifest_s3_key
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

    @classmethod
    def create_submission_data_object_from_s3_event(cls, s3_record):
        s3_key: str = s3_record['s3']['object']['key']
        bucket: str = s3_record['s3']['bucket']['name']
        prefix: str = os.path.dirname(s3_key)

        return cls(
            bucket_name=bucket,
            manifest_s3_key=s3_key,
            submission_prefix=prefix
        )


def retrieve_manifest_data(bucket_name: str, manifest_key: str):
    client_s3 = util.get_client('s3')

    logger.info(
        f'Getting manifest from: {bucket_name}/{manifest_key}')

    try:
        manifest_obj = client_s3.get_object(
            Bucket=bucket_name, Key=manifest_key)
    except botocore.exceptions.ClientError as e:
        message = f'could not retrieve manifest data from S3:\r{e}'
        logger.error(message)
        raise ValueError(message)

    try:
        manifest_str = io.BytesIO(manifest_obj['Body'].read())
        manifest_data = pd.read_csv(manifest_str, sep='\t', encoding='utf8')
        manifest_data.fillna(value='not provided', inplace=True)
    except Exception as e:
        message = f'could not convert manifest into DataFrame:\r{e}'
        logger.error(message)
        raise ValueError(message)

    return manifest_data


def validate_manifest(data: SubmissionData, postfix_exception_list: list, skip_checksum_check: bool = False):
    # Check manifest columns
    columns_present = set(data.manifest_data.columns.tolist())
    columns_missing = MANIFEST_REQUIRED_COLUMNS.difference(columns_present)

    if columns_missing:
        plurality = 'column' if len(columns_missing) == 1 else 'columns'
        cmissing_str = '\r\t'.join(columns_missing)
        cfound_str = '\r\t'.join(columns_present)
        message_base = f'required {plurality} missing from manifest:'
        message_str = f'{message_base}\r\t{cmissing_str}\rGot:\r\t{cfound_str}'
        notification.log_and_store_message(message_str, level='critical')
        raise ValueError(json.dumps(message_str, indent=4, cls=util.JsonSerialEncoder))

    # File discovery
    # Entry count
    notification.log_and_store_message(f'Entries in manifest: {len(data.manifest_data)}')

    # Files present on S3
    message_text = f'Entries on S3 (including manifest)'
    files_s3 = {s3.get_s3_filename(md) for md in data.file_metadata}
    notification.log_and_store_file_message(message_text, files_s3)

    # Files missing from S3
    files_manifest = set(data.manifest_data['filename'].to_list())
    files_missing_from_s3 = list(files_manifest.difference(files_s3))
    message_text = f'Entries in manifest, but not on S3'
    notification.log_and_store_file_message(message_text, files_missing_from_s3)

    # Extra files present on S3
    files_missing_from_manifest = list(files_s3.difference(files_manifest))
    message_text = f'Entries on S3, but not in manifest (including manifest)'
    notification.log_and_store_file_message(message_text, files_missing_from_manifest)

    # Files present in manifest *and* S3
    files_matched = list(files_manifest.intersection(files_s3))
    message_text = f'Entries matched in manifest and S3'
    notification.log_and_store_file_message(message_text, files_matched)

    # Matched files that are accepted. Here files such as indices are filtered.
    files_matched_prohibited = list()
    files_matched_accepted = list()

    for filename in files_matched:
        # Do not processed skipped file
        if is_file_skipped(filename, postfix_exception_list):
            continue

        if agha.FileType.from_name(filename) != agha.FileType.UNSUPPORTED:
            files_matched_accepted.append(filename)
        else:
            files_matched_prohibited.append(filename)

    message_text = f'Matched entries excluded on file extension'
    notification.log_and_store_file_message(message_text, files_matched_prohibited)
    message_text = f'Matched entries eligible for validation'
    notification.log_and_store_file_message(message_text, files_matched_accepted)

    # Record error messages for extra files (other than manifest.txt, *.tbi, *.bai) or missing files
    remove_list = []
    for missing_filename in files_missing_from_manifest:
        if is_file_skipped(missing_filename, postfix_exception_list):
            remove_list.append(missing_filename)
            logger.info(f'{missing_filename} is in the exclusion filetype list. '
                        f'Adding it for removal')

    # Removing it
    for remove_item in remove_list:
        files_missing_from_manifest.remove(remove_item)
        logger.info(f'{remove_item} is removed from missing list')

    messages_error = list()
    if files_missing_from_s3:
        messages_error.append('Files listed in manifest were absent from S3.')
        messages_error.append(
            f'Files missing from s3: {json.dumps(files_missing_from_s3, indent=4, cls=util.JsonSerialEncoder)}')

    if files_missing_from_manifest:
        # File exist in s3 but not in the manifest, would just consider on file in the manifest.
        # This would give warning without any termination
        notification.MESSAGE_STORE.append('')  # New line to separate warning in email
        notification.log_and_store_message(
            f'Files in s3, but not in manifest (excluding manifest, index, and md5 file): {json.dumps(files_missing_from_manifest, indent=4, cls=util.JsonSerialEncoder)}',
            level='critical')
        notification.log_and_store_message('Submission contain file that is not in the manifest. '
                                           'Proceeding with only file in the manifest.',
                                           level='critical')

        notification.MESSAGE_STORE.append('')  # New line to separate warning in email

    # Field validation in the manifest file
    for row in data.manifest_data.itertuples():

        # Skipping check on manifest for skipped name
        if is_file_skipped(row.filename, postfix_exception_list):
            continue

        # Study ID
        if not AGHA_ID_RE.match(row.agha_study_id):
            message = f'got malformed AGHA study ID for {row.filename} ({row.agha_study_id})'
            messages_error.append(message)
        # Checksum
        if not MD5_RE.match(row.checksum) and not skip_checksum_check:
            message = f'got malformed MD5 checksum for {row.filename} ({row.checksum})'
            messages_error.append(message)

    # Check for error messages, exit in strict mode otherwise just emit warnings
    if messages_error:
        plurality = 'message' if len(messages_error) == 1 else 'messages'
        errors = '\r\t'.join(messages_error)
        message_base = f'Manifest failed validation with the following {plurality}:'
        messages_error.insert(0, message_base)
        notification.log_and_store_list_message(messages_error, level='critical')
        raise ValueError(json.dumps(messages_error, indent=4))

    # Notify with success message
    message = f'Manifest successfully validated.'
    notification.log_and_store_message(message)

    # NOTE(SW): returning files that are on S3 and (1) in manifest, and (2) not in manifest. This
    # will allow flexibility in the future for any refactor if we decide to modify handling of
    # missing files
    return files_matched_accepted, list(files_missing_from_manifest)


def find_study_id_from_manifest_df_and_filename(manifest_df, filename):
    file_info = manifest_df.loc[manifest_df['filename'] == filename].iloc[0]
    return file_info['agha_study_id']


def find_checksum_from_manifest_df_and_filename(manifest_df, filename):
    file_info = manifest_df.loc[manifest_df['filename'] == filename].iloc[0]
    return file_info['checksum']


def is_file_skipped(filename: str, postfix_exception_list: list = None) -> bool:
    """
    Should filename be accepted for validation.
    :param filename:
    :param postfix_exception_list:
    :return:
    """
    if postfix_exception_list is None:
        postfix_exception_list = []

    if agha.FileType.is_manifest_file(filename) or \
            agha.FileType.is_index_file(filename) or \
            agha.FileType.is_md5_file(filename) or \
            len([filename for postfix in postfix_exception_list if filename.endswith(postfix)]) > 0:
        return True
    return False
