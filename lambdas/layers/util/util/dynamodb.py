import os.path
import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr

from .s3 import S3EventRecord
from .agha import get_flagship_from_key, get_file_type
from util import submission_data
from util import get_datetimestamp

from typing import List
from enum import Enum

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_RESOURCE = ''
DATE_EXCEPTIONS = ["2020-02-30"]

class FileRecordAttribute(Enum):
    S3_KEY = "s3_key"
    ETAG = "etag"
    FILENAME = "filename"
    FILETYPE = "filetype"
    DATE_MODIFIED = "date_modified"
    FLAGSHIP = "flagship"
    PROVIDED_CHECKSUM = "provided_checksum"
    IS_IN_MANIFEST = "is_in_manifest"
    AGHA_STUDY_ID = "agha_study_id"
    IS_VALIDATED = "is_validated"
    SIZE_IN_BYTES = "size_in_bytes"

    def __str__(self):
        return self.value

class BucketFileRecord:
    """
    The DynamoDB table is configured with a mandatory composite key composed of two elements:
    - s3_key: The key of s3 bucket that is part of the partition key
    All other attributes are optional (although some can automatically be derived from the object key)
    """

    def __init__(self,
                 s3_key: str,
                 size_in_bytes: int,
                 etag: str = "",
                 date_modified: str = "",
                 provided_checksum: str = "",
                 agha_study_id: str = "",
                 is_in_manifest: str = "False",
                 is_validated: str = "False"):
        self.s3_key = s3_key    
        self.flagship = get_flagship_from_key(s3_key)
        self.etag = etag
        self.submission = os.path.dirname(s3_key)
        self.filename = os.path.basename(s3_key)
        self.filetype = get_file_type(s3_key).value
        self.date_modified = date_modified
        self.size_in_bytes = size_in_bytes
        self.provided_checksum = provided_checksum
        self.agha_study_id = agha_study_id
        self.is_in_manifest = is_in_manifest
        self.is_validated = is_validated

    def to_dict(self):
        return {
            FileRecordAttribute.S3_KEY.value: self.s3_key,
            FileRecordAttribute.ETAG.value: self.etag,
            FileRecordAttribute.FILENAME.value: self.filename,
            FileRecordAttribute.FILETYPE.value: self.filetype,
            FileRecordAttribute.DATE_MODIFIED.value: self.date_modified,
            FileRecordAttribute.FLAGSHIP.value: self.flagship,
            FileRecordAttribute.PROVIDED_CHECKSUM.value: self.provided_checksum,
            FileRecordAttribute.AGHA_STUDY_ID.value: self.agha_study_id,
            FileRecordAttribute.IS_VALIDATED.value: self.is_validated,
            FileRecordAttribute.SIZE_IN_BYTES.value: self.size_in_bytes,
            FileRecordAttribute.IS_IN_MANIFEST.value: self.is_validated
        }

    def __str__(self):
        return f"s3://{self.s3_key}"


    @classmethod
    def from_manifest_record(cls, filename, data: submission_data.SubmissionData):

        # Check for some required data, and get record from manifest data
        assert not data.manifest_data.empty
        manifest_info = data.manifest_data.loc[data.manifest_data['filename']==filename].iloc[0]
        
        s3_metadata = find_s3_metadata_from_s3_metadata_list_by_filename(filename, data.file_metadata)

        # Create class instance
        record = cls(
            s3_key= s3_metadata['Key'],
            size_in_bytes=s3_metadata['Size'],
            etag=s3_metadata["ETag"],
            date_modified=s3_metadata["LastModified"],
            provided_checksum =manifest_info["checksum"],
            agha_study_id=manifest_info["agha_study_id"],
            is_in_manifest = "True",
            is_validated= "True"
        )

        return record

def find_s3_metadata_from_s3_metadata_list_by_filename(filename, array):
    """
    Expected Output:
    {
        'Key': 'string',
        'LastModified': datetime(2015, 1, 1),
        'ETag': 'string',
        'Size': 123,
        'StorageClass': 'STANDARD'|'REDUCED_REDUNDANCY'|'GLACIER'|'STANDARD_IA'|'ONEZONE_IA'|'INTELLIGENT_TIERING'|'DEEP_ARCHIVE'|'OUTPOSTS',
        'Owner': {
            'DisplayName': 'string',
            'ID': 'string'
        }
    }
    """
    for each_array in array:

        key = each_array.get('Key')
        key_basename = os.path.basename(key)

        if key_basename == filename:
            return each_array
    raise ValueError('No metadata data found at S3')

class ArchiveBucketFileRecord(BucketFileRecord):
    """
    This is an arvhiced class based on the DynamoDb bucket storage record.
    There is an arvived action to log the status being updated
    """

    def __init__(self, bucket_record_json, archive_action):
        super().__init__(
            s3_key=bucket_record_json["s3_key"],
            size_in_bytes=bucket_record_json['size_in_bytes'],
            etag= bucket_record_json["etag"],
            date_modified= get_datetimestamp(),
            provided_checksum= bucket_record_json["provided_checksum"],
            agha_study_id= bucket_record_json["agha_study_id"],
            is_in_manifest=bucket_record_json["is_in_manifest"],
            is_validated=bucket_record_json["is_validated"]
        )
        self.archive_action = archive_action

    def __str__(self):
        return f"s3://{self.s3_key}"

def convert_s3_record_to_db_record(s3_record: S3EventRecord) -> BucketFileRecord:
    return BucketFileRecord(
        s3_key=s3_record.object_key,
        etag=s3_record.etag,
        date_modified=s3_record.event_time,
        size_in_bytes=s3_record.size_in_bytes)


def create_archive_record_from_db_record(bucket_record: BucketFileRecord, archive_action: str) -> ArchiveBucketFileRecord:
    bucket_record_json = bucket_record.__dict__
    
    return ArchiveBucketFileRecord(
        bucket_record_json=bucket_record_json,
        archive_action=archive_action
    )

def get_resource():
    global DYNAMODB_RESOURCE
    if DYNAMODB_RESOURCE:
        return DYNAMODB_RESOURCE
    else:
        if os.getenv('AWS_ENDPOINT'):
            logger.info("Using local DynamoDB instance")
            DYNAMODB_RESOURCE = boto3.resource(
                service_name='dynamodb', endpoint_url=os.getenv('AWS_ENDPOINT'))
        else:
            logger.info("Using AWS DynamoDB instance")
            DYNAMODB_RESOURCE = boto3.resource(service_name='dynamodb')
        return DYNAMODB_RESOURCE


def delete_record(table_name, records: BucketFileRecord) -> dict:
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    return tbl.delete_item(
        Key=records.to_dict,
        ReturnValues='ALL_OLD'
    )


def write_record(table_name, record) -> dict:
    dynamodb_resource = get_resource()
    dynamodb_table = dynamodb_resource.Table(table_name)

    resp = dynamodb_table.put_item(Item=record.__dict__, ReturnValues='ALL_OLD')
    return resp


def batch_write_records(table_name: str, records: list()):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for record in records:
            batch.put_item(Item=record.__dict__)

def db_response_to_file_record(db_dict: dict) -> BucketFileRecord:
    retval = BucketFileRecord(
        s3_key = db_dict[FileRecordAttribute.S3_KEY.value],
        flagship = db_dict[FileRecordAttribute.FLAGSHIP.value]
    )

    if FileRecordAttribute.ETAG.value in db_dict:
        retval.etag = db_dict[FileRecordAttribute.ETAG.value]
    if FileRecordAttribute.PROVIDED_CHECKSUM.value in db_dict:
        retval.provided_checksum = db_dict[FileRecordAttribute.PROVIDED_CHECKSUM.value]
    if FileRecordAttribute.IS_IN_MANIFEST.value in db_dict:
        retval.is_in_manifest = db_dict[FileRecordAttribute.IS_IN_MANIFEST.value]
    if FileRecordAttribute.AGHA_STUDY_ID.value in db_dict:
        retval.agha_study_id = db_dict[FileRecordAttribute.AGHA_STUDY_ID.value]
    if FileRecordAttribute.IS_VALIDATED.value in db_dict:
        retval.is_validated = db_dict[FileRecordAttribute.IS_VALIDATED.value]
    if FileRecordAttribute.FILENAME.value in db_dict:
        retval.filename = db_dict[FileRecordAttribute.FILENAME.value]
    if FileRecordAttribute.FILETYPE.value in db_dict:
        retval.filetype = db_dict[FileRecordAttribute.FILETYPE.value]
    if FileRecordAttribute.DATE_MODIFIED.value in db_dict:
        retval.date_modified = db_dict[FileRecordAttribute.DATE_MODIFIED.value]
    if FileRecordAttribute.SIZE_IN_BYTES.value in db_dict:
        retval.size_in_bytes = db_dict[FileRecordAttribute.SIZE_IN_BYTES.value]
    
    return retval
   

def get_record_from_s3_key(table_name, s3_key: str) -> BucketFileRecord:
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    expr = Key(FileRecordAttribute.S3_KEY.value).eq(s3_key)

    resp = tbl.get_item(expr)
    if not 'Item' in resp:
        raise ValueError(f"No record found for \'{s3_key}\' at \'{table_name}\' table.")

    return db_response_to_file_record(resp['Item'])


# def get_by_prefix(bucket: str, prefix: str):
#     ddb = get_resource()
#     tbl = ddb.Table(TABLE_NAME)

#     expr = Key(DbAttribute.BUCKET.value).eq(bucket) & Key(DbAttribute.S3KEY.value).begins_with(prefix)

#     response = tbl.query(
#         KeyConditionExpression=expr
#     )
#     result = response['Items']

#     while 'LastEvaluatedKey' in response:
#         response = tbl.query(
#             KeyConditionExpression=expr,
#             ExclusiveStartKey=response['LastEvaluatedKey']
#         )
#         result.extend(response['Items'])

#     return result


# def get_pending_validation(bucket: str, prefix: str = None):
#     ddb = get_resource()
#     tbl = ddb.Table(TABLE_NAME)

#     if prefix:
#         key_expr = Key(DbAttribute.BUCKET.value).eq(bucket) & Key(DbAttribute.S3KEY.value).begins_with(prefix)
#     else:
#         key_expr = Key(DbAttribute.BUCKET.value).eq(bucket)
#     filter_expr = Attr(DbAttribute.QUICK_CHECK_STATUS.value).eq("Pending")

#     response = tbl.query(
#         KeyConditionExpression=key_expr,
#         FilterExpression=filter_expr
#     )
#     result = response['Items']

#     while 'LastEvaluatedKey' in response:
#         response = tbl.query(
#             KeyConditionExpression=key_expr,
#             FilterExpression=filter_expr,
#             ExclusiveStartKey=response['LastEvaluatedKey']
#         )
#         result.extend(response['Items'])

#     return result


# def update_store_record(record):
#     """
#     A store record should only be created when a validated staging record/file is transferred from the
#     STAGING to the STORE bucket.
#     As such we want to make sure the STORE record is updated with the metadata from the STAGING record.
#     :param record: the STORE record to update
#     :return: a dict containing any 'old' values that have been replaced with this update
#     """
#     if record.bucket != agha.STORE_BUCKET:
#         logger.warning(f"Attempt to update non-STORE record! Skipping {record}")
#         return

#     # get the corresponding STAGING record to retrieve the (validation) metadata from
#     # (there should always be one, unless the object keys have been changed during the STAGING -> STORE transfer)
#     staging_record = get_record(agha.STAGING_BUCKET, record.s3key)

#     # make sure the s3 object keys are the same
#     if not staging_record:
#         logger.warning(f"Store and Staging records don't have the same object key! Skipping {record}!")
#         return

#     # Copy the validation metadata from the staging record to the store record
#     record.checksum_calculated = staging_record.checksum_calculated
#     record.checksum_provided = staging_record.checksum_provided
#     record.has_index = staging_record.has_index
#     record.study_id = staging_record.study_id
#     record.quick_ckeck = staging_record.quick_ckeck

#     # persist the record
#     resp = write_record(record=record)
#     return resp
