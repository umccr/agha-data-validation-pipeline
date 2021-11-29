import os.path
import logging
import boto3
from boto3.dynamodb.conditions import Key, Attr

from .s3 import S3EventRecord
from .agha import FileType
import util

from typing import List
from enum import Enum

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DYNAMODB_RESOURCE = ''

########################################################################################################################
# Table: agha-gdr-e-tag

class ETagFileRecord:
    """
    The DynamoDb table will store ETag records to check if multiple record had been stored.
    - etag: Will store all etag across database
    - sort_key: Will Store a combination of bucket_name and s3_key
    - s3_key: Location of s3 in the bucket
    - bucket_name: Name of the bucket stored
    """

    def __init__(self, etag='', s3_key='', bucket_name=''):
        self.partition_key = etag
        self.sort_key = self.construct_etag_table_sort_key(bucket_name, s3_key)
        self.etag = etag
        self.s3_key = s3_key
        self.bucket_name = bucket_name

    @staticmethod
    def construct_etag_table_sort_key(bucket_name, s3_key):
        return f'BUCKET:{bucket_name}:S3_KEY:{s3_key}'

########################################################################################################################
# Table: agha-gdr-staging-bucket, agha-gdr-staging-bucket-archive, agha-gdr-store-bucket, agha-gdr-store-bucket-archive

# Field Attribute
class FileRecordAttribute(Enum):
    PARTITION_KEY = "partition_key"
    SORT_KEY = "sort_key"
    S3_KEY = "s3_key"
    ETAG = "etag"
    FILENAME = "filename"
    FILETYPE = "filetype"
    DATE_MODIFIED = "date_modified"
    SIZE_IN_BYTES = "size_in_bytes"
    ARCHIVE_LOG = "archive_log"

    def __str__(self):
        return self.value

class ManifestFileRecordAttribute(Enum):
    PARTITION_KEY = "partition_key"
    SORT_KEY = "sort_key"
    S3_KEY = "s3_key"
    FLAGSHIP = "flagship"
    FILENAME = "filename"
    FILETYPE = "filetype"
    SUBMISSION = "submission"
    DATE_MODIFIED = "date_modified"
    PROVIDED_CHECKSUM = "provided_checksum"
    AGHA_STUDY_ID = "agha_study_id"
    VALIDATION_STATUS = "validation_status"
    ARCHIVE_LOG = "archive_log"

    def __str__(self):
        return self.value

class FileRecordSortKey(Enum):

    FILE_RECORD = 'TYPE:FILE'
    MANIFEST_FILE_RECORD = 'TYPE:MANIFEST'

    def __str__(self):
        return self.value

# Record Attribute for STAGING and STORE bucket
class FileRecord:
    """
    The DynamoDB table is configured with a mandatory composite key composed of two elements:
    - partition_key: Most Probably the s3_key
    - sort_key: The type of file indicate it stores the file properties.
    All other attributes are self explanatory (although some can automatically be derived from the object key)
    """

    def __init__(self,
                 partition_key = "",
                 sort_key = "",
                 s3_key = "",
                 bucket_name="",
                 etag = "",
                 filename = "",
                 filetype = "",
                 date_modified = "",
                 size_in_bytes = 0):
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.s3_key = s3_key
        self.bucket_name = bucket_name
        self.etag = etag
        self.filename = filename
        self.filetype = filetype
        self.date_modified = date_modified
        self.size_in_bytes = size_in_bytes

    @classmethod
    def create_file_record_from_s3_record(cls, s3_record):

        # S3 event parsing
        bucket_name = s3_record.bucket_name
        s3_key = s3_record.object_key
        e_tag =s3_record.etag
        date_modified = util.get_datetimestamp()
        size_in_bytes = s3_record.size_in_bytes
        filename = os.path.basename(s3_key)
        filetype = FileType.from_name(filename)

        # Additional Field
        partition_key = s3_key
        sort_key = FileRecordSortKey.FILE_RECORD.value

        return cls(
            partition_key=partition_key,
            sort_key=sort_key,
            bucket_name=bucket_name,
            s3_key=s3_key,
            etag=e_tag,
            filename=filename,
            filetype=filetype.get_name(),
            date_modified=date_modified,
            size_in_bytes=size_in_bytes
        )



# Record for archive FileRecord
class ArchiveFileRecord(FileRecord):
    """
    This is an archived class based on the DynamoDb FileRecord storage.
    There is an archived action to log the status being updated
    """

    def __init__(self,
                 partition_key = "",
                 sort_key = "",
                 bucket_name = "",
                 s3_key = "",
                 etag = "",
                 filename = "",
                 filetype = "",
                 date_modified = "",
                 size_in_bytes = 0,
                 archive_log = ""):
        super().__init__(
            partition_key=partition_key,
            sort_key=sort_key,
            bucket_name=bucket_name,
            s3_key=s3_key,
            etag= etag,
            filename= filename,
            filetype= filetype,
            date_modified= date_modified,
            size_in_bytes= size_in_bytes
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_file_record_from_file_record(cls, file_record:FileRecord, archive_log):
        return cls(
            partition_key=file_record.partition_key,
            sort_key=file_record.sort_key,
            bucket_name=file_record.bucket_name,
            s3_key=file_record.s3_key,
            etag=file_record.etag,
            filename=file_record.filename,
            filetype=file_record.filetype,
            date_modified=util.get_datetimestamp(),
            size_in_bytes=file_record.size_in_bytes,
            archive_log=archive_log
        )

    @classmethod
    def create_archive_file_record_from_json(cls, file_record_json, archive_log):
        return cls(
            **file_record_json, archive_log=archive_log
        )

# Manifest Record
class ManifestFileRecord:
    """
    The DynamoDB table is configured with a mandatory composite key composed of two elements:
    - partition_key: Most Probably the s3_key
    - sort_key: The type of file indicate it stores the file properties.
    All other attributes are self explanatory (although some can automatically be derived from the object key)
    """

    def __init__(self,
                partition_key = "",
                sort_key = "",
                flagship = "",
                filename = "",
                filetype = "",
                submission = "",
                date_modified = "",
                provided_checksum = "",
                agha_study_id = "",
                is_in_manifest = "",
                validation_status = ""):
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.flagship = flagship
        self.filename = filename
        self.filetype = filetype
        self.submission = submission
        self.date_modified = date_modified
        self.provided_checksum = provided_checksum
        self.agha_study_id = agha_study_id
        self.is_in_manifest = is_in_manifest
        self.validation_status = validation_status

    @classmethod
    def create_manifest_record_from_manifest_file(cls):
        print("under development")

# Archive File Record
class ArchiveManifestFileRecord(ManifestFileRecord):
    """
    This is an archived class based on the DynamoDb ManifestRecord storage.
    There is an archived action to log the status being updated
    """
    def __init__(self,
                partition_key = "",
                sort_key = "",
                flagship = "",
                filename = "",
                filetype = "",
                submission = "",
                date_modified = "",
                provided_checksum = "",
                agha_study_id = "",
                validation_status = "",
                is_in_manifest = "",
                archive_log = ""
                 ):
        super().__init__(
            partition_key = partition_key,
            sort_key = sort_key,
            flagship = flagship,
            filename = filename,
            filetype = filetype,
            submission = submission,
            date_modified = date_modified,
            provided_checksum = provided_checksum,
            agha_study_id = agha_study_id,
            is_in_manifest = is_in_manifest,
            validation_status = validation_status
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_manifest_record_from_manifest_record(cls, manifest_record: ManifestFileRecord, archive_log):
        return cls(
            partition_key = manifest_record.partition_key,
            sort_key = manifest_record.sort_key,
            flagship = manifest_record.flagship,
            filename = manifest_record.filename,
            filetype = manifest_record.filetype,
            submission = manifest_record.submission,
            date_modified = util.get_datetimestamp(),
            provided_checksum = manifest_record.provided_checksum,
            agha_study_id = manifest_record.agha_study_id,
            validation_status = manifest_record.validation_status,
            is_in_manifest = manifest_record.is_in_manifest,
            archive_log=archive_log
        )


########################################################################################################################
# Table: agha-gdr-result-bucket, agha-gdr-result-bucket-archive

# agha-gdr-result-bucket, agha-gdr-result-bucket-archive may contain 3 type of different class
# 1. FileRecord class (defined above) that also be used as staging/store bucket file record
# 2. ResultRecord class (defined below) to hold the data output of the test. ResultRecord class may contain 2 types. \
#       Explanation defined at the docstring class

class ResultSortKeyPrefix(Enum):

    FILE = "FILE"
    STATUS = "STATUS"
    DATA = "DATA"

    def __str__(self):
        return self.value

class ResultRecord:
    """
    partition_key: Most probably the s3 of the original file
    sort_key:
        Can be defined 2 types:
            1. Prefix will be 'DATA' followed by check_type seperated by colon
                (e.g. Result of checksum check would be stored as 'DATA:CHECKSUM')
            2. Prefix will be 'STATUS' followed by check_type seperated by colon
                (e.g. Result of checksum check would be stored as 'STATUS:CHECKSUM')
    value:
        Can be defined 2 types depending on the sort_key type:
            1. If 'DATA', it will contain the output result (e.g for CHECKSUM might be '120EA8A25E5D487BF68B5F7096440')
            2. If 'STATUS', it will contain the status result (e.g 'SUCCESS', 'FAILURE')

    """

    def __init__(self, partition_key='', date_modified="", sort_key='', value=''):
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.date_modified = date_modified
        self.value = value

class ArchiveResultRecord:
    def __init__(self,
                partition_key = "",
                sort_key = "",
                date_modified = "",
                value = "",
                archive_log = ""
                ):
        super().__init__(
            partition_key = partition_key,
            sort_key = sort_key,
            date_modified = date_modified,
            value = value
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_result_record_from_result_record(cls, result_record: ResultRecord, archive_log):
        return cls(
            partition_key = result_record.partition_key,
            sort_key = result_record.sort_key,
            date_modified = util.get_datetimestamp(),
            value = result_record.value,
            archive_log = archive_log
        )


########################################################################################################################
# The following will contain function related to boto3 DynamoDB API

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


def delete_record_from_record_class(table_name:str, record):
    """
    This will delete record from dynamodb with given table_name and record class.
    Record class MUST have 'partition_key' and 'sort_key' as their property as deletetion are based on those
    :param table_name: DynamoDb table name
    :param record: A class that contain 'partition_key' and 'sort_key' which will be deleted based on.
    :return:
    """

    ddb = get_resource()
    tbl = ddb.Table(table_name)

    delete_res =  tbl.delete_item(
        Key={
            'partition_key':record.partition_key,
            'sort_key':record.sort_key
        },
        ReturnValues='ALL_OLD'
    )

    if 'Attributes' in delete_res:
        return delete_res['Attributes']
    else:
        return ValueError(f"partition_key: {record.partition_key}, sort_key: {record.sort_key}\
         has not been successfully deleted from table '{table_name}'")


def write_record_from_class(table_name, record) -> dict:
    dynamodb_resource = get_resource()
    dynamodb_table = dynamodb_resource.Table(table_name)

    resp = dynamodb_table.put_item(Item=record.__dict__, ReturnValues='ALL_OLD')
    return resp


def batch_write_records(table_name: str, records: list()):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for record in records:
            batch.put_item(Item=record.__dict__)


def get_item_from_pk(table_name: str, partition_key: str):
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    expr = Key(FileRecordAttribute.PARTITION_KEY.value).eq(partition_key)

    response = tbl.query(
        KeyConditionExpression=expr
    )

    return response

def get_item_from_pk_and_sk(table_name: str, partition_key: str, sort_key_prefix: str):
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    expr = Key(FileRecordAttribute.PARTITION_KEY.value).eq(partition_key) & \
           Key(FileRecordAttribute.SORT_KEY.value).begins_with(sort_key_prefix)

    response = tbl.query(
        KeyConditionExpression=expr
    )

    return response

# def db_response_to_file_record(db_dict: dict) -> BucketFileRecord:
#     retval = BucketFileRecord(
#         s3_key=db_dict[FileRecordAttribute.S3_KEY.value],
#         flagship=db_dict[FileRecordAttribute.FLAGSHIP.value]
#     )
#
#     if FileRecordAttribute.ETAG.value in db_dict:
#         retval.etag = db_dict[FileRecordAttribute.ETAG.value]
#     if FileRecordAttribute.PROVIDED_CHECKSUM.value in db_dict:
#         retval.provided_checksum = db_dict[FileRecordAttribute.PROVIDED_CHECKSUM.value]
#     if FileRecordAttribute.IS_IN_MANIFEST.value in db_dict:
#         retval.is_in_manifest = db_dict[FileRecordAttribute.IS_IN_MANIFEST.value]
#     if FileRecordAttribute.AGHA_STUDY_ID.value in db_dict:
#         retval.agha_study_id = db_dict[FileRecordAttribute.AGHA_STUDY_ID.value]
#     if FileRecordAttribute.IS_VALIDATED.value in db_dict:
#         retval.is_validated = db_dict[FileRecordAttribute.IS_VALIDATED.value]
#     if FileRecordAttribute.FILENAME.value in db_dict:
#         retval.filename = db_dict[FileRecordAttribute.FILENAME.value]
#     if FileRecordAttribute.FILETYPE.value in db_dict:
#         retval.filetype = db_dict[FileRecordAttribute.FILETYPE.value]
#     if FileRecordAttribute.DATE_MODIFIED.value in db_dict:
#         retval.date_modified = db_dict[FileRecordAttribute.DATE_MODIFIED.value]
#     if FileRecordAttribute.SIZE_IN_BYTES.value in db_dict:
#         retval.size_in_bytes = db_dict[FileRecordAttribute.SIZE_IN_BYTES.value]
#
#     return retval

# def db_response_to_file_record(db_dict: dict) -> BucketFileRecord:
#     retval = BucketFileRecord(
#         s3_key=db_dict[FileRecordAttribute.S3_KEY.value],
#         flagship=db_dict[FileRecordAttribute.FLAGSHIP.value]
#     )
#
#     if FileRecordAttribute.ETAG.value in db_dict:
#         retval.etag = db_dict[FileRecordAttribute.ETAG.value]
#     if FileRecordAttribute.PROVIDED_CHECKSUM.value in db_dict:
#         retval.provided_checksum = db_dict[FileRecordAttribute.PROVIDED_CHECKSUM.value]
#     if FileRecordAttribute.IS_IN_MANIFEST.value in db_dict:
#         retval.is_in_manifest = db_dict[FileRecordAttribute.IS_IN_MANIFEST.value]
#     if FileRecordAttribute.AGHA_STUDY_ID.value in db_dict:
#         retval.agha_study_id = db_dict[FileRecordAttribute.AGHA_STUDY_ID.value]
#     if FileRecordAttribute.IS_VALIDATED.value in db_dict:
#         retval.is_validated = db_dict[FileRecordAttribute.IS_VALIDATED.value]
#     if FileRecordAttribute.FILENAME.value in db_dict:
#         retval.filename = db_dict[FileRecordAttribute.FILENAME.value]
#     if FileRecordAttribute.FILETYPE.value in db_dict:
#         retval.filetype = db_dict[FileRecordAttribute.FILETYPE.value]
#     if FileRecordAttribute.DATE_MODIFIED.value in db_dict:
#         retval.date_modified = db_dict[FileRecordAttribute.DATE_MODIFIED.value]
#     if FileRecordAttribute.SIZE_IN_BYTES.value in db_dict:
#         retval.size_in_bytes = db_dict[FileRecordAttribute.SIZE_IN_BYTES.value]
#
#     return retval

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
