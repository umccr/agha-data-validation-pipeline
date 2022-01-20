import json
import os.path
import logging
import boto3
from boto3.dynamodb.conditions import Key

from .agha import FileType
import util

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


class FileRecordPartitionKey(Enum):
    FILE_RECORD = 'TYPE:FILE'
    MANIFEST_FILE_RECORD = 'TYPE:MANIFEST'
    STATUS_MANIFEST = 'STATUS:MANIFEST'

    def __str__(self):
        return self.value


class ManifestStatusCheckValue(Enum):
    FAIL = 'FAIL'
    PASS = 'PASS'
    NOT_COMPLETED = 'NOT_COMPLETED'

    def __str__(self):
        return self.value


class ManifestStatusCheckRecord:

    def __init__(self,
                 partition_key=FileRecordPartitionKey.STATUS_MANIFEST.value,
                 sort_key="",
                 status=ManifestStatusCheckValue.NOT_COMPLETED.value,
                 additional_information="",
                 date_modified=util.get_datetimestamp()):
        self.partition_key = partition_key
        self.sort_key = sort_key
        self.status = status
        self.additional_information = additional_information
        self.date_modified = date_modified

    def create_archive_dictionary(self, archive_log: str):
        archive_dict = self.__dict__.copy()
        new_sort_key = archive_dict['sort_key'] + ':' + util.get_datetimestamp()
        archive_dict['sort_key'] = new_sort_key
        archive_dict['archive_log'] = archive_log

        return archive_dict


# Record Attribute for STAGING and STORE bucket
class FileRecord:
    """
    The DynamoDB table is configured with a mandatory composite key composed of two elements:
    - partition_key: The type of file indicate it stores the file properties.
    - sort_key: Most Probably the s3_key.
    All other attributes are self explanatory (although some can automatically be derived from the object key)
    """

    def __init__(self,
                 partition_key="",
                 sort_key="",
                 s3_key="",
                 bucket_name="",
                 etag="",
                 filename="",
                 filetype="",
                 date_modified="",
                 size_in_bytes=0):
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
        e_tag = s3_record.etag
        date_modified = util.get_datetimestamp()
        size_in_bytes = s3_record.size_in_bytes
        filename = os.path.basename(s3_key)
        filetype = FileType.from_name(filename)

        # Additional Field
        partition_key = FileRecordPartitionKey.FILE_RECORD.value
        sort_key = s3_key

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
                 partition_key="",
                 sort_key="",
                 bucket_name="",
                 s3_key="",
                 etag="",
                 filename="",
                 filetype="",
                 date_modified="",
                 size_in_bytes=0,
                 archive_log=""):
        super().__init__(
            partition_key=partition_key,
            sort_key=sort_key,
            bucket_name=bucket_name,
            s3_key=s3_key,
            etag=etag,
            filename=filename,
            filetype=filetype,
            date_modified=date_modified,
            size_in_bytes=size_in_bytes
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_file_record_from_file_record(cls, file_record: FileRecord, archive_log):
        sort_key = file_record.sort_key + ':' + util.get_datetimestamp()

        return cls(
            partition_key=file_record.partition_key,
            sort_key=sort_key,
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
    - partition_key: The type of file indicate it stores the file properties.
    - sort_key: Most Probably the s3_key
    All other attributes are self explanatory (although some can automatically be derived from the object key)
    """

    def __init__(self,
                 partition_key="",
                 sort_key="",
                 flagship="",
                 filename="",
                 filetype="",
                 submission="",
                 date_modified="",
                 provided_checksum="",
                 agha_study_id="",
                 is_in_manifest="",
                 validation_status=""):
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
                 partition_key="",
                 sort_key="",
                 flagship="",
                 filename="",
                 filetype="",
                 submission="",
                 date_modified="",
                 provided_checksum="",
                 agha_study_id="",
                 validation_status="",
                 is_in_manifest="",
                 archive_log=""
                 ):
        super().__init__(
            partition_key=partition_key,
            sort_key=sort_key,
            flagship=flagship,
            filename=filename,
            filetype=filetype,
            submission=submission,
            date_modified=date_modified,
            provided_checksum=provided_checksum,
            agha_study_id=agha_study_id,
            is_in_manifest=is_in_manifest,
            validation_status=validation_status
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_manifest_record_from_manifest_record(cls, manifest_record: ManifestFileRecord, archive_log):
        sort_key = manifest_record.sort_key + ':' + util.get_datetimestamp()

        return cls(
            partition_key=manifest_record.partition_key,
            sort_key=sort_key,
            flagship=manifest_record.flagship,
            filename=manifest_record.filename,
            filetype=manifest_record.filetype,
            submission=manifest_record.submission,
            date_modified=util.get_datetimestamp(),
            provided_checksum=manifest_record.provided_checksum,
            agha_study_id=manifest_record.agha_study_id,
            validation_status=manifest_record.validation_status,
            is_in_manifest=manifest_record.is_in_manifest,
            archive_log=archive_log
        )


########################################################################################################################
# Table: agha-gdr-result-bucket, agha-gdr-result-bucket-archive

# agha-gdr-result-bucket, agha-gdr-result-bucket-archive may contain 3 type of different class
# 1. FileRecord class (defined above) that also be used as staging/store bucket file record
# 2. ResultRecord class (defined below) to hold the data output of the test. ResultRecord class may contain 2 types. \
#       Explanation defined at the docstring class

class ResultPartitionKey(Enum):
    FILE = "FILE"
    STATUS = "STATUS"
    DATA = "DATA"

    def __str__(self):
        return self.value

    @staticmethod
    def create_partition_key_with_result_prefix(data_type, check_type):
        return f'{data_type}:{check_type}'


class ResultRecord:
    """
    sort_key: Most probably the s3 of the original file
    partition_key:
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


class ArchiveResultRecord(ResultRecord):
    def __init__(self,
                 partition_key="",
                 sort_key="",
                 date_modified="",
                 value="",
                 archive_log=""
                 ):
        super().__init__(
            partition_key=partition_key,
            sort_key=sort_key,
            date_modified=date_modified,
            value=value
        )
        self.archive_log = archive_log

    @classmethod
    def create_archive_result_record_from_result_record(cls, result_record: ResultRecord, archive_log):
        sort_key = result_record.sort_key + ':' + util.get_datetimestamp()

        return cls(
            partition_key=result_record.partition_key,
            sort_key=sort_key,
            date_modified=util.get_datetimestamp(),
            value=result_record.value,
            archive_log=archive_log
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


def delete_record_from_record_class(table_name: str, record):
    """
    This will delete record from dynamodb with given table_name and record class.
    Record class MUST have 'partition_key' and 'sort_key' as their property as deletetion are based on those
    :param table_name: DynamoDb table name
    :param record: A class that contain 'partition_key' and 'sort_key' which will be deleted based on.
    :return:
    """

    ddb = get_resource()
    tbl = ddb.Table(table_name)

    delete_res = tbl.delete_item(
        Key={
            'partition_key': record.partition_key,
            'sort_key': record.sort_key
        },
        ReturnValues='ALL_OLD'
    )

    if 'Attributes' in delete_res:
        return delete_res['Attributes']
    else:
        return ValueError(f"partition_key: {record.partition_key}, sort_key: {record.sort_key}\
         has not been successfully deleted from table '{table_name}'")


def delete_record_from_dictionary(table_name: str, dictionary: dict):
    """
    This will delete record from dynamodb with given table_name and record class.
    Record class MUST have 'partition_key' and 'sort_key' as their property as deletetion are based on those
    :param table_name: DynamoDb table name
    :param record: A class that contain 'partition_key' and 'sort_key' which will be deleted based on.
    :return:
    """

    ddb = get_resource()
    tbl = ddb.Table(table_name)

    delete_res = tbl.delete_item(
        Key={
            'partition_key': dictionary['partition_key'],
            'sort_key': dictionary['sort_key']
        },
        ReturnValues='ALL_OLD'
    )

    if 'Attributes' in delete_res:
        return delete_res['Attributes']
    else:
        return ValueError(f"partition_key: {dictionary['partition_key'],}, sort_key: {dictionary['sort_key']}\
         has not been successfully deleted from table '{table_name}'")

def batch_delete_from_dictionary(table_name: str, dictionary_list: list):

    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for record in dictionary_list:
            key = {
                "partition_key":record['partition_key'],
                "sort_key":record['sort_key'],
            }
            batch.delete_item(Key=key)

def write_main_and_archive_record_from_class(main_table_name: str, archive_table_name: str, record_class,
                                             archive_log: str):
    dynamodb_resource = get_resource()

    dynamodb_table = dynamodb_resource.Table(main_table_name)
    record_dict = record_class.__dict__
    resp = dynamodb_table.put_item(Item=record_dict, ReturnValues='ALL_OLD')

    archive_dynamodb_table = dynamodb_resource.Table(archive_table_name)
    record_dict['sort_key'] = record_dict['sort_key'] + ':' + util.get_datetimestamp()
    record_dict['archive_log'] = archive_log
    resp = archive_dynamodb_table.put_item(Item=record_dict, ReturnValues='ALL_OLD')

    return resp


def write_record_from_class(table_name, record) -> dict:
    dynamodb_resource = get_resource()
    dynamodb_table = dynamodb_resource.Table(table_name)

    resp = dynamodb_table.put_item(Item=record.__dict__, ReturnValues='ALL_OLD')
    return resp


def write_record_from_dict(table_name, record_dict) -> dict:
    dynamodb_resource = get_resource()
    dynamodb_table = dynamodb_resource.Table(table_name)

    resp = dynamodb_table.put_item(Item=record_dict, ReturnValues='ALL_OLD')
    return resp


def batch_write_records(table_name: str, records: list):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for record in records:
            batch.put_item(Item=record.__dict__)


def batch_write_record_archive(table_name: str, records: list, archive_log: str):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for record in records:
            object = record.__dict__
            object["archive_log"] = archive_log
            object['sort_key'] = object['sort_key'] + ':' + util.get_datetimestamp()
            batch.put_item(Item=object)


def batch_write_objects(table_name: str, object_list: list):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for object in object_list:
            batch.put_item(Item=object)


def batch_write_objects_archive(table_name: str, object_list: list, archive_log: str):
    tbl = get_resource().Table(table_name)
    with tbl.batch_writer() as batch:
        for object in object_list:
            object["archive_log"] = archive_log
            batch.put_item(Item=object)


def get_item_from_pk(table_name: str, partition_key: str):
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    expr = Key(FileRecordAttribute.PARTITION_KEY.value).eq(partition_key)

    response = tbl.query(
        KeyConditionExpression=expr
    )

    return response


def get_item_from_pk_and_sk(table_name: str, partition_key: str, sort_key_prefix: str, filter: str = None):
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    key_expr = Key(FileRecordAttribute.PARTITION_KEY.value).eq(partition_key) & Key(
        FileRecordAttribute.SORT_KEY.value).begins_with(sort_key_prefix)

    func_parameter = {
        "KeyConditionExpression": key_expr
    }

    if filter:
        func_parameter["FilterExpression"] = filter

    response = tbl.query(**func_parameter)

    return response


def get_item_from_exact_pk_and_sk(table_name: str, partition_key: str, sort_key: str):
    ddb = get_resource()
    tbl = ddb.Table(table_name)

    expr = Key(FileRecordAttribute.PARTITION_KEY.value).eq(partition_key) & Key(
        FileRecordAttribute.SORT_KEY.value).eq(sort_key)

    response = tbl.query(
        KeyConditionExpression=expr
    )

    return response


def get_field_list_from_dynamodb_record(table_name: str, field_name: str, partition_key: str, sort_key_prefix):
    res = get_item_from_pk_and_sk(table_name=table_name, partition_key=partition_key, sort_key_prefix=sort_key_prefix)

    field_list = []
    for record in res["Items"]:
        field_list.append(record[field_name])

    return field_list
