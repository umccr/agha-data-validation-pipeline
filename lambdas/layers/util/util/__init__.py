import json
import logging
import os
import subprocess
import sys
import decimal
import datetime

import boto3
import pytz

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)

FEXT_FASTQ = {".fq", ".fq.gz", ".fastq", ".fastq.gz"}
FEXT_BAM = {".bam"}
FEXT_CRAM = {".cram"}
FEXT_VCF = {".vcf.gz", ".gvcf.gz"}
FEXT_ACCEPTED = {*FEXT_FASTQ, *FEXT_BAM, *FEXT_CRAM, *FEXT_VCF}

MELBOURNE_TZ = "Australia/Melbourne"  # Options: `print(pytz.all_timezones)`
TIME_ZONE = pytz.timezone(MELBOURNE_TZ)


class StreamHandlerNewLine(logging.StreamHandler):
    """Override emit so that we can use '\n' in file logs"""

    def emit(self, record):
        try:
            msg = self.format(record)
            msg = msg.replace("\r", "\n")
            stream = self.stream
            # issue 35046: merged two stream.writes into one.
            stream.write(msg + self.terminator)
            self.flush()
        except RecursionError:  # See issue 36272
            raise
        except Exception:
            self.handleError(record)


class FileHandlerNewLine(logging.FileHandler):
    """Override emit so that we can use '\n' in file logs"""

    def emit(self, record):
        if self.stream is None:
            if self.mode != "w" or not self._closed:
                self.stream = self._open()
        if self.stream:
            StreamHandlerNewLine.emit(self, record)


def get_environment_variable(name):
    if not (value := os.environ.get(name)):
        LOGGER.critical(f"could not find env variable {name}")
        sys.exit(1)
    return value


def get_client(service_name, region_name=None):
    try:
        response = boto3.client(service_name, region_name=region_name)
    except Exception as err:
        LOGGER.critical(f"could not get AWS client for {service_name}:\r{err}")
        sys.exit(1)
    return response


def get_resource(service_name, region_name=None):
    try:
        response = boto3.resource(service_name, region_name=region_name)
    except Exception as err:
        LOGGER.critical(f"could not get AWS resource for {service_name}:\r{err}")
        sys.exit(1)
    return response


def get_dynamodb_table_resource(dynamodb_table, region_name=None):
    return get_resource("dynamodb", region_name=region_name).Table(dynamodb_table)


def get_ssm_parameter(name, ssm_client, with_decryption=False):
    try:
        response = ssm_client.get_parameter(
            Name=name,
            WithDecryption=with_decryption,
        )
    except ssm_client.exceptions.ParameterNotFound:
        LOGGER.critical(f"could not find SSM parameter '{name}'")
        sys.exit(1)
    if "Parameter" not in response:
        LOGGER.critical(f"SSM response for '{name}' was malformed")
        sys.exit(1)
    return response["Parameter"]["Value"]


def get_s3_object_metadata(bucket, prefix, client_s3):
    results = list()
    response = client_s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if not (object_mdata := response.get("Contents")):
        return False
    else:
        results.extend(object_mdata)
    while response["IsTruncated"]:
        token = response["NextContinuationToken"]
        response = client_s3.list_objects_v2(
            Bucket=bucket, Prefix=prefix, ContinuationToken=token
        )
        results.extend(object_mdata)
    return results


def get_context_info(context):
    attributes = {
        "function_name",
        "function_version",
        "invoked_function_arn",
        "memory_limit_in_mb",
        "aws_request_id",
        "log_group_name",
        "log_stream_name",
    }
    return {attr: getattr(context, attr) for attr in attributes}


def get_datetimestamp():
    return f"{get_datestamp()}_{get_timestamp()}"


def get_timestamp():
    return "{:%H%M%S}".format(datetime.datetime.now(TIME_ZONE))


def get_datestamp():
    return "{:%Y%m%d}".format(datetime.datetime.now(TIME_ZONE))


def execute_command(command):
    process_result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=True,
        encoding="utf-8",
    )
    return process_result


class JsonSerialEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)

        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()

        return super(JsonSerialEncoder, self).default(o)


# TODO: Replace DecimalEncoder and json_serial (below) with the JsonSerialEncoder class (above)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()

    raise TypeError("Type %s not serializable" % type(obj))


def replace_record_decimal_object(record):
    for k in record:
        if isinstance(record[k], decimal.Decimal):
            record[k] = int(record[k]) if record[k] % 1 == 0 else float(record[k])
    return record


def get_record_from_given_field_and_panda_df(
    panda_df, fieldname_lookup: str, fieldvalue_lookup: str
):
    file_info = panda_df.loc[panda_df[fieldname_lookup] == fieldvalue_lookup].iloc[0]
    return file_info


def call_lambda(lambda_arn: str, payload: dict):
    lambda_client = boto3.client("lambda")
    response = lambda_client.invoke(
        FunctionName=lambda_arn, InvocationType="Event", Payload=json.dumps(payload)
    )
    return response
