import json
import os
import sys
import argparse
import boto3
from typing import List
from boto3.dynamodb.conditions import Attr
from botocore.client import Config
from urllib.parse import urlencode

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "lambdas", "layers", "util")
sys.path.append(SOURCE_PATH)

# Needed to be defined here after joining path to the util location.
from util import dynamodb, agha

DYNAMODB_ARCHIVE_RESULT_TABLE_NAME = "agha-gdr-result-bucket-archive"
DYNAMODB_ARCHIVE_STAGING_TABLE_NAME = "agha-gdr-staging-bucket-archive"
DYNAMODB_ARCHIVE_STORE_TABLE_NAME = "agha-gdr-store-bucket-archive"
DYNAMODB_ETAG_TABLE_NAME = "agha-gdr-e-tag"
DYNAMODB_RESULT_TABLE_NAME = "agha-gdr-result-bucket"
DYNAMODB_STAGING_TABLE_NAME = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE_NAME = "agha-gdr-store-bucket"
STAGING_BUCKET = "agha-gdr-staging-2.0"
RESULT_BUCKET = "agha-gdr-results-2.0"
STORE_BUCKET = "agha-gdr-store-2.0"

##############################################################################################################
# Configure generate_presigned_url to accept custom parameter.
# Taken from: https://stackoverflow.com/a/59057975/13137208
CUSTOM_S3_CLIENT = boto3.client("s3", config=Config(signature_version="s3v4"))


def is_custom(k):
    return k.lower().startswith("x-")  # To indicate 'x-' prefix is a custom param


def client_param_handler(*, params, context, **_kw):
    context["custom_params"] = {k: v for k, v in params.items() if is_custom(k)}
    return {k: v for k, v in params.items() if not is_custom(k)}


def request_param_injector(*, request, **_kw):
    if request.context["custom_params"]:
        request.url += "&" if "?" in request.url else "?"
        request.url += urlencode(request.context["custom_params"])


CUSTOM_S3_CLIENT.meta.events.register(
    "provide-client-params.s3.GetObject", client_param_handler
)
CUSTOM_S3_CLIENT.meta.events.register(
    "before-sign.s3.GetObject", request_param_injector
)
##############################################################################################################


def get_argument():
    parser = argparse.ArgumentParser(
        description="Generate pre-signed URLs for AGHA files."
    )
    parser.add_argument(
        "-o",
        "--out-file",
        default="presigned-urls.txt",
        help="Name of the output file. Default: presigned-urls.txt",
    )
    parser.add_argument(
        "--dryrun", default=False, action="store_true", help="Perform a dry run."
    )
    parser.add_argument(
        "-t",
        "--filetype",
        default=["FASTQ", "BAM", "CRAM", "VCF"],
        nargs="+",
        choices=["FASTQ", "BAM", "CRAM", "VCF"],
        help="Filetype to filter by (FASTQ, BAM, CRAM,VCF). Space separated if more than one. Default: all files",
    )
    parser.add_argument(
        "-s",
        "--study-ids",
        required=True,
        nargs="+",
        help="AGHA study ID(s) to retrieve files for. Space separated if more than one.",
    )
    parser.add_argument(
        "--release-id",
        required=True,
        help="A unique ID to identify the sharing request. "
        "This ID will be inserted as a custom parameter in the presignedUrl for access tracking.",
    )
    flagship_list = list(set(agha.FlagShip.list_flagship_enum()) - {"UNKNOWN", "TEST"})
    parser.add_argument(
        "-f",
        "--flagship",
        required=True,
        choices=flagship_list,
        help="Code of the flagship the sample belongs to.",
    )
    args = parser.parse_args()

    print("######################" * 6)
    print("Running the following")
    print(f"Release Id: {args.study_ids}")
    print(f"Flagship  : {args.flagship}")
    print(f"Study Ids : {args.study_ids}")
    print(f"Filetype  : {args.filetype}")
    print(f"OutFile   : {args.out_file}")
    print(f"DryRun    : {args.dryrun}")
    print("######################" * 6)

    return args


def generate_presign_s3_url(
    agha_study_id_list: List[str],
    flagship: str,
    filetype_list: List[str],
    dry_run: bool,
    out_file: str,
    release_id: str,
):
    sort_key_flagship_prefix = agha.FlagShip.from_name(flagship).preferred_code()
    filetype_list = run_filetype_sanitize(filetype_list)
    file_metadata_list = []

    # Find files with relevant study_id, flagship, and filetype
    for study_id in agha_study_id_list:

        # study_id filter
        filter_expr = Attr("agha_study_id").eq(study_id)

        # filetype filter
        if filetype_list:
            filetype_attr_expr = Attr("filetype").eq(
                filetype_list[0]
            )  # Init expression
            for filetype in filetype_list[1:]:
                filetype_attr_expr = filetype_attr_expr | Attr("filetype").eq(
                    filetype
                )  # Appending expression
            filter_expr = filter_expr & filetype_attr_expr

        # query to dydb
        file_list = dynamodb.get_batch_item_from_pk_and_sk(
            table_name=DYNAMODB_STORE_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
            sort_key_prefix=sort_key_flagship_prefix,
            filter_expr=filter_expr,
        )
        file_metadata_list.extend(file_list)

        # Logging
        sort_key_list = [metadata["sort_key"] for metadata in file_list]
        print(
            f"File with matching study_id ({study_id}) and flagship ({sort_key_flagship_prefix}) are: {json.dumps(sort_key_list, indent=4)}"
        )

    if dry_run:
        return

    # Fetch additional field needed
    for file_metadata in file_metadata_list:
        s3_key = file_metadata["sort_key"]

        # Generate presigned-url
        presigned_url = CUSTOM_S3_CLIENT.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": STORE_BUCKET,
                "Key": s3_key,
                "x-releaseId": release_id,  # Prefix with X to indicate for custom param.
            },
            ExpiresIn=604800,  # 7 full days
        )
        file_metadata["presigned_url"] = presigned_url

        # Get fileSize
        object_s3_metadata = dynamodb.get_item_from_exact_pk_and_sk(
            table_name=DYNAMODB_STORE_TABLE_NAME,
            partition_key=dynamodb.FileRecordPartitionKey.FILE_RECORD.value,
            sort_key=s3_key,
        )
        if object_s3_metadata["Count"] != 1:
            raise ValueError("Cannot retrieve size")
        size_in_bytes = object_s3_metadata["Items"][0]["size_in_bytes"]
        file_metadata["size_in_bytes"] = size_in_bytes

    # Write output file
    f = open(out_file, "w")
    f.write("agha_study_id\tfilename\tsize_in_bytes\tchecksum\tpresigned_url\n")
    for metadata in file_metadata_list:
        # parse data
        study_id = metadata["agha_study_id"]
        filename = metadata["filename"]
        checksum = metadata["provided_checksum"]
        presigned_url = metadata["presigned_url"]
        size_in_bytes = metadata["size_in_bytes"]

        f.write(
            f"{study_id}\t{filename}\t{size_in_bytes}\t{checksum}\t{presigned_url}\n"
        )
    f.close()

    print(f"PresignUrl generated. Checkout: {out_file}")


def run_filetype_sanitize(filetype_list: List[str]):
    result = set()
    for filetype in filetype_list:
        filetype_enum = agha.FileType.from_enum_name(filetype)
        # Raise for unknown filetype
        if filetype_enum == agha.FileType.UNSUPPORTED:
            raise ValueError

        # Add corresponding index when necessary
        if agha.FileType.is_indexable(filetype):
            if filetype_enum == agha.FileType.VCF:
                result.add(agha.FileType.VCF_INDEX.get_name())
            elif filetype_enum == agha.FileType.BAM:
                result.add(agha.FileType.BAM_INDEX.get_name())
            elif filetype_enum == agha.FileType.CRAM:
                result.add(agha.FileType.CRAM_INDEX.get_name())

        result.add(filetype_enum.get_name())
    return list(result)


if __name__ == "__main__":
    args = get_argument()
    generate_presign_s3_url(
        agha_study_id_list=args.study_ids,
        flagship=args.flagship,
        filetype_list=args.filetype,
        dry_run=args.dryrun,
        out_file=args.out_file,
        release_id=args.release_id,
    )
