import os
import logging
import util.s3 as s3
import util.dynamodb as dy
import util.agha as agha
import util as util
import pandas as pd
import json
from collections import defaultdict
from typing import Dict, List

import botocore

# Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_NAME_STORE_OLD = "agha-gdr-store"
BUCKET_NAME_STORE = "agha-gdr-store-2.0"
BUCKET_NAME_STAGING = "agha-gdr-staging-2.0"

DYN_TABLE_STORE = "agha-gdr-store-bucket"
DYN_TABLE_STAGING = "agha-gdr-staging-bucket"
DYN_PK_MANIFEST = "TYPE:MANIFEST"
DYN_PK_FILE = "TYPE:FILE"

LEGACY_SUBS = {
    "acute_care_genomics": "2019-05-16",
    "brain_malformations": "2019-07-22",
    "leukodystrophies": "2019-08-12",
    "mitochondrial_disease": "2019-04-18",
}

flagships = [
    "Leuko",
    "GI",
    "EE",
    "BM",
    "HIDDEN",
    "chILDRANZ",
    "AC",
    "Mito",
    "KidGen",
    "ID",
    "ICCon",
]  # .append('Cardiac')

submission_skip_list = [
    "GI/2020-06-04/",
    "GI/2019-08-27/",
    "Mito/2019-04-18/",
    "HIDDEN/2019-09-27/",
]


class Report:
    INDENT = "    "

    def __init__(self, filename: str = "old2new_store_comparison.txt"):
        self.msg_buffer = list()
        self.all_messages = list()
        self.filename = filename

    def add_msg(self, msg: str, level: int = 0):
        for p in msg.split("\n"):
            self.msg_buffer.append(f"{self.INDENT * level}{p}")

    def flush(self):
        f = open(self.filename, "a")

        print(f"Writing {len(self.msg_buffer)} messages:")
        for message in self.msg_buffer:
            f.write(message)
            f.write("\n")
            print(message)
        f.close()

        # reset buffer
        self.all_messages.extend(self.msg_buffer)
        self.msg_buffer.clear()

    def get_messages(self):
        ret = list()
        ret.extend(self.all_messages)
        ret.extend(self.msg_buffer)
        return ret


def write_message_to_file(filename: str = "output_result.txt", message_list=None):
    f = open(filename, "w")

    for message in message_list:
        f.write(message)
        f.write("\n")


def get_fs_and_sub_from_key(key: str) -> tuple[str, str]:
    parts = key.split("/")
    return parts[0], parts[1]


def get_filename_from_key(key: str) -> str:
    return os.path.basename(key)


def is_legacy_sub(prefix: str):
    fs = prefix.strip("/")
    return fs in LEGACY_SUBS.keys()


def check_all():
    validation_report.add_msg(
        f"Comparison Report between old/new store ({BUCKET_NAME_STORE_OLD} / {BUCKET_NAME_STORE})"
    )

    roots = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STORE_OLD, prefix="")
    # roots = ['ACG/', 'acute_care_genomics/']  # TODO: remove
    # roots = ['MITO/']
    all_prefixes = list()
    for root in roots:
        if is_legacy_sub(root):
            all_prefixes.append(root)
            continue
        all_prefixes.extend(
            s3.aws_s3_ls(bucket_name=BUCKET_NAME_STORE_OLD, prefix=root)
        )
    validation_report.add_msg(
        f"Total number of prefixes (submissions): {len(all_prefixes)}", 1
    )

    prefix_by_fs_map = group_by_flagship(all_prefixes)
    for fs in prefix_by_fs_map.keys():
        validation_report.add_msg("\n\n\n", 1)
        validation_report.add_msg(
            f"################################################################################"
        )
        validation_report.add_msg(f"Checking FlagShip {fs}")
        validation_report.add_msg(
            f"################################################################################"
        )
        for prefix in prefix_by_fs_map[fs]:
            # if prefix not in ['MITO/2019-08-14/']:  # TODO remove
            #     continue
            prefix_new = get_new_store_prefix(prefix)
            validation_report.add_msg(
                f"\n\n================================================================================",
                1,
            )
            if exists_submission(bucket=BUCKET_NAME_STORE, prefix=prefix_new):
                validation_report.add_msg(
                    f"Checking {BUCKET_NAME_STORE_OLD}/{prefix} against {BUCKET_NAME_STORE}/{prefix_new}",
                    1,
                )
                compare_s3_listing(
                    bucket_1=BUCKET_NAME_STORE_OLD,
                    prefix_1=prefix,
                    bucket_2=BUCKET_NAME_STORE,
                    prefix_2=prefix_new,
                )
            else:
                validation_report.add_msg(
                    f"WARNING: could not find new submission for {BUCKET_NAME_STORE_OLD}/{prefix}",
                    1,
                )
            validation_report.flush()


def group_by_flagship(prefixes: list) -> Dict[str, list]:
    fs_map = defaultdict(list)
    for prefix in prefixes:
        fs_prefix = prefix.split("/")[0]
        fs = agha.FlagShip.from_name(fs_prefix)
        fs_map[fs.name].append(prefix)
    return fs_map


def get_new_store_prefix(prefix: str) -> str:
    fs, sub = get_fs_and_sub_from_key(prefix)
    pfs = agha.FlagShip.from_name(fs)
    if is_legacy_sub(fs):
        new_prefix = f"{pfs.preferred_code()}/{LEGACY_SUBS[fs]}/"
    else:
        new_prefix = f"{pfs.preferred_code()}/{sub}/"
    return new_prefix


def exists_submission(bucket: str, prefix: str):
    s3_client = util.get_client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=2)

    key_count = response["KeyCount"]
    return key_count > 0


# def check_flagship(flagship):
#     validation_report.add_msg(f'Comparison {flagship} between {BUCKET_NAME_STORE_OLD} and {BUCKET_NAME_STORE}')
#
#     # List all submission in the flagship
#     ls_flagship = flagship + '/'
#     submission_s3_list = s3.aws_s3_ls(BUCKET_NAME_STORE, ls_flagship)
#     validation_report.add_msg(f'Number of submissions for {flagship} in {BUCKET_NAME_STORE}: {len(submission_s3_list)}')
#
#     submission_map = map_submissions(flagship, submission_s3_list)
#
#     for prefix in submission_map.keys():
#         if prefix in submission_skip_list:
#             validation_report.add_msg(f"Skipping {prefix}", 1)
#             continue
#         else:
#             validation_report.add_msg(f"Processing {prefix}", 1)
#
#         compare_s3_listing(bucket_1=BUCKET_NAME_STORE_OLD,
#                            prefix_1=submission_map[prefix],
#                            bucket_2=BUCKET_NAME_STORE,
#                            prefix_2=prefix)


# def check_submission_legacy(report_message, submission_prefix):
#     need_double_check_array = []
#     print(f'Checking submission {submission_prefix}')
#     report_message.append(f'\nSubmission: {submission_prefix}')
#     # Create a list of files for particular submission
#     staging_bucket_list = s3.get_s3_object_metadata(bucket_name=BUCKET_NAME_STAGING,
#                                                     directory_prefix=submission_prefix)
#     # Current data
#     all_file = [obj['Key'] for obj in staging_bucket_list]
#     manifest_file = []
#     index_file = []
#     compress_file = []
#     md5_file = []
#     # Processing data
#     indexable_file = []
#     compressible_file = []
#     compressible_and_indexable_file = []
#     # Filling array above
#     for each_staging_object in staging_bucket_list:
#         staging_key = each_staging_object['Key']
#         filetype = agha.FileType.from_name(staging_key).get_name()
#
#         if filetype == agha.FileType.MANIFEST.get_name():
#             manifest_file.append(staging_key)
#
#         if agha.FileType.is_index_file(staging_key):
#             index_file.append(staging_key)
#
#         if agha.FileType.is_compress_file(staging_key):
#             compress_file.append(staging_key)
#
#         if agha.FileType.is_compressable(filetype) and not agha.FileType.is_compress_file(staging_key):
#             compressible_file.append(staging_key)
#
#         if agha.FileType.is_indexable(filetype):
#             indexable_file.append(staging_key)
#
#         if agha.FileType.is_compressable(filetype) and not agha.FileType.is_compress_file(staging_key) and \
#                 agha.FileType.is_indexable(filetype) and not agha.FileType.is_index_file(staging_key):
#             compressible_and_indexable_file.append(staging_key)
#
#         if agha.FileType.is_md5_file(staging_key):
#             md5_file.append(staging_key)
#     # TODO: Uncomment below for statistic report to be printed as the result
#     # report_message.append(f'Existing file at {submission_prefix}')
#     # report_message.append(f'Total number of files in staging bucket:{len(staging_bucket_list)}')
#     # report_message.append(f'Total number of manifest file: {len(manifest_file)}')
#     # report_message.append(f'Total number of index file: {len(index_file)}')
#     # report_message.append(f'Total number of compress file: {len(compress_file)}')
#     # report_message.append(f'More information about existing file:')
#     # report_message.append(f'Total number of compressible file: {len(compressible_file)}')
#     # report_message.append(f'Total number of indexable file: {len(indexable_file)}')
#     # report_message.append(
#     #     f'Total number of compressible and indexable file: {len(compressible_and_indexable_file)}')
#     # Calculating More value below
#     should_not_exist_after_transfer = len(all_file) - len(manifest_file) - len(index_file) - len(
#         md5_file) - len(compressible_file)
#     if should_not_exist_after_transfer > 0:
#         report_message.append(
#             f'If s3 transfer have initiated, {should_not_exist_after_transfer} number of file should not exist.')
#
#         attention_dict = {
#             'submission': submission_prefix,
#             'no_file_not_transferred_due_to_compressing': should_not_exist_after_transfer,
#             'file_not_transferred_due_to_compressing': compressible_file,
#             'file_should_be_transferred': list(
#                 set(all_file) - set(manifest_file) - set(index_file) - set(md5_file) - set(compressible_file))
#         }
#
#         need_double_check_array.append(attention_dict)
#     report_message.append(f'\n'
#                           f'Some file is submitted not in compressed format. In this case, the pipeline will create\n'
#                           f'a compression and will only copy the created compression to the store bucket.\n'
#                           f'Please check the following if the compression work as intended.\n')
#     report_message.append(json.dumps(need_double_check_array, indent=4))


def compare_s3_listing(bucket_1, prefix_1, bucket_2, prefix_2):
    validation_report.add_msg(f"Comparing S3 bucket content", 2)
    s3_list_1 = s3.get_s3_object_metadata(
        bucket_name=bucket_1, directory_prefix=prefix_1
    )
    s3_list_2 = s3.get_s3_object_metadata(
        bucket_name=bucket_2, directory_prefix=prefix_2
    )

    validation_report.add_msg(f"Object count {bucket_1}: {len(s3_list_1)}", 2)
    validation_report.add_msg(f"Object count {bucket_2}: {len(s3_list_2)}", 2)

    # Create DataFrames from S3 listing
    df_1 = pd.DataFrame.from_records(s3_list_1)
    df_2 = pd.DataFrame.from_records(s3_list_2)
    # Exclude columns we don't want to compare
    df_1.pop("LastModified")
    df_1.pop("StorageClass")
    df_2.pop("LastModified")
    df_2.pop("StorageClass")
    # adjust S3 key (ignore prefix as that would likely have changed)
    df_1["Key"] = df_1["Key"].apply(get_filename_from_key)
    df_2["Key"] = df_2["Key"].apply(get_filename_from_key)

    # Check if the contents are identical (should not be the case)
    if df_1.equals(df_2):
        validation_report.add_msg(f"Data in {bucket_1} is identical to {bucket_2}", 2)
        return

    ###
    # Start comparing the differences

    # basic stats first
    diff_df = df_1.merge(df_2, how="outer", indicator=True)
    df_both = diff_df.loc[diff_df["_merge"] == "both"]
    validation_report.add_msg(f"Number of identical records: {df_both.shape[0]}", 2)
    df_1_only = diff_df.loc[diff_df["_merge"] == "left_only"]
    validation_report.add_msg(
        f"Number of records only in {bucket_1}: {df_1_only.shape[0]}", 2
    )
    df_2_only = diff_df.loc[diff_df["_merge"] == "right_only"]
    validation_report.add_msg(
        f"Number of records only in {bucket_2}: {df_2_only.shape[0]}", 2
    )

    # check the records only in the old bucket
    validation_report.add_msg(f"\nChecking file only in {bucket_1}", 2)
    key_1_list_explained_cnt = 0
    df_1_only_issues = pd.DataFrame()
    tcnt = 0
    for index, row in df_1_only.iterrows():
        tcnt += 1
        if row.Key.endswith(".md5"):
            # checksums are supposed to be in the manifest, not separate files, ignore
            key_1_list_explained_cnt += 1
            continue
        if agha.FileType.is_index_file(row.Key):
            # we recreate indexes, so we can ignore the provided files
            key_1_list_explained_cnt += 1
            continue
        if not agha.FileType.is_compress_file(
            row.Key
        ) and agha.FileType.is_compressable_file(row.Key):
            # we automatically compress, so there should be a compressed equivalent
            if f"{row.Key}.gz" in df_2_only["Key"].values:
                # a file we compressed, ignore
                key_1_list_explained_cnt += 1
                continue
        if row.Key == "manifest.txt":
            # manifest file (not yet in store 2.0) # TODO: change once manifest is added to store-2.0
            key_1_list_explained_cnt += 1
            continue
        # uncompressed file an no compressed equivalent, warn
        # potential_issues_1.append(F"{key_1}")
        df_1_only_issues = df_1_only_issues.append(row)

    validation_report.add_msg(
        f"{key_1_list_explained_cnt}/{df_1_only.shape[0]} differences explainable ({df_1_only_issues.shape[0]} unexplained).",
        3,
    )
    if df_1_only_issues.shape[0] > 0:
        validation_report.add_msg(f"Unexplained differences:", 3)
        validation_report.add_msg(
            f"--------------------------------------------------------------------------------",
            3,
        )
        validation_report.add_msg(f"{df_1_only_issues.to_string(index=False)}", 3)
        validation_report.add_msg(
            f"--------------------------------------------------------------------------------",
            3,
        )

    # validation_report.add_msg(f"Checking file only in {bucket_1}", 2)
    # key_1_list_explained_cnt = 0
    # potential_issues_1 = list()
    # for key_1 in key_1_list:
    #     if key_1.endswith('.md5'):
    #         # checksums are supposed to be in the manifest, not separate files, ignore
    #         key_1_list_explained_cnt += 1
    #         continue
    #     if agha.FileType.is_index_file(key_1):
    #         # we recreate indexes, so we can ignore the provided files
    #         key_1_list_explained_cnt += 1
    #         continue
    #     if not agha.FileType.is_compress_file(key_1) and agha.FileType.is_compressable_file(key_1):
    #         # we automatically compress, so there should be a compressed equivalent
    #         if f"{key_1}.gz" in key_2_list:
    #             # a file we compressed, ignore
    #             key_1_list_explained_cnt += 1
    #             continue
    #     # uncompressed file an no compressed equivalent, warn
    #     potential_issues_1.append(F"{key_1}")
    #
    # validation_report.add_msg(f"{key_1_list_explained_cnt}/{len(key_1_list)} differences explainable.", 3)
    # if len(potential_issues_1) > 0:
    #     validation_report.add_msg(f"Unexplained differences: {potential_issues_1}", 3)

    # check the records only in the new bucket
    validation_report.add_msg(f"\nChecking file only in {bucket_2}", 2)
    key_2_list_explained_cnt = 0
    df_2_only_issues = pd.DataFrame()
    tcnt = 0
    for index, row in df_2_only.iterrows():
        tcnt += 1
        if row.Key.endswith(".md5"):
            # checksums are supposed to be in the manifest, not separate files, ignore
            key_2_list_explained_cnt += 1
            continue
        if agha.FileType.is_index_file(row.Key):
            # we recreate indexes, so we can ignore the provided files
            key_2_list_explained_cnt += 1
            continue
        if agha.FileType.is_compress_file(
            row.Key
        ) and agha.FileType.is_compressable_file(row.Key):
            # we automatically compress, so there should be a compressed equivalent
            if f"{row.Key}".rstrip(".gz") in df_1_only["Key"].values:
                # a file we compressed, ignore
                key_2_list_explained_cnt += 1
                continue
        df_2_only_issues = df_2_only_issues.append(row)

    validation_report.add_msg(
        f"{key_2_list_explained_cnt}/{df_2_only.shape[0]} differences explainable ({df_2_only_issues.shape[0]} unexplained).",
        3,
    )
    if df_2_only_issues.shape[0] > 0:
        validation_report.add_msg(f"Unexplained differences:", 3)
        validation_report.add_msg(
            f"--------------------------------------------------------------------------------",
            3,
        )
        validation_report.add_msg(f"{df_2_only_issues.to_string(index=False)}", 3)
        validation_report.add_msg(
            f"--------------------------------------------------------------------------------",
            3,
        )

    # validation_report.add_msg(f"Checking files only in {bucket_2}", 2)
    # key_2_list_explained_cnt = 0
    # potential_issues_2 = list()
    # for key_2 in key_2_list:
    #     if key_2.endswith('.md5'):
    #         # checksums are supposed to be in the manifest, not separate files, ignore
    #         key_2_list_explained_cnt += 1
    #         continue
    #     if agha.FileType.is_index_file(key_2):
    #         # we recreate indexes, so we expect those to show up
    #         key_2_list_explained_cnt += 1
    #         continue
    #     if agha.FileType.is_compress_file(key_2) and agha.FileType.is_compressable_file(key_2):
    #         # we automatically compress, so there should be a compressed equivalent
    #         if key_2.rstrip('.gz') in key_1_list:
    #             # a file we compressed, ignore
    #             key_2_list_explained_cnt += 1
    #             continue
    #     # uncompressed file an no compressed equivalent, warn
    #     potential_issues_2.append(F"{key_2}")
    #
    # validation_report.add_msg(f"{key_2_list_explained_cnt}/{len(key_2_list)} differences explainable.", 3)
    # if len(potential_issues_2) > 0:
    #     validation_report.add_msg(f"Unexplained differences: {potential_issues_2}", 3)


def get_flagship_from_s3_key(s3_key: str) -> agha.FlagShip:
    # simply return first path element
    # TODO: may need further checks
    root_part = s3_key.split("/")[0]
    return agha.FlagShip.from_name(root_part)


def read_manifest_db_records(table_name, prefix):
    db_resp = dy.get_item_from_pk_and_sk(table_name, DYN_PK_MANIFEST, prefix)

    for item in db_resp.get("Items"):
        print(item["filename"])
    print(db_resp)


if __name__ == "__main__":
    validation_report = Report()
    check_all()

    print(f"Total messages: {len(validation_report.get_messages())}")
    # for msg in validation_report.get_messages():
    #     print(msg)
