import sys
import os
import argparse
import pandas as pd
import json

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(DIR_PATH, "..", "..", "..", "lambdas", "layers", "util")
sys.path.append(SOURCE_PATH)

# Needs to be declared under the path append above
import util.dynamodb as dynamodb
import util as util

STAGING_BUCKET_NAME = "agha-gdr-staging-2.0"
RESULTS_BUCKET_NAME = "agha-gdr-results-2.0"
STORE_BUCKET_NAME = "agha-gdr-store-2.0"

DYNAMODB_STAGING_TABLE = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"


class TxtReport:
    INDENT = "    "

    def __init__(self, filename: str = ""):
        self.filename = f"{filename}_MANIFEST_{util.get_datestamp()}.txt"
        self.msg_buffer = list()
        self.all_messages = list()

    def add_msg(self, msg: str, level: int = 0):
        for p in msg.split("\n"):
            self.msg_buffer.append(f"{self.INDENT * level}{p}")

    def flush(self, python_open_mode: str = "a"):
        f = open(self.filename, python_open_mode)

        print(f"Writing {len(self.msg_buffer)} messages:")
        for message in self.msg_buffer:
            f.write(message)
            f.write("\n")
            print(message)
        f.close()

        # reset buffer
        self.all_messages.extend(self.msg_buffer)
        self.msg_buffer.clear()

    def from_filetype_count_df(self, manifest_df):

        self.add_msg(f"\nNumber of files per types:\n", 1)
        self.add_msg(f"TOTAL: {len(manifest_df)}", 1)
        filetype_count_df = manifest_df.groupby(by="filetype", as_index=False).size()

        for i, val in filetype_count_df.iterrows():
            filetype = val["filetype"]
            count = val["size"]

            self.add_msg(f"{filetype}: {count}", 1)

    def to_string_filename_and_checksum_from_study_id_df(self, study_id_df):
        self.add_msg("\nFilename and checksums:\n", 1)

        report_string = study_id_df.to_string(
            columns=["filename", "provided_checksum"], index=False, header=True
        )

        for i in report_string.split("\n"):
            self.add_msg(i, 1)

    def get_messages(self):
        ret = list()
        ret.extend(self.all_messages)
        ret.extend(self.msg_buffer)
        return ret

    def insert_heading(self, title: str):

        self.add_msg("\n")
        self.add_msg(f"#############################################")
        self.add_msg(title)
        self.add_msg(f"#############################################")

    def insert_line(self, level: int = 0):
        self.add_msg("################################################", level)


class TsvFilecountReport:
    INDENT = "    "

    def __init__(self, filename: str = ""):
        self.filename = f"{filename}_FILECOUNT_{util.get_datestamp()}.tsv"
        self.filecount_json_list = list()

    def flush(self, python_open_mode: str = "a"):
        f = open(self.filename, python_open_mode)

        pd_df = pd.json_normalize(self.filecount_json_list)
        pd_df.fillna(value=0, inplace=True)
        f.write(pd_df.to_csv(sep="\t", index=False))
        f.close()

    def from_manifest_df_and_study_id(self, manifest_df, study_id: str):
        filecount_json = {"agha_study_id": study_id}
        filetype_count_df = manifest_df.groupby(by="filetype", as_index=False).size()
        filecount_json["TOTAL"] = str(len(manifest_df))

        for i, val in filetype_count_df.iterrows():
            filetype = val["filetype"]
            count = val["size"]
            filecount_json[filetype] = str(count)

        self.filecount_json_list.append(filecount_json)


class Data:
    def __init__(self):
        self.data = dict


def parse_json_with_pandas(data_list):
    pd_df = pd.json_normalize(data_list)
    pd_df.fillna(value=False, inplace=True)

    return pd_df


def summarize_manifest_key_prefix(s3_key_prefix: str):
    replace_slash_with_underscore_key = s3_key_prefix.replace("/", "_")
    txt_report = TxtReport(filename=replace_slash_with_underscore_key)
    filecount_report = TsvFilecountReport(filename=replace_slash_with_underscore_key)

    # Collect and parse info from DynamoDB
    manifest_data = dynamodb.get_batch_item_from_pk_and_sk(
        table_name=DYNAMODB_STORE_TABLE,
        partition_key=dynamodb.FileRecordPartitionKey.MANIFEST_FILE_RECORD.value,
        sort_key_prefix=s3_key_prefix,
    )
    pd_df = parse_json_with_pandas(manifest_data)
    unique_study_id_list = pd_df["agha_study_id"].unique().tolist()

    # Writing TXT Report
    title = f"Manifest report for '{STORE_BUCKET_NAME}' bucket with '{s3_key_prefix}' prefix on {util.get_datetimestamp()}."
    txt_report.add_msg(title)
    txt_report.add_msg("")
    txt_report.add_msg(f"Study Id Count: {len(unique_study_id_list)}")
    txt_report.add_msg(f"Study Id list: {json.dumps(unique_study_id_list)}")
    txt_report.add_msg("\nDetails")

    for study_id in unique_study_id_list:
        txt_report.insert_line()
        txt_report.add_msg(study_id)

        manifest_df = pd_df.loc[pd_df["agha_study_id"] == study_id]
        sorted_manifest_df = manifest_df.sort_values(by=["filetype"], inplace=False)

        filecount_report.from_manifest_df_and_study_id(manifest_df, study_id)

        txt_report.from_filetype_count_df(sorted_manifest_df)
        txt_report.to_string_filename_and_checksum_from_study_id_df(sorted_manifest_df)
        txt_report.add_msg("")

    txt_report.flush(python_open_mode="w")
    filecount_report.flush(python_open_mode="w")


def get_argument():
    parser = argparse.ArgumentParser(description="Generate bucket summary in GDR.")
    parser.add_argument("--sort-key-prefix", help="The prefix of s3 key to look.")
    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_argument()
    summarize_manifest_key_prefix(args.sort_key_prefix)
