import sys
import os
import json
import argparse
import pandas as pd
import boto3

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SOURCE_PATH = os.path.join(
    DIR_PATH, "..", "..", "..", "lambdas", "layers", "util"
)
sys.path.append(SOURCE_PATH)

import util.agha as agha
import util as util

STAGING_BUCKET_NAME = 'agha-gdr-staging-2.0'
RESULTS_BUCKET_NAME = 'agha-gdr-results-2.0'
STORE_BUCKET_NAME = 'agha-gdr-store-2.0'

DYNAMODB_STAGING_TABLE = "agha-gdr-staging-bucket"
DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"

flagships = ['Leuko', 'GI', 'EE', 'BM', 'HIDDEN', 'chILDRANZ', 'AC', 'Mito', 'KidGen', 'ID',
             'ICCon', 'Cardiac']


class Report:
    INDENT = '    '

    def __init__(self, filename=f'bucket_summary_{util.get_datestamp()}.txt'):
        self.filename = filename
        self.msg_buffer = list()
        self.all_messages = list()

    def add_msg(self, msg: str, level: int = 0):
        for p in msg.split('\n'):
            self.msg_buffer.append(f"{self.INDENT * level}{p}")

    def flush(self):
        f = open(self.filename, 'w')

        print(f"Writing {len(self.msg_buffer)} messages:")
        for message in self.msg_buffer:
            f.write(message)
            f.write('\n')
            print(message)
        f.close()

        # reset buffer
        self.all_messages.extend(self.msg_buffer)
        self.msg_buffer.clear()

    def from_size_in_bytes_df(self, size_in_byte_df):

        self.add_msg('Total file size in bytes per file type.\n', 2)

        for i, val in size_in_byte_df.iterrows():
            filetype = val['filetype']
            size = val['size_in_bytes']

            self.add_msg(f'Total file size of {filetype} (in bytes): {size}', 2)

    def from_filetype_count_df(self, filetype_count_df):

        self.add_msg('File count per file type.\n', 2)

        for i, val in filetype_count_df.iterrows():
            filetype = val['filetype']
            count = val['size']

            self.add_msg(f'Total number of {filetype}: {count}', 2)

    def get_messages(self):
        ret = list()
        ret.extend(self.all_messages)
        ret.extend(self.msg_buffer)
        return ret

    def insert_heading(self, title: str):

        self.add_msg('\n')
        self.add_msg(f"################################################################################")
        self.add_msg(title)
        self.add_msg(f"################################################################################")

    def add_double_line(self):
        self.add_msg(
            f"\n================================================================================\n", 1)


class Data:

    def __init__(self):
        self.data = dict


def parse_json_with_pandas(data_list):

    pd_df = pd.json_normalize(data_list)
    pd_df.fillna(value=False, inplace=True)
    pd_df['size_in_bytes'] = pd_df['size_in_bytes'].astype(int)

    return pd_df


def summarize_files_from_bucket(consent: bool = True):
    # Some data storage
    report_txt = Report()

    all_data = download_dynamodb_table()

    pd_df = parse_json_with_pandas(all_data)
    title = f'General statistic report for \'{STORE_BUCKET_NAME}\' bucket at date {util.get_datestamp()}.'
    if consent:
        pd_df = pd_df.loc[pd_df['Consent'] == bool(True)]
        report_txt.filename = f'consented_bucket_summary_{util.get_datestamp()}.txt'
        title = f'General statistic report for CONSENTED data in \'{STORE_BUCKET_NAME}\' bucket at date {util.get_datestamp()}.'

    report_txt.add_msg(title)

    # For ALL files in the bucket
    report_txt.insert_heading("Statistic Entire Bucket")

    size_in_byte_df = pd_df.groupby(by="filetype", as_index=False)['size_in_bytes'].sum()
    number_of_filetypes_df = pd_df.groupby(by="filetype", as_index=False).size()

    report_txt.from_filetype_count_df(number_of_filetypes_df)
    report_txt.add_double_line()
    report_txt.from_size_in_bytes_df(size_in_byte_df)

    report_txt.flush()

    # Per Flagship level
    for new_flagship in flagships:
        flagship_enum = agha.FlagShip.from_name(new_flagship)
        flagship_official_name = flagship_enum.official_name()
        flagship_preferred_code = flagship_enum.preferred_code()
        report_txt.insert_heading(f'Checking FlagShip: {flagship_official_name} ({flagship_preferred_code})')

        flagship_df = pd_df[pd_df['sort_key'].str.startswith(new_flagship)]

        size_in_byte_df = flagship_df.groupby(by="filetype", as_index=False)['size_in_bytes'].sum()
        number_of_filetypes_df = flagship_df.groupby(by="filetype", as_index=False).size()

        report_txt.from_filetype_count_df(number_of_filetypes_df)
        report_txt.add_double_line()
        report_txt.from_size_in_bytes_df(size_in_byte_df)

        # IMPROVEMENT: Could write it to a json file for post-processing if needed.

        report_txt.flush()


def download_dynamodb_table():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMODB_STORE_TABLE)

    results_data = []

    func_parameter = {}
    response = table.scan(**func_parameter)
    results_data.extend(response['Items'])

    # Re-fetch until the last
    while response.get('LastEvaluatedKey') is not None:
        func_parameter['ExclusiveStartKey'] = response['LastEvaluatedKey']
        response = table.scan(**func_parameter)
        results_data.extend(response['Items'])

    return results_data

def get_argument():
    parser = argparse.ArgumentParser(description='Generate bucket summary in GDR.')
    parser.add_argument('--consent',
                        default=False,
                        action='store_true',
                        help="Set this to only calculate based on consented tag.")

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_argument()

    summarize_files_from_bucket(consent=args.consent)