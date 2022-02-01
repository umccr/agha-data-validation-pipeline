import boto3
import json
import util

from util import dynamodb


DYNAMODB_STORE_TABLE = "agha-gdr-store-bucket"


class Data:

    def __init__(self, filename: str = 'agha_gdr_store_bucket.json'):
        self.msg_buffer = list()
        self.filename = filename

    def flush(self):
        f = open(self.filename, 'w')

        print(f"Writing {len(self.msg_buffer)} messages:")
        f.write(json.dumps(self.msg_buffer, indent=4, cls=util.JsonSerialEncoder))
        f.close()

        # reset buffer
        self.msg_buffer.clear()


def download_dynamodb_table(class_data: Data):

    pk = dynamodb.FileRecordPartitionKey.FILE_RECORD.value
    item_data = dynamodb.get_batch_item_from_pk_only(DYNAMODB_STORE_TABLE, pk)

    class_data.msg_buffer.extend(item_data)


if __name__ == '__main__':
    storage_data = Data()

    download_dynamodb_table(storage_data)

    storage_data.flush()