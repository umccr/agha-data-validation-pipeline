import boto3
import json
import util


DYNAMODB_TABLE = "agha_store_temp"


class Data:
    def __init__(self, filename: str = "agha_store_temp.json"):
        self.msg_buffer = list()
        self.filename = filename

    def flush(self):
        f = open(self.filename, "w")

        print(f"Writing {len(self.msg_buffer)} messages:")
        f.write(json.dumps(self.msg_buffer, indent=4, cls=util.JsonSerialEncoder))
        f.close()

        # reset buffer
        self.msg_buffer.clear()


def download_dynamodb_table(class_data: Data):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(DYNAMODB_TABLE)

    func_parameter = {}
    response = table.scan(**func_parameter)
    class_data.msg_buffer.extend(response["Items"])

    # Re-fetch until the last
    while response.get("LastEvaluatedKey") is not None:
        func_parameter["ExclusiveStartKey"] = response["LastEvaluatedKey"]
        response = table.scan(**func_parameter)
        class_data.msg_buffer.extend(response["Items"])


if __name__ == "__main__":
    storage_data = Data()

    download_dynamodb_table(storage_data)

    storage_data.flush()
