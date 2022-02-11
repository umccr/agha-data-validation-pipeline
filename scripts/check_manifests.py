import io
import util.s3 as s3
import util as util
from typing import Generator, List

BUCKET_NAME_STAGING_NEW = 'agha-gdr-staging-2.0'
BUCKET_NAME_STORE_NEW = 'agha-gdr-store-2.0'

# TODO: logging instead of prints


def mainfest_key_from_prefix(prefix: str) -> str:
    if not prefix.endswith('/'):
        prefix += '/'
    return f"{prefix}manifest.txt"


def exists_key(bucket: str, key: str):
    client_s3 = util.get_client('s3')
    response = client_s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key
    )
    return response.get('Contents')


def get_file_bytes(bucket: str, key: str):
    """
    Read content of a file object in S3 into an in-memory byte buffer.
    :param bucket: name of the S3 bucket to read from
    :param key: the S3 object key of the file in the bucket
    :return: a file-like object, a byte buffer: io.BytesIO
    """
    client_s3 = util.get_client('s3')
    with io.BytesIO() as f:
        client_s3.download_fileobj(bucket, key, f)
        f.seek(0)
        data = io.BytesIO(f.read())
    return data


def get_file_lines(bucket: str, key: str) -> Generator[str, None, None]:
    byte_date = get_file_bytes(bucket=bucket, key=key)
    for line in byte_date:
        yield line.decode()


def get_prefixes(bucket: str) -> List[str]:
    prefixes: List[str] = list()
    flagship_prefixes = s3.aws_s3_ls(bucket_name=bucket, prefix='')
    for flagship_prefix in flagship_prefixes:
        submission_prefixes = s3.aws_s3_ls(bucket_name=bucket, prefix=flagship_prefix)
        prefixes.extend(submission_prefixes)
    return prefixes


def get_manifest_keys(bucket: str) -> List[str]:
    mainfest_keys: List[str] = list()
    prefixes = get_prefixes(bucket)
    for prefix in prefixes:
        mf = mainfest_key_from_prefix(prefix)
        if exists_key(bucket, mf):
            print(f"Found manifest {mf}")
            mainfest_keys.append(mf)
        else:
            print(f"No manifest for {mf}")
    return mainfest_keys


def generate_old_manifest_migration_commands(source_bucket: str, source_manifest_keys: List[str], dest_bucket: str):
    commands: List[str] = list()
    for source_manifest in source_manifest_keys:
        dest_manifest = source_manifest.replace('.txt', '.orig')
        cmd = f"aws s3 cp --dryrun s3://{source_bucket}/{source_manifest} s3://{dest_bucket}/{dest_manifest}"
        commands.append(cmd)
    return commands


def get_all_manifest_headers(manifest_keys: List[str]) -> List[str]:
    headers: List[str] = list()
    for manifest in manifest_keys:
        mf_lines = get_file_lines(bucket=BUCKET_NAME_STAGING_NEW, key=manifest)
        header_line = next(mf_lines)
        headers.append(f"{manifest}\t{header_line}")

    return headers


if __name__ == '__main__':
    mf_headers_file_name = 'manifest_headers.txt'
    mf_cmd_file_name = 'manifest_migration_cmds.txt'

    # generate manifest keys for the staging bucket
    manifest_keys = get_manifest_keys(BUCKET_NAME_STAGING_NEW)
    print(f"Found {len(manifest_keys)} manifests.")
    f = open('manifests-staging.txt', 'w')
    for h in manifest_keys:
        f.write(h)
    f.close()

    # generate manifest keys for the staging bucket
    manifest_keys_store = get_manifest_keys(BUCKET_NAME_STORE_NEW)
    print(f"Found {len(manifest_keys)} manifests.")
    f = open('manifests-store.txt', 'w')
    for h in manifest_keys:
        f.write(h)
    f.close()

    # # write manifest headers to file
    # manifest_headers = get_all_manifest_headers(manifest_keys)
    # f = open(mf_headers_file_name, 'w')
    # for h in manifest_headers:
    #     f.write(h)
    # f.close()
    # # print column count for each header
    # import csv
    # with open(mf_headers_file_name) as tsvfile:
    #     tsvreader = csv.reader(tsvfile, delimiter="\t")
    #     for line in tsvreader:
    #         print(len(line))

    # # generate migration commands for the original manifests to store
    # cmds = generate_old_manifest_migration_commands(
    #     source_bucket=BUCKET_NAME_STAGING_NEW,
    #     source_manifest_keys=manifest_keys,
    #     dest_bucket=BUCKET_NAME_STORE_NEW)
    # f = open(mf_cmd_file_name, 'w')
    # for cmd in cmds:
    #     f.write(cmd + '\n')
    # f.close()

    print("All done.")
