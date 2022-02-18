import json
import util.s3 as s3
import util.agha as agha
import util as util
from collections import defaultdict


from typing import Generator

BUCKET_NAME_STAGING_OLD = 'agha-gdr-staging'
BUCKET_NAME_STAGING_NEW = 'agha-gdr-staging-2.0'
BUCKET_NAME_STORE_NEW = 'agha-gdr-store-2.0'

ignored_fs = [
    agha.FlagShip.UNKNOWN,
    agha.FlagShip.TEST
]
exceptions = {
    'KidGen': 'KidGen/2020-03-28',
    'mito-batch-7': 'Mito/2021-06-08'
}


def exists_submission(bucket: str, prefix: str):
    s3_client = util.get_client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=2)

    key_count = response['KeyCount']
    return key_count > 0


def exists_key(bucket: str, key: str):
    client_s3 = util.get_client('s3')
    response = client_s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key
    )
    return response.get('Contents')


def get_listing(bucket: str, prefix: str = "", filter_dirs: bool = True) -> Generator[dict, None, None]:
    paginator = util.get_client('s3').get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in page_iterator:
        for obj in page.get('Contents', list()):
            if filter_dirs and obj['Key'].endswith('/'):
                continue
            yield obj


def get_submission_map():
    submissions = dict()

    old_roots = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STAGING_OLD, prefix='')
    for old_root in old_roots:
        old_flagship = old_root.strip('/')
        fs = agha.FlagShip.from_name(old_flagship)
        if fs in ignored_fs:
            continue
        new_flagship = fs.preferred_code()
        prefixes = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STAGING_OLD, prefix=old_root)
        if len(prefixes) < 1:
            print(f"Needs manual exception: {old_root}")
            if old_flagship in exceptions.keys():
                print(f"Adding exception: {old_flagship} -> {exceptions[old_flagship]}")
                new_submission = exceptions[old_flagship]
                if exists_submission(bucket=BUCKET_NAME_STORE_NEW, prefix=new_submission):
                    print(f"Submission already exists: {new_submission} in bucket {BUCKET_NAME_STORE_NEW}")
                    continue
                submissions[old_flagship] = new_submission
        for prefix in prefixes:
            sub = prefix.split('/')[1]
            old_submission = f"{old_flagship}/{sub}"
            new_submission = f"{new_flagship}/{sub}"
            # TODO: check that new submission does not exist
            if exists_submission(bucket=BUCKET_NAME_STORE_NEW, prefix=new_submission):
                print(f"Submission already exists: {new_submission} in bucket {BUCKET_NAME_STORE_NEW}")
                continue
            submissions[old_submission] = new_submission
    return submissions


def generate_sync_commands(old_to_new_map: dict):
    commands = list()
    for key in old_to_new_map.keys():
        old : str = key
        new : str = old_to_new_map[old]
        commands.append(f'aws s3 sync --only-show-errors --include "*" --exclude "*manifest.txt*" s3://{BUCKET_NAME_STAGING_OLD}/{old}/ s3://{BUCKET_NAME_STAGING_NEW}/{new}/ > {old.replace("/", "_")}.log &')

    return commands


def generate_manifest_commands(old_to_new_map: dict):
    commands = list()
    for key in old_to_new_map.keys():
        old : str = key
        new : str = old_to_new_map[old]
        # TODO: check that the folder actually contains data files
        if not exists_key(bucket=BUCKET_NAME_STAGING_NEW, key=f"{new}/manifest.txt"):
            commands.append(f'aws s3 cp s3://{BUCKET_NAME_STAGING_OLD}/{old}/manifest.txt s3://{BUCKET_NAME_STAGING_NEW}/{new}/manifest.txt')
        else:
            print(f"Manifest already present for s3://{BUCKET_NAME_STAGING_NEW}/{new}")
    return commands


def compare_bucket_etags():
    prefix = ''

    # # get all Etags from the old bucket
    # old_etags = defaultdict(list)
    # for s3_object in get_listing(bucket=BUCKET_NAME_STAGING_OLD, prefix=prefix):
    #     etag = s3_object['ETag'].strip('"')
    #     key = s3_object['Key']
    #     old_etags[etag].append(key)

    # with open('../tmp/temp-etag-staging-old.json', 'w') as convert_file:
    #     convert_file.write(json.dumps(old_etags, indent=6))

    with open('../tmp/temp-etag-staging-old.json', 'r') as convert_file:
        old_etags = json.loads(convert_file.read())

    print(f"Number of ETags in old staging bucket: {len(old_etags.keys())}")


    # # get all Etags from the new bucket
    # new_etags = defaultdict(list)
    # for s3_object in get_listing(bucket=BUCKET_NAME_STAGING_NEW, prefix=prefix):
    #     etag = s3_object['ETag'].strip('"')
    #     key = s3_object['Key']
    #     new_etags[etag].append(key)

    # with open('../tmp/temp-etag-staging-new.json', 'w') as convert_file:
    #     convert_file.write(json.dumps(new_etags, indent=6))

    with open('../tmp/temp-etag-staging-new.json', 'r') as convert_file:
        new_etags = json.loads(convert_file.read())

    print(f"Number of ETags in new staging bucket: {len(new_etags.keys())}")

    # # get all Etags from the new store
    # store_etags = defaultdict(list)
    # for s3_object in get_listing(bucket=BUCKET_NAME_STORE_NEW, prefix=prefix):
    #     etag = s3_object['ETag'].strip('"')
    #     key = s3_object['Key']
    #     store_etags[etag].append(key)
    #
    # with open('../tmp/temp-etag-store-new.json', 'w') as convert_file:
    #     convert_file.write(json.dumps(store_etags, indent=6))

    with open('../tmp/temp-etag-store-new.json', 'r') as convert_file:
        store_etags = json.loads(convert_file.read())

    print(f"Number of ETags in new store bucket: {len(store_etags.keys())}")

    # Compare the ETags
    mismatch_cnt = 0
    for old_etag in old_etags.keys():
        if old_etag in new_etags.keys() or old_etag in store_etags.keys():
            continue
        mismatch_cnt += 1
        print(f"ETag not found: {old_etags[old_etag]}")
    print(f"Number of ETags not found: {mismatch_cnt}")


if __name__ == '__main__':
    # subs = get_submission_map()

    # data_commands = generate_sync_commands(subs)
    # for cmd in data_commands:
    #     print(cmd)
    #
    # print("\n\n")

    # manifest_commands = generate_manifest_commands(subs)
    # for cmd in manifest_commands:
    #     print(cmd)

    compare_bucket_etags()
    print("All done.")
