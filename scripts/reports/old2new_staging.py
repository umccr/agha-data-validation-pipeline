import logging
import util.s3 as s3
import util.agha as agha
import util as util

from typing import Dict, List

BUCKET_NAME_STAGING_OLD = "agha-gdr-staging"
BUCKET_NAME_STAGING_NEW = "agha-gdr-staging-2.0"
BUCKET_NAME_STORE_NEW = "agha-gdr-store-2.0"

ignored_fs = [agha.FlagShip.UNKNOWN, agha.FlagShip.TEST]
exceptions = {"KidGen": "KidGen/2020-03-28", "mito-batch-7": "Mito/2021-06-08"}


def exists_submission(bucket: str, prefix: str):
    s3_client = util.get_client("s3")
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=2)

    key_count = response["KeyCount"]
    return key_count > 0


def get_submission_map():
    submissions = dict()

    old_roots = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STAGING_OLD, prefix="")
    for old_root in old_roots:
        old_flagship = old_root.strip("/")
        fs = agha.FlagShip.from_name(old_flagship)
        if fs in ignored_fs:
            continue
        new_flagship = fs.preferred_code()
        prefixes = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STAGING_OLD, prefix=old_root)
        if len(prefixes) < 1:
            print(f"Needs manual exception: {old_root}")
            if old_flagship in exceptions.keys():
                print(f"Adding exception: {old_flagship} -> {exceptions[old_flagship]}")
                submissions[old_flagship] = exceptions[old_flagship]
        for prefix in prefixes:
            sub = prefix.split("/")[1]
            old_submission = f"{old_flagship}/{sub}"
            new_submission = f"{new_flagship}/{sub}"
            # TODO: check that new submission does not exist
            if exists_submission(bucket=BUCKET_NAME_STORE_NEW, prefix=new_submission):
                print(
                    f"Submission already exists: {new_submission} in bucket {BUCKET_NAME_STORE_NEW}"
                )
                continue
            submissions[old_submission] = new_submission
    return submissions


def generate_sync_commands(old_to_new_map: dict):
    commands = list()
    for key in old_to_new_map.keys():
        old: str = key
        new: str = old_to_new_map[old]
        commands.append(
            f'aws s3 sync --only-show-errors --include "*" --exclude "*manifest.txt*" s3://{BUCKET_NAME_STAGING_OLD}/{old}/ s3://{BUCKET_NAME_STAGING_NEW}/{new}/ > {old.replace("/", "_")}.log &'
        )

    return commands


def generate_manifest_commands(old_to_new_map: dict):
    commands = list()
    for key in old_to_new_map.keys():
        old: str = key
        new: str = old_to_new_map[old]
        # TODO: check that the folder actually contains data files
        commands.append(
            f"aws s3 cp s3://{BUCKET_NAME_STAGING_OLD}/{old}/manifest.txt s3://{BUCKET_NAME_STAGING_NEW}/{new}/manifest.txt"
        )


if __name__ == "__main__":
    subs = get_submission_map()

    data_commands = generate_sync_commands(subs)
    for cmd in data_commands:
        print(cmd)

    print("\n\n")

    manifest_commands = generate_sync_commands(subs)
    for cmd in manifest_commands:
        print(cmd)
