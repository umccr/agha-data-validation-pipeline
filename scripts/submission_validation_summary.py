import util.s3 as s3
import util.agha as agha
import util as util
import json
from collections import defaultdict
from typing import List, Set

BUCKET_NAME_STAGING_NEW = 'agha-gdr-staging-2.0'
BUCKET_NAME_STORE_NEW = 'agha-gdr-store-2.0'

ignored_fs = [
    agha.FlagShip.UNKNOWN,
    agha.FlagShip.TEST
]

LAMBDA_NAME = 'agha-gdr-validation-pipeline-data-transfer-manager'
CLIENT_LAMBDA = util.get_client('lambda')


class Report:
    INDENT = '    '

    def __init__(self, filename: str = 'submission_validation_summary.txt'):
        self.msg_buffer = defaultdict(list)
        self.filename = filename

    def add_msg(self, msg: str, flagship: agha.FlagShip, level: int = 0):
        for p in msg.split('\n'):
            self.msg_buffer[flagship].append(f"{self.INDENT * level}{p}")

    def write_messages(self):
        f = open(self.filename, 'w')

        print(f"Writing messages for {len(self.msg_buffer)} flagships")
        fs: agha.FlagShip
        for fs in self.msg_buffer.keys():
            f.write(f"Validation messages for {fs.name} ({fs.preferred_code()})\n")
            for msg in self.msg_buffer[fs]:
                f.write(msg)
                f.write('\n')
                print(msg)

        f.close()

    def get_messages_per_flagship(self):
        return self.msg_buffer


def check_flagship(flagship: agha.FlagShip):
    report.add_msg(f"Checking flagship {flagship.name} ({flagship.preferred_code()})", flagship=flagship)
    subs = find_submissions(flagship)
    report.add_msg(f"{len(subs)} submissions for {flagship.preferred_code()}", flagship=flagship, level=1)
    ok_count = 0
    fail_count = 0
    for sub in subs:
        resp = fetch_validation_summary(flagship, sub)
        if is_ok_submission(resp):
            report.add_msg(f"Submission {sub}: OK", flagship=flagship, level=2)
            ok_count += 1
        else:
            report.add_msg(f"Submission {sub}: FAIL", flagship=flagship, level=2)
            fail_count += 1
    report.add_msg(f"Total: {len(subs)}, OK: {ok_count}, FAIL: {fail_count}", flagship=flagship, level=1)


def find_submissions(fs: agha.FlagShip) -> List[str]:
    submission_prefixes: Set[str] = set()
    prefix = f"{fs.preferred_code()}/"
    prefixes = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STAGING_NEW, prefix=prefix)
    submission_prefixes.update(prefixes)
    prefixes = s3.aws_s3_ls(bucket_name=BUCKET_NAME_STORE_NEW, prefix=prefix)
    submission_prefixes.update(prefixes)

    subs = list()
    for sp in submission_prefixes:
        parts = sp.split('/')
        if parts[0] != fs.preferred_code():
            raise ValueError(f"Found prefix does not match flagship: {sp} for FS {fs.preferred_code()}")
        subs.append(parts[1])

    return subs


def create_request_pl(flahship: agha.FlagShip, submission: str):
    # Example payload:  {"flagship_code": "Mito", "submission": "2019-09-29", "validation_check_only": "true"}
    pl = {
        'flagship_code': flahship.preferred_code(),
        'submission': submission,
        'validation_check_only': 'true'
    }
    return pl


def fetch_validation_summary(flahship: agha.FlagShip, submission: str):
    request_payload = create_request_pl(flahship, submission)
    response = CLIENT_LAMBDA.invoke(
        FunctionName=LAMBDA_NAME,
        Payload=json.dumps(request_payload),
    )
    # TODO log original response: print(response)
    status = response['StatusCode']
    if 'Payload' in response:
        response_payload = response['Payload'].read().decode("utf-8")
        print(f"Response payload: {response_payload}")
    else:
        response_payload = None
    if status != 200:
        raise ValueError(f"Request did not return as expected. Code {status}, with message {response_payload}")

    return response_payload


def is_ok_submission(raw_payload) -> bool:
    payload = json.loads(json.loads(raw_payload))
    failed_batch_jobs = payload['fail_batch_job_s3_key']
    failed_validations = payload['fail_validation_result_s3_key']
    return len(failed_batch_jobs) == 0 and len(failed_validations) == 0


def get_submission_failures(raw_payload) -> bool:
    payload = json.loads(json.loads(raw_payload))
    failed_batch_jobs = payload['fail_batch_job_s3_key']
    failed_validations = payload['fail_validation_result_s3_key']
    return failed_batch_jobs, failed_validations


if __name__ == '__main__':

    report = Report()
    # check_flagship(agha.FlagShip.RENAL_GENETICS)

    for fs in agha.FlagShip:
        if fs in ignored_fs:
            continue
        check_flagship(fs)

    # msg_per_fs = report.get_messages_per_flagship()
    # for fs in msg_per_fs.keys():
    #     msgs = msg_per_fs[fs]
    #     for msg in msgs:
    #         print(msg)

    report.write_messages()
