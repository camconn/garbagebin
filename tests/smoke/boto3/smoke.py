# /// script
# requires-python = ">=3.12"
# dependencies = ["boto3==1.42.*"]
# ///
"""boto3 smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY."""

import io
import os
import sys
import urllib.request
import uuid

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9000")
BUCKET = f"gb-smoke-boto3-{uuid.uuid4().hex[:12]}"
SMALL = b"hello, garbage bin\n"
BIG = os.urandom(20 * 1024 * 1024)


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY", "test"),
        config=Config(s3={"addressing_style": "path"}),
    )

    s3.create_bucket(Bucket=BUCKET)

    s3.put_object(Bucket=BUCKET, Key="dir/a/small.txt", Body=SMALL)

    # upload_fileobj switches to multipart above 8 MB.
    s3.upload_fileobj(io.BytesIO(BIG), BUCKET, "dir/b/big.bin")
    head = s3.head_object(Bucket=BUCKET, Key="dir/b/big.bin")
    check("-" in head["ETag"], f"big object ETag is not multipart: {head['ETag']}")
    check(head["ContentLength"] == len(BIG), f"big object size {head['ContentLength']}")
    body = s3.get_object(Bucket=BUCKET, Key="dir/b/big.bin")["Body"].read()
    check(body == BIG, "big object body mismatch")

    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="dir/", Delimiter="/")
    prefixes = sorted(p["Prefix"] for p in resp.get("CommonPrefixes", []))
    check(prefixes == ["dir/a/", "dir/b/"], f"CommonPrefixes: {prefixes}")
    check(not resp.get("Contents"), f"unexpected Contents: {resp.get('Contents')}")

    got = s3.get_object(Bucket=BUCKET, Key="dir/b/big.bin", Range="bytes=100-199")
    check(got["ResponseMetadata"]["HTTPStatusCode"] == 206, "range get status")
    check(got["Body"].read() == BIG[100:200], "range get bytes mismatch")

    s3.copy_object(Bucket=BUCKET, Key="copy/small.txt",
                   CopySource={"Bucket": BUCKET, "Key": "dir/a/small.txt"})
    got = s3.get_object(Bucket=BUCKET, Key="copy/small.txt")["Body"].read()
    check(got == SMALL, "copied object mismatch")

    url = s3.generate_presigned_url("get_object", Params={"Bucket": BUCKET, "Key": "copy/small.txt"},
                                    ExpiresIn=300)
    with urllib.request.urlopen(url) as r:
        check(r.read() == SMALL, "presigned get mismatch")

    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET)["Contents"]]
    check(len(keys) == 3, f"expected 3 keys, got {keys}")
    resp = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in keys]})
    check(not resp.get("Errors"), f"delete_objects errors: {resp.get('Errors')}")
    check(len(resp.get("Deleted", [])) == 3, f"delete_objects deleted: {resp.get('Deleted')}")
    check(s3.list_objects_v2(Bucket=BUCKET)["KeyCount"] == 0, "bucket not empty after batch delete")

    s3.delete_bucket(Bucket=BUCKET)
    try:
        s3.head_bucket(Bucket=BUCKET)
        check(False, "bucket still exists after delete")
    except ClientError as e:
        check(e.response["Error"]["Code"] in ("404", "NoSuchBucket"), f"head_bucket: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FAIL boto3: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
    print("PASS boto3")
