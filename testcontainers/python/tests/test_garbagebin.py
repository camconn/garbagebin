import os

import boto3
from botocore.config import Config

from testcontainers_garbagebin import GarbageBinContainer


def test_put_get():
    image = os.environ.get("GB_TEST_IMAGE", "garbagebin:latest")
    with GarbageBinContainer(
        image, init_buckets=["tc-bucket"], access_key="gbaccess", secret_key="gbsecret123"
    ).with_env("S3_STRICT_AUTH", "true") as gb:  # proves the credentials reach the server
        s3 = boto3.client(
            "s3",
            endpoint_url=gb.endpoint(),
            aws_access_key_id=gb.access_key(),
            aws_secret_access_key=gb.secret_key(),
            region_name="us-east-1",
            config=Config(s3={"addressing_style": "path"}),
        )
        s3.put_object(Bucket="tc-bucket", Key="hello.txt", Body=b"hello")
        assert s3.get_object(Bucket="tc-bucket", Key="hello.txt")["Body"].read() == b"hello"
