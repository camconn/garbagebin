#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3", "requests"]
# ///
"""Replay a scenario against two S3 endpoints (A = real S3, B = Garbage Bin) and diff the responses."""

import argparse
import base64
import difflib
import hashlib
import json
import os
import random
import re
import sys
import uuid
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote

import boto3
import requests
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError

HERE = Path(__file__).parent

# Headers compared by value. Last-Modified is compared by presence only.
HEADERS = [
    "etag",
    "content-type",
    "content-length",
    "content-range",
    "accept-ranges",
    "cache-control",
    "content-disposition",
    "content-encoding",
    "location",
    "x-amz-mp-parts-count",
    "x-amz-tagging-count",
    "x-amz-copy-source-version-id",
    "x-amz-bucket-region",
]
PRESENCE_HEADERS = ["last-modified"]
# XML elements whose text is compared by presence only.
BLANK_TAGS = {
    "RequestId",
    "HostId",
    "ID",
    "DisplayName",
    "LastModified",
    "Initiated",
    "UploadId",
    "NextContinuationToken",
    "ContinuationToken",
    "Message",
}
VAR = re.compile(r"\{([a-z0-9_]+)\}")


class Target:
    def __init__(self, name, endpoint, access, secret, token, region, bucket):
        self.name, self.endpoint, self.region, self.bucket = name, endpoint.rstrip("/"), region, bucket
        self.creds = Credentials(access, secret, token)
        self.vars = {"bucket": bucket}
        self.http = requests.Session()

    def sub(self, s):
        return VAR.sub(lambda m: self.vars.get(m.group(1), m.group(0)), s)

    def send(self, step):
        bucket = self.sub(step.get("bucket", "{bucket}"))
        path = "/" + quote(bucket, safe="")
        if "key" in step:
            path += "/" + quote(self.sub(step["key"]), safe="/~")
        query = "&".join(
            quote(k, safe="") + ("=" + quote(self.sub(str(v)), safe="~") if v != "" else "")
            for k, v in step.get("query", {}).items()
        )
        url = self.endpoint + path + ("?" + query if query else "")
        body = make_body(step.get("body"), self.sub)
        headers = {k: self.sub(v) for k, v in step.get("headers", {}).items()}
        headers["Accept-Encoding"] = "identity"
        if step.get("content_md5"):
            headers["Content-MD5"] = base64.b64encode(hashlib.md5(body).digest()).decode()
        req = AWSRequest(method=step["method"], url=url, data=body, headers=headers)
        S3SigV4Auth(self.creds, "s3", self.region).add_auth(req)
        resp = self.http.request(
            step["method"], url, headers=dict(req.headers.items()), data=body, allow_redirects=False, timeout=120
        )
        for var, spec in step.get("capture", {}).items():
            if "header" in spec:
                self.vars[var] = resp.headers.get(spec["header"], "")
            else:
                el = find_xml(resp.content, spec["xml"])
                self.vars[var] = el if el is not None else ""
        return resp

    def s3(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint,
            region_name=self.region,
            aws_access_key_id=self.creds.access_key,
            aws_secret_access_key=self.creds.secret_key,
            aws_session_token=self.creds.token,
            config=Config(s3={"addressing_style": "path"}),
        )

    def cleanup(self):
        try:
            s3 = self.s3()
            for up in s3.list_multipart_uploads(Bucket=self.bucket).get("Uploads", []):
                s3.abort_multipart_upload(Bucket=self.bucket, Key=up["Key"], UploadId=up["UploadId"])
            for page in s3.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
                keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if keys:
                    s3.delete_objects(Bucket=self.bucket, Delete={"Objects": keys, "Quiet": True})
            s3.delete_bucket(Bucket=self.bucket)
        except (BotoCoreError, ClientError) as e:
            if "NoSuchBucket" not in str(e):
                print(f"cleanup {self.name}: {e}", file=sys.stderr)


def make_body(spec, sub):
    if spec is None:
        return b""
    if isinstance(spec, str):
        return sub(spec).encode()
    return random.Random(spec.get("seed", 0)).randbytes(spec["random"])


def strip_ns(tag):
    return tag.rsplit("}", 1)[-1]


def find_xml(content, tag):
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return None
    for el in root.iter():
        if strip_ns(el.tag) == tag:
            return el.text or ""
    return None


def normalize(resp, target, ignore_headers, ignore_xml):
    """Render a response as comparable lines with target-specific values replaced."""

    def fix(s):
        return s.replace(target.endpoint, "<endpoint>").replace(target.bucket, "<bucket>")

    lines = [f"status {resp.status_code}"]
    body = resp.content
    root = None
    if body.lstrip().startswith(b"<"):
        try:
            root = ET.fromstring(body)
        except ET.ParseError:
            pass
    for h in HEADERS:
        if h in ignore_headers or (h == "content-length" and root is not None):
            continue
        if h in resp.headers:
            lines.append(f"header {h}: {fix(resp.headers[h])}")
    for h, v in sorted(resp.headers.items()):
        if h.lower().startswith("x-amz-meta-") and h.lower() not in ignore_headers:
            lines.append(f"header {h.lower()}: {v}")
    for h in PRESENCE_HEADERS:
        if h in resp.headers and h not in ignore_headers:
            lines.append(f"header {h}: <present>")
    if root is not None:

        def walk(el, depth):
            tag = strip_ns(el.tag)
            if tag in ignore_xml:
                return
            text = (el.text or "").strip()
            if text and tag in BLANK_TAGS:
                text = "<present>"
            lines.append("  " * depth + tag + (f": {fix(text)}" if text else ""))
            for child in el:
                walk(child, depth + 1)

        ns = root.tag[1:].split("}")[0] if root.tag.startswith("{") else ""
        lines.append(f"body xml {ns}".rstrip() + ":")
        walk(root, 1)
    elif body:
        lines.append(f"body {len(body)} bytes sha256 {hashlib.sha256(body).hexdigest()}")
    return lines


def target_from_env(side, prefix):
    s = side.lower()
    run = uuid.uuid4().hex[:8]
    if side == "A":
        return Target(
            "A",
            os.environ.get("DIFF_A_ENDPOINT", "https://s3.us-east-1.amazonaws.com"),
            os.environ.get("DIFF_A_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID"),
            os.environ.get("DIFF_A_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY"),
            os.environ.get("DIFF_A_SESSION_TOKEN") or os.environ.get("AWS_SESSION_TOKEN"),
            os.environ.get("DIFF_A_REGION") or os.environ.get("AWS_REGION", "us-east-1"),
            f"{prefix}-{s}-{run}",
        )
    return Target(
        "B",
        os.environ.get("DIFF_B_ENDPOINT", "http://localhost:9000"),
        os.environ.get("DIFF_B_ACCESS_KEY", "test"),
        os.environ.get("DIFF_B_SECRET_KEY", "test"),
        None,
        os.environ.get("DIFF_B_REGION", "us-east-1"),
        f"{prefix}-{s}-{run}",
    )


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("scenario", nargs="?", default=str(HERE / "scenarios" / "basic.json"))
    ap.add_argument("-v", "--verbose", action="store_true", help="print normalized responses for every step")
    args = ap.parse_args()

    scenario = json.loads(Path(args.scenario).read_text())
    prefix = os.environ.get("DIFF_BUCKET_PREFIX", "gb-diff")
    a, b = target_from_env("A", prefix), target_from_env("B", prefix)
    if not a.creds.access_key:
        sys.exit("side A needs credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or DIFF_A_*")
    ignore_h = {h.lower() for h in scenario.get("ignore_headers", [])}
    ignore_x = set(scenario.get("ignore_xml", []))

    diffs = 0
    try:
        for step in scenario["steps"]:
            name = step["name"]
            sh = ignore_h | {h.lower() for h in step.get("ignore_headers", [])}
            sx = ignore_x | set(step.get("ignore_xml", []))
            la = normalize(a.send(step), a, sh, sx)
            lb = normalize(b.send(step), b, sh, sx)
            if la == lb:
                print(f"ok    {name}")
                if args.verbose:
                    print("\n".join("      " + line for line in la))
            else:
                diffs += 1
                print(f"DIFF  {name}")
                for line in difflib.unified_diff(la, lb, "A", "B", lineterm="", n=2):
                    print("      " + line)
    finally:
        a.cleanup()
        b.cleanup()
    print(f"{diffs} step(s) differ" if diffs else "no differences")
    sys.exit(1 if diffs else 0)


if __name__ == "__main__":
    main()
