#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["boto3", "requests"]
# ///
"""Replay a scenario against two S3 endpoints (A = real S3, B = Garbage Bin) and diff the responses."""

import argparse
import base64
import datetime
import difflib
import hashlib
import http.client
import json
import os
import random
import re
import sys
import uuid
import xml.etree.ElementTree as ET
import zlib
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import quote, urlsplit

import boto3
import requests
from botocore.auth import S3SigV4Auth, S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.credentials import Credentials
from botocore.exceptions import BotoCoreError, ClientError
from requests.structures import CaseInsensitiveDict

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
# Buckets under the prefix older than this are left over from a killed run.
STALE_AFTER = datetime.timedelta(hours=1)
TRAILER_DIGESTS = {
    "crc32": lambda b: zlib.crc32(b).to_bytes(4, "big"),
    "sha1": lambda b: hashlib.sha1(b).digest(),
    "sha256": lambda b: hashlib.sha256(b).digest(),
}


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
        key = "/" + quote(self.sub(step["key"]), safe="/~") if "key" in step else ""
        query = "&".join(
            quote(k, safe="") + ("=" + quote(self.sub(str(v)), safe="~") if v != "" else "")
            for k, v in step.get("query", {}).items()
        )
        if step.get("vhost"):
            u = urlsplit(self.endpoint)
            url = f"{u.scheme}://{quote(bucket, safe='')}.{u.netloc}{key or '/'}"
        else:
            url = self.endpoint + "/" + quote(bucket, safe="") + key
        url += "?" + query if query else ""
        body = make_body(step.get("body"), self.sub)
        create_bucket = step["method"] == "PUT" and not key and not query and "body" not in step
        if create_bucket and self.region != "us-east-1":
            body = (
                "<CreateBucketConfiguration><LocationConstraint>"
                f"{self.region}</LocationConstraint></CreateBucketConfiguration>"
            ).encode()
        headers = {k: self.sub(v) for k, v in step.get("headers", {}).items()}
        headers["Accept-Encoding"] = "identity"
        if step.get("content_md5"):
            headers["Content-MD5"] = base64.b64encode(hashlib.md5(body).digest()).decode()
        # A presigned URL does not cover the body, and botocore's query signer would try to parse it.
        req = AWSRequest(method=step["method"], url=url, data={} if "presign" in step else body, headers=headers)
        if "aws_chunked" in step:
            body = aws_chunked(req, body, step["aws_chunked"])
        if "presign" in step:
            S3SigV4QueryAuth(self.creds, "s3", self.region, expires=step["presign"].get("expires", 300)).add_auth(req)
            if step["presign"].get("tamper"):
                req.url = re.sub(r"(X-Amz-Signature=)(.)", lambda m: m.group(1) + "10"[m.group(2) == "1"], req.url)
        else:
            S3SigV4Auth(self.creds, "s3", self.region).add_auth(req)
        if step.get("no_length"):
            resp = send_unframed(step["method"], req.url, dict(req.headers.items()))
        else:
            resp = self.http.request(
                step["method"],
                req.url,
                headers=dict(req.headers.items()),
                data=body,
                allow_redirects=False,
                timeout=120,
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

    def cleanup(self, bucket=None):
        bucket = bucket or self.bucket
        try:
            s3 = self.s3()
            for up in s3.list_multipart_uploads(Bucket=bucket).get("Uploads", []):
                s3.abort_multipart_upload(Bucket=bucket, Key=up["Key"], UploadId=up["UploadId"])
            for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
                versions = page.get("Versions", []) + page.get("DeleteMarkers", [])
                objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in versions]
                if objs:
                    s3.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
            s3.delete_bucket(Bucket=bucket)
        except (BotoCoreError, ClientError) as e:
            if "NoSuchBucket" not in str(e):
                print(f"cleanup {self.name} {bucket}: {e}", file=sys.stderr)

    def sweep(self, prefix):
        """Deletes buckets leaked by earlier runs that were killed before cleanup."""
        cutoff = datetime.datetime.now(datetime.timezone.utc) - STALE_AFTER
        try:
            buckets = self.s3().list_buckets(Prefix=prefix + "-").get("Buckets", [])
        except (BotoCoreError, ClientError) as e:
            print(f"sweep {self.name}: {e}", file=sys.stderr)
            return
        for b in buckets:
            if b["CreationDate"] < cutoff:
                print(f"sweep {self.name}: deleting stale bucket {b['Name']}")
                self.cleanup(b["Name"])


def make_body(spec, sub):
    if spec is None:
        return b""
    if isinstance(spec, str):
        return sub(spec).encode()
    return random.Random(spec.get("seed", 0)).randbytes(spec["random"])


def send_unframed(method, url, headers):
    """Sends a request with neither Content-Length nor Transfer-Encoding, which requests always adds."""
    u = urlsplit(url)
    conn = (http.client.HTTPSConnection if u.scheme == "https" else http.client.HTTPConnection)(u.netloc, timeout=120)
    conn.putrequest(method, u.path + ("?" + u.query if u.query else ""), skip_accept_encoding=True)
    for k, v in headers.items():
        conn.putheader(k, v)
    conn.endheaders()
    r = conn.getresponse()
    resp = SimpleNamespace(status_code=r.status, headers=CaseInsensitiveDict(r.getheaders()), content=r.read())
    conn.close()
    return resp


def aws_chunked(req, body, algorithm):
    """Frames `body` as an unsigned aws-chunked upload with a trailing checksum, as SDKs send by default."""
    header = f"x-amz-checksum-{algorithm}"
    digest = base64.b64encode(TRAILER_DIGESTS[algorithm](body)).decode()
    req.headers["Content-Encoding"] = "aws-chunked"
    req.headers["x-amz-decoded-content-length"] = str(len(body))
    req.headers["x-amz-trailer"] = header
    # Makes botocore sign with STREAMING-UNSIGNED-PAYLOAD-TRAILER.
    req.context["checksum"] = {"request_algorithm": {"in": "trailer", "name": header}}
    chunk = f"{len(body):x}\r\n".encode() + body + b"\r\n" if body else b""
    framed = chunk + f"0\r\n{header}:{digest}\r\n\r\n".encode()
    req.data = framed
    return framed


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
        prefixes = ("x-amz-meta-", "x-amz-checksum-", "x-amz-server-side-encryption")
        if h.lower().startswith(prefixes) and h.lower() not in ignore_headers:
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
            # Attributes keep their namespace ({uri}name) so xsi:type and friends stay distinct.
            attrs = "".join(f" @{k}={fix(v)}" for k, v in sorted(el.attrib.items()))
            lines.append("  " * depth + tag + attrs + (f": {fix(text)}" if text else ""))
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
    ap.add_argument("scenarios", nargs="*", help="scenario files (default: all in scenarios/)")
    ap.add_argument("-v", "--verbose", action="store_true", help="print normalized responses for every step")
    args = ap.parse_args()

    paths = args.scenarios or sorted(str(p) for p in (HERE / "scenarios").glob("*.json"))
    prefix = os.environ.get("DIFF_BUCKET_PREFIX", "gb-diff")
    a = target_from_env("A", prefix)
    if not a.creds.access_key:
        sys.exit("side A needs credentials: set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or DIFF_A_*")
    a.sweep(prefix)
    diffs = sum(run(json.loads(Path(p).read_text()), Path(p).stem, prefix, args.verbose) for p in paths)
    print(f"{diffs} step(s) differ" if diffs else "no differences")
    sys.exit(1 if diffs else 0)


def run(scenario, label, prefix, verbose):
    """Replays one scenario on fresh buckets; returns the number of differing steps."""
    print(f"== {label}")
    a, b = target_from_env("A", prefix), target_from_env("B", prefix)
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
                if verbose:
                    print("\n".join("      " + line for line in la))
            else:
                diffs += 1
                print(f"DIFF  {name}")
                for line in difflib.unified_diff(la, lb, "A", "B", lineterm="", n=2):
                    print("      " + line)
    finally:
        a.cleanup()
        b.cleanup()
    return diffs


if __name__ == "__main__":
    main()
