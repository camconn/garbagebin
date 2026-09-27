#!/usr/bin/env bash
# AWS CLI smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
set -euo pipefail

fail() { echo "FAIL cli: $*" >&2; exit 1; }
trap 'fail "command failed at line $LINENO: $BASH_COMMAND"' ERR

endpoint=${S3_ENDPOINT:-http://localhost:9000}
bucket="gb-smoke-cli-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Isolate from the user's AWS config; force path-style.
export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
export AWS_DEFAULT_REGION=us-east-1
export AWS_CONFIG_FILE="$tmp/config" AWS_SHARED_CREDENTIALS_FILE=/dev/null
unset AWS_PROFILE AWS_SESSION_TOKEN
printf '[default]\ns3 =\n  addressing_style = path\n' >"$AWS_CONFIG_FILE"

aws() { command aws --endpoint-url "$endpoint" --output text "$@"; }

printf 'hello, garbage bin\n' >"$tmp/small"
head -c $((20 * 1024 * 1024)) /dev/urandom >"$tmp/big"

aws s3 mb "s3://$bucket" >/dev/null
aws s3 cp "$tmp/small" "s3://$bucket/dir/a/small.txt" >/dev/null

# aws s3 cp switches to multipart above 8 MB.
aws s3 cp "$tmp/big" "s3://$bucket/dir/b/big.bin" >/dev/null
etag=$(aws s3api head-object --bucket "$bucket" --key dir/b/big.bin --query ETag)
[[ $etag == *-* ]] || fail "big object ETag is not multipart: $etag"
aws s3 cp "s3://$bucket/dir/b/big.bin" "$tmp/big.got" >/dev/null
cmp -s "$tmp/big" "$tmp/big.got" || fail "big object body mismatch"

prefixes=$(aws s3api list-objects-v2 --bucket "$bucket" --prefix dir/ --delimiter / \
    --query 'CommonPrefixes[].Prefix' | tr '\t' ' ')
[[ $prefixes == "dir/a/ dir/b/" ]] || fail "CommonPrefixes: $prefixes"

aws s3api get-object --bucket "$bucket" --key dir/b/big.bin --range bytes=100-199 "$tmp/range" >/dev/null
cmp -s "$tmp/range" <(tail -c +101 "$tmp/big" | head -c 100) || fail "range get bytes mismatch"

aws s3 cp "s3://$bucket/dir/a/small.txt" "s3://$bucket/copy/small.txt" >/dev/null
aws s3 cp "s3://$bucket/copy/small.txt" "$tmp/copy" >/dev/null
cmp -s "$tmp/small" "$tmp/copy" || fail "copied object mismatch"

url=$(aws s3 presign "s3://$bucket/copy/small.txt" --expires-in 300)
curl -fsS "$url" -o "$tmp/presigned" || fail "presigned get failed"
cmp -s "$tmp/small" "$tmp/presigned" || fail "presigned get mismatch"

deleted=$(aws s3api delete-objects --bucket "$bucket" --output json --delete \
    '{"Objects":[{"Key":"dir/a/small.txt"},{"Key":"dir/b/big.bin"},{"Key":"copy/small.txt"}]}')
[[ $(jq '.Deleted | length' <<<"$deleted") == 3 ]] || fail "delete-objects: $deleted"
[[ $(jq '.Errors // [] | length' <<<"$deleted") == 0 ]] || fail "delete-objects: $deleted"
left=$(aws s3api list-objects-v2 --bucket "$bucket" --query 'length(Contents || `[]`)')
[[ $left == 0 ]] || fail "bucket not empty after batch delete: $left"

aws s3api delete-bucket --bucket "$bucket"
if aws s3api head-bucket --bucket "$bucket" 2>/dev/null; then fail "bucket still exists after delete"; fi

echo "PASS cli"
