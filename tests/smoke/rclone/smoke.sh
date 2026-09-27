#!/usr/bin/env bash
# rclone smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
set -euo pipefail

fail() { echo "FAIL rclone: $*" >&2; exit 1; }
trap 'fail "command failed at line $LINENO: $BASH_COMMAND"' ERR

bucket="gb-smoke-rclone-$(od -An -N6 -tx1 /dev/urandom | tr -d ' \n')"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# On-the-fly remote "gb:"; ignore any user config.
export RCLONE_CONFIG="$tmp/rclone.conf"
: >"$RCLONE_CONFIG"
export RCLONE_CONFIG_GB_TYPE=s3
export RCLONE_CONFIG_GB_PROVIDER=Other
export RCLONE_CONFIG_GB_ENDPOINT=${S3_ENDPOINT:-http://localhost:9000}
export RCLONE_CONFIG_GB_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
export RCLONE_CONFIG_GB_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}
export RCLONE_CONFIG_GB_REGION=us-east-1
export RCLONE_CONFIG_GB_FORCE_PATH_STYLE=true
# rclone's default cutoff is far above 20 MB; lower it so the big file goes multipart.
export RCLONE_S3_UPLOAD_CUTOFF=8M RCLONE_S3_CHUNK_SIZE=5M
# Surface server errors instead of retrying past them.
export RCLONE_RETRIES=1 RCLONE_LOW_LEVEL_RETRIES=1

rc() { rclone -q "$@"; }

mkdir -p "$tmp/src/dir/a" "$tmp/src/dir/b"
printf 'hello, garbage bin\n' >"$tmp/src/dir/a/small.txt"
head -c $((20 * 1024 * 1024)) /dev/urandom >"$tmp/src/dir/b/big.bin"

rc mkdir "gb:$bucket"
rc copy "$tmp/src" "gb:$bucket"

# Compares sizes and MD5s (ETag, or rclone's md5chksum metadata for multipart).
rc check "$tmp/src" "gb:$bucket" || fail "rclone check reported differences"
want=$(md5sum <"$tmp/src/dir/b/big.bin" | cut -d' ' -f1)
got=$(rclone md5sum "gb:$bucket/dir/b/big.bin" | cut -d' ' -f1)
[[ $got == "$want" ]] || fail "big object md5: got $got want $want"

listing=$(rclone lsf "gb:$bucket/dir" | tr '\n' ' ')
[[ $listing == "a/ b/ " ]] || fail "lsf dirs: $listing"
[[ $(rclone lsd "gb:$bucket/dir" | wc -l) == 2 ]] || fail "lsd did not list 2 dirs"
[[ $(rclone ls "gb:$bucket" | wc -l) == 2 ]] || fail "ls did not list 2 objects"

cmp -s <(rclone cat --offset 100 --count 100 "gb:$bucket/dir/b/big.bin") \
    <(tail -c +101 "$tmp/src/dir/b/big.bin" | head -c 100) || fail "range read mismatch"

rc copyto "gb:$bucket/dir/a/small.txt" "gb:$bucket/copy/small.txt"
cmp -s <(rclone cat "gb:$bucket/copy/small.txt") "$tmp/src/dir/a/small.txt" || fail "copied object mismatch"

url=$(rclone link --expire 5m "gb:$bucket/copy/small.txt")
cmp -s <(curl -fsS "$url") "$tmp/src/dir/a/small.txt" || fail "presigned link mismatch"

rc delete "gb:$bucket"
[[ -z $(rclone ls "gb:$bucket") ]] || fail "bucket not empty after delete"
rc rmdir "gb:$bucket"
if rclone lsf gb: | grep -qx "$bucket/"; then fail "bucket still exists after rmdir"; fi

echo "PASS rclone"
