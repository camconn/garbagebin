#!/usr/bin/env bash
# Run MinIO mint against a running endpoint.
#
# Usage: scripts/mint.sh [mint test names, e.g. aws-sdk-go-v2 s3cmd]   (default: SUITES)
#
# The endpoint should run with --strict-auth and the same key pair: mint asserts that bad signatures fail.
# minio/* images are no longer on Docker Hub, so the image is built once from mint's source with
# scripts/mint.Dockerfile (SUITES only, EXCLUDE removed) and cached by a tag hashing both.
#
# Env:
#   S3_ENDPOINT            endpoint to test (default http://localhost:9000); must already be running
#   AWS_ACCESS_KEY_ID      default test
#   AWS_SECRET_ACCESS_KEY  default test
#   S3_REGION              default us-east-1
#   MINT_IMAGE             image to run (default: built from minio/mint source at MINT_COMMIT)
#   MINT_DIR               source checkout (default ~/.cache/garbagebin/mint)
#   MINT_MODE              full (default; minio-go's multipart and listing tests only run here) or core
#   MINT_OUT               log dir (default target/mint); log.json has one line per test
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
endpoint=${S3_ENDPOINT:-http://localhost:9000}
out=${MINT_OUT:-$root/target/mint}

MINT_REPO=https://github.com/minio/mint
MINT_COMMIT=12559d50625b722d11fd798ae8ac2fb204e66dd1

# Suites built into the image. Left out:
#   aws-sdk-java-v2, minio-java, minio-js, aws-sdk-php, aws-sdk-ruby: need JVM/Gradle, Node, PHP and Ruby
#     toolchains that made the build impractically slow; Java and JS SDKs are covered by tests/smoke
#   mc: its binary downloads (dl.min.io) are gone
#   s3select: out of scope (docs/design.md)
#   healthcheck: MinIO's own /minio/health endpoints
SUITES=(awscli aws-sdk-go-v2 minio-go minio-py s3cmd versioning)

# suite:test pairs removed from the image. Tests that only log NA (NotImplemented) need no entry.
EXCLUDE=(
  # SigV2: design is SigV4 only; AWS has refused SigV2 for buckets created since 2020.
  minio-go:testFunctionalV2 minio-go:testMakeBucketErrorV2 minio-go:testGetObjectClosedTwiceV2
  minio-go:testFPutObjectV2 minio-go:testMakeBucketRegionsV2 minio-go:testGetObjectReadSeekFunctionalV2
  minio-go:testGetObjectReadAtFunctionalV2 minio-go:testCopyObjectV2 minio-go:testComposeObjectErrorCasesV2
  minio-go:testCompose10KSourcesV2 minio-go:testUserMetadataCopyingV2 minio-go:testPutObject0ByteV2
  minio-go:testPutObjectMetadataNonUSASCIIV2 minio-go:testPutObjectNoLengthV2 minio-go:testPutObjectsUnknownV2
  minio-go:testGetObjectContextV2 minio-go:testFPutObjectContextV2 minio-go:testFGetObjectContextV2
  minio-go:testPutObjectContextV2 minio-go:testCopyObjectWithChecksums minio-go:testReplaceObjectWithChecksums
  # Buckets in other regions: one region per server.
  minio-go:testMakeBucketError minio-go:testMakeBucketRegions aws-sdk-go-v2:testCreateBucketError
  # Operations outside docs/design.md's "In scope" table.
  aws-sdk-go-v2:testSelectObject aws-sdk-go-v2:testGetObjectAttributesBasic
  aws-sdk-go-v2:testGetObjectAttributesWithChecksum aws-sdk-go-v2:testGetObjectAttributesMultipart
  aws-sdk-go-v2:testGetObjectAttributesNonExistent aws-sdk-go-v2:testGetObjectAttributesCombined
  # Wants BadRequest for more than 10 tags; Ceph s3-tests (the CI gate) wants gb's InvalidTag.
  aws-sdk-go-v2:testObjectTaggingErrors
  # Asserts MinIO's ListMultipartUploads (no Owner/Initiator/StorageClass; key-marker without
  # upload-id-marker includes the marker key), which differs from AWS.
  minio-go:testListMultipartUpload
  # Object Lock (deferred, docs/design.md).
  minio-go:testRemoveMultipleObjectsWithResult versioning:testLockingLegalhold
  versioning:testPutGetRetentionCompliance versioning:testPutGetDeleteRetentionGovernance
  versioning:testLockingRetentionGovernance versioning:testLockingRetentionCompliance
  # Browser POST uploads (PostObject) are not an in-scope operation.
  minio-go:testPresignedPostPolicy minio-go:testPresignedPostPolicyWrongFile
  # Per-part checksums on GET partNumber are not kept (TODO/s3-api-gaps.md).
  minio-go:testTrailingChecksums
  # Bucket versioning ExcludedPrefixes/ExcludeFolders are MinIO extensions.
  minio-go:testBucketVersioningExcludedPrefixes minio-py:test_set_get_bucket_versioning
  # GetObjectAttributes is not an in-scope operation (reached now that its versioned setup works).
  minio-go:testGetObjectAttributesErrorCases
  # Snowball auto-extract is a MinIO extension.
  minio-py:test_upload_snowball_objects minio-py:test_upload_snowball_objects_with_staging
)

tag=$(printf '%s\n' "$MINT_COMMIT" "${SUITES[*]}" "${EXCLUDE[*]}" | cat - "$root/scripts/mint.Dockerfile" |
  sha256sum | cut -c1-12)
image=${MINT_IMAGE:-garbagebin-mint:$tag}
if ! docker image inspect "$image" >/dev/null 2>&1; then
  src=${MINT_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/garbagebin/mint}
  if [[ "$(git -C "$src" rev-parse HEAD 2>/dev/null)" != "$MINT_COMMIT" ]]; then
    rm -rf "$src"
    mkdir -p "$src"
    git -C "$src" init -q
    git -C "$src" remote add origin "$MINT_REPO"
    git -C "$src" fetch -q --depth 1 origin "$MINT_COMMIT"
    git -C "$src" checkout -q FETCH_HEAD
  fi
  docker build -t "$image" -f "$root/scripts/mint.Dockerfile" \
    --build-arg SUITES="${SUITES[*]}" --build-arg EXCLUDE="${EXCLUDE[*]}" "$src"
fi

hostport=${endpoint#*://}
hostport=${hostport%%/*}
https=0
[[ $endpoint == https://* ]] && https=1

[[ $# -eq 0 ]] && set -- "${SUITES[@]}"

rm -rf "$out"
mkdir -p "$out"

# Logs are copied out rather than bind-mounted so they aren't root-owned.
name=gb-mint-$$
trap 'docker rm -f "$name" >/dev/null 2>&1' EXIT
set +e
docker run --name "$name" --network host \
  -e SERVER_ENDPOINT="$hostport" \
  -e ACCESS_KEY="${AWS_ACCESS_KEY_ID:-test}" \
  -e SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-test}" \
  -e SERVER_REGION="${S3_REGION:-us-east-1}" \
  -e ENABLE_HTTPS="$https" \
  -e MINT_MODE="${MINT_MODE:-full}" \
  -e RUN_ON_FAIL=1 \
  "$image" "$@"
status=$?
set -e
docker cp -q "$name":/mint/log/. "$out"

if [[ -s $out/log.json ]] && command -v jq >/dev/null; then
  jq -Rr 'fromjson? | select(.status != "PASS") | "\(.status)\t\(.name)\t\(.function // "")\t\(.error // .message // "")"' \
    "$out"/log.json | cut -c1-300
  jq -Rrn '[inputs | fromjson?] | group_by(.status) | map("\(.[0].status): \(length)") | join(", ")' "$out"/log.json
fi
echo "mint: exit $status, logs in $out"
exit "$status"
