#!/usr/bin/env bash
# Run Ceph s3-tests against a running endpoint and compare failures to tests/s3-tests/allowlist.txt.
#
# Usage: scripts/s3-tests.sh [extra pytest args, e.g. -k test_bucket_list]
#
# Env:
#   S3_ENDPOINT            endpoint to test (default http://localhost:9000); must already be running
#   AWS_ACCESS_KEY_ID      key pair used for every s3-tests user (default test/test)
#   AWS_SECRET_ACCESS_KEY
#   S3TESTS_DIR            checkout location (default ~/.cache/garbagebin/s3-tests)
#   S3TESTS_OUT            junit/conf output dir (default target/s3-tests)
#   S3TESTS_JOBS           pytest-xdist workers (default auto; 0 disables)
#
# Runs test_s3.py and test_headers.py with out-of-scope markers deselected (see MARKERS).
# Exits non-zero if a test fails that no allowlist pattern matches. Prints allowlist patterns
# whose tests all passed, so the list can shrink.
set -euo pipefail

S3TESTS_REPO=https://github.com/ceph/s3-tests
S3TESTS_COMMIT=5522d1c351f75bc00ae0f64f742f3f095f5939d9

root=$(cd "$(dirname "$0")/.." && pwd)
endpoint=${S3_ENDPOINT:-http://localhost:9000}
access_key=${AWS_ACCESS_KEY_ID:-test}
secret_key=${AWS_SECRET_ACCESS_KEY:-test}
dir=${S3TESTS_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/garbagebin/s3-tests}
out=${S3TESTS_OUT:-$root/target/s3-tests}
jobs=${S3TESTS_JOBS:-auto}
allowlist=$root/tests/s3-tests/allowlist.txt

# Features Garbage Bin never builds or defers (docs/design.md "Out of scope"), plus RGW-only behaviour.
MARKERS=(
  fails_on_aws appendobject abac_test
  auth_aws2
  bucket_logging bucket_logging_cleanup fails_without_logging_rollover
  bucket_policy user_policy role_policy session_policy group group_policy object_ownership
  encryption bucket_encryption sse_s3
  lifecycle lifecycle_expiration lifecycle_transition cloud_transition cloud_restore target_by_bucket storage_class
  object_lock
  iam_account iam_cross_account iam_role iam_tenant iam_user
  test_of_sts webidentity_test token_claims_trust_policy_test token_principal_tag_role_policy_test
  token_request_tag_trust_policy_test token_resource_tags_test token_role_tags_test token_tag_keys_test
  s3select s3website s3website_routing_rules s3website_redirect_location s3control sns
)

if [[ "$(git -C "$dir" rev-parse HEAD 2>/dev/null)" != "$S3TESTS_COMMIT" ]]; then
  rm -rf "$dir"
  mkdir -p "$dir"
  git -C "$dir" init -q
  git -C "$dir" remote add origin "$S3TESTS_REPO"
  git -C "$dir" fetch -q --depth 1 origin "$S3TESTS_COMMIT"
  git -C "$dir" checkout -q FETCH_HEAD
fi

if ! curl -sS -o /dev/null "$endpoint/"; then
  echo "s3-tests: endpoint $endpoint is not reachable" >&2
  exit 2
fi

hostport=${endpoint#*://}
hostport=${hostport%%/*}
host=${hostport%:*}
port=${hostport##*:}
secure=False
[[ $endpoint == https://* ]] && secure=True
[[ $hostport == "$host" ]] && { [[ $secure == True ]] && port=443 || port=80; }

mkdir -p "$out"
conf=$out/s3tests.conf
junit=$out/junit.xml
rm -f "$junit"

user() { # section, id
  cat <<EOF
[$1]
display_name = $2
user_id = $2
email = $2@example.com
access_key = $access_key
secret_key = $secret_key
EOF
}
{
  cat <<EOF
[DEFAULT]
host = $host
port = $port
is_secure = $secure
ssl_verify = False

[fixtures]
bucket prefix = gb-{random}-

EOF
  user "s3 main" main; echo "api_name = us-east-1"; echo
  user "s3 alt" alt; echo
  user "s3 tenant" tenant; echo "tenant = testx"; echo
  user "iam" iam; echo
  user "iam root" root; echo "account_id = root"; echo
  user "iam alt root" altroot; echo "account_id = altroot"
} >"$conf"

deselect=$(printf ' or %s' "${MARKERS[@]}")
xdist=()
[[ $jobs != 0 ]] && xdist=(-n "$jobs")

set +e
(cd "$dir" && S3TEST_CONF=$conf PYTHONPATH="$dir" uv run --quiet --no-project --python 3.12 \
  --with-requirements requirements.txt --with pytest-xdist \
  pytest s3tests/functional/test_s3.py s3tests/functional/test_headers.py \
  -m "not (${deselect# or })" -p no:cacheprovider -q -rN --tb=line --show-capture=no --junitxml="$junit" "${xdist[@]}" "$@")
status=$?
set -e
# pytest: 0 all passed, 1 some failed, 5 none collected; anything else is a harness error.
if [[ $status -ne 0 && $status -ne 1 ]]; then
  echo "s3-tests: pytest exited $status" >&2
  exit "$status"
fi

uv run --quiet --no-project --python 3.12 python - "$junit" "$allowlist" <<'EOF'
import fnmatch, re, sys, xml.etree.ElementTree as ET

junit, allowlist = sys.argv[1:]
patterns = []
for line in open(allowlist):
    line = line.split("#", 1)[0].strip()
    if line:
        patterns.append(line)

results = {}  # test name -> passed
for case in ET.parse(junit).iter("testcase"):
    name = re.sub(r"\[.*\]$", "", case.get("name"))
    if case.find("skipped") is not None:
        continue
    failed = case.find("failure") is not None or case.find("error") is not None
    results[name] = results.get(name, True) and not failed

def matches(p, name):
    return fnmatch.fnmatchcase(name, p)

unexpected = sorted(n for n, ok in results.items() if not ok and not any(matches(p, n) for p in patterns))
removable = []
for p in patterns:
    hit = [ok for n, ok in results.items() if matches(p, n)]
    if hit and all(hit):
        removable.append(p)

passed = sum(results.values())
allowed = sum(1 for n, ok in results.items() if not ok) - len(unexpected)
print(f"s3-tests: {passed} passed, {allowed} allowlisted failures, {len(unexpected)} unexpected failures")
if removable:
    print("allowlist patterns whose tests all passed in this run (remove them):")
    for p in removable:
        print(f"  {p}")
if unexpected:
    print("unexpected failures (fix, or allowlist if out of scope):")
    for n in unexpected:
        print(f"  {n}")
    sys.exit(1)
EOF
