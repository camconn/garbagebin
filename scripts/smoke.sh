#!/usr/bin/env bash
# Run the SDK smoke tests (tests/smoke/*) in parallel.
#
# Usage: scripts/smoke.sh [--backend memory|fs] [sdk...]
#   sdk: boto3 go js java cli rclone (default: all)
#
# Starts gb on a free port unless S3_ENDPOINT is set. GB_BIN overrides the binary
# (default: cargo build --release, then target/release/gb).
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
backend=memory
sdks=()
while (($#)); do
    case $1 in
        --backend) backend=$2; shift 2 ;;
        --backend=*) backend=${1#*=}; shift ;;
        -h | --help) sed -n '2,8p' "$0"; exit 0 ;;
        *) sdks+=("$1"); shift ;;
    esac
done
[[ $backend == memory || $backend == fs ]] || { echo "unknown backend: $backend" >&2; exit 2; }
((${#sdks[@]})) || sdks=(boto3 go js java cli rclone)

work=$(mktemp -d)
gb_pid=
cleanup() {
    [[ -n $gb_pid ]] && kill "$gb_pid" 2>/dev/null && wait "$gb_pid" 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-test}
export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-test}

if [[ -z ${S3_ENDPOINT:-} ]]; then
    if [[ -z ${GB_BIN:-} ]]; then
        cargo build --release --locked --quiet --manifest-path "$root/Cargo.toml"
        GB_BIN=$root/target/release/gb
    fi
    port=$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')
    mkdir -p "$work/data"
    S3_LISTEN=127.0.0.1:$port S3_BACKEND=$backend S3_DATA_DIR=$work/data \
        S3_ACCESS_KEY=$AWS_ACCESS_KEY_ID S3_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
        "$GB_BIN" >"$work/gb.log" 2>&1 &
    gb_pid=$!
    export S3_ENDPOINT=http://127.0.0.1:$port
    for _ in $(seq 100); do
        curl -fsS -o /dev/null "$S3_ENDPOINT/health" 2>/dev/null && break
        kill -0 "$gb_pid" 2>/dev/null || { cat "$work/gb.log"; echo "gb exited" >&2; exit 1; }
        sleep 0.1
    done
    curl -fsS -o /dev/null "$S3_ENDPOINT/health" || { cat "$work/gb.log"; echo "gb not healthy" >&2; exit 1; }
    echo "gb ($backend) on $S3_ENDPOINT"
else
    echo "using $S3_ENDPOINT"
fi

smoke=$root/tests/smoke
run_sdk() {
    case $1 in
        boto3) uv run --quiet "$smoke/boto3/smoke.py" ;;
        go) cd "$smoke/go" && go run . ;;
        js) cd "$smoke/js" && npm ci --silent --prefer-offline --no-audit --no-fund && node smoke.mjs ;;
        java) cd "$smoke/java" && ./gradlew --quiet run ;;
        cli) "$smoke/cli/smoke.sh" ;;
        rclone) "$smoke/rclone/smoke.sh" ;;
        *) echo "unknown sdk: $1" >&2; return 2 ;;
    esac
}

declare -A pids
start=$SECONDS
for sdk in "${sdks[@]}"; do
    (timeout 540 bash -c "$(declare -f run_sdk); smoke=$smoke; set -e; run_sdk $sdk") >"$work/$sdk.log" 2>&1 &
    pids[$sdk]=$!
done

failed=()
for sdk in "${sdks[@]}"; do
    if wait "${pids[$sdk]}"; then
        echo "PASS $sdk"
    else
        echo "FAIL $sdk"
        sed 's/^/    /' "$work/$sdk.log"
        failed+=("$sdk")
    fi
done
echo "smoke: $((${#sdks[@]} - ${#failed[@]}))/${#sdks[@]} passed in $((SECONDS - start))s"
if ((${#failed[@]})); then
    [[ -f $work/gb.log ]] && { echo "--- gb log (tail)"; tail -n 50 "$work/gb.log"; }
    exit 1
fi
