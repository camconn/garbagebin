# Testing

Why each suite exists is in [design.md](design.md#conformance-and-testing); this is how to run them.

| Command | What it does |
| --- | --- |
| `cargo test` | Unit and integration tests |
| `scripts/smoke.sh [--backend memory\|fs] [sdk...]` | SDK smoke tests in [tests/smoke/](../tests/smoke/) (boto3, aws-sdk-go-v2, aws-sdk-js v3, aws-sdk-java v2, AWS CLI, rclone), in parallel. Builds and starts `gb` on a free port unless `S3_ENDPOINT` is set. The merge gate. |
| `scripts/s3-tests.sh` | Ceph s3-tests against `S3_ENDPOINT`; failures must be listed in [tests/s3-tests/allowlist.txt](../tests/s3-tests/allowlist.txt), which can only shrink |
| `scripts/mint.sh` | MinIO mint (Docker) against `S3_ENDPOINT` |
| `uv run tests/differential/diff.py [-v] [scenario.json...]` | Differential tests; see below |
| `go test ./...`, `uv run pytest`, `npm ci && npm test`, `./gradlew test` | Testcontainers module tests, run in each `testcontainers/<lang>/` directory against the locally built `garbagebin:latest` image (`GB_TEST_IMAGE` overrides) |

## Prerequisites

- `s3-tests.sh` and `mint.sh` need a running server (e.g. `target/release/gb`); `mint.sh` also needs Docker.
- Smoke tests need uv, Go, Node, Java, the AWS CLI v2 and rclone.
- Every script reads `S3_ENDPOINT`, `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (default `test`/`test`), so
  they also run against any other S3 endpoint.

## Differential tests

`diff.py` replays the scenarios in [tests/differential/scenarios/](../tests/differential/scenarios/) (all by
default) against two sides and diffs the responses, ignoring request ids and dates.

- Side A is real S3, using `AWS_*` credentials; `DIFF_A_ENDPOINT` points it at another server instead.
- Side B is `DIFF_B_ENDPOINT`, default `http://localhost:9000`. Run it with strict auth (the scenarios include
  bad signatures) and `S3_VHOST_DOMAIN` set to its endpoint's host, e.g. `localhost` (steps with
  `"vhost": true` address `<bucket>.<endpoint host>`).
- `DIFF_A_REGION`/`DIFF_B_REGION` pick each side's region; outside `us-east-1`, CreateBucket sends a
  `LocationConstraint`.
- Side A's stale `gb-diff-*` buckets (older than `STALE_AFTER`) are deleted first, as left over from a killed
  run.

Run by hand; not in CI.

## CI

- [ci.yml](../.github/workflows/ci.yml), every push: fmt, clippy, tests, smoke tests.
- [conformance.yml](../.github/workflows/conformance.yml), push and PR: s3-tests and mint, both backends.
- [merge.yml](../.github/workflows/merge.yml), merge to `master` and `v*` tags: Testcontainers module tests,
  then the multi-arch image push to Docker Hub and the Hub overview sync from [dockerhub.md](dockerhub.md).
