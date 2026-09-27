# Garbage Bin

A single-process, S3-compatible object store that runs as one Docker container for local development and
integration tests. Unmodified AWS SDKs work against it with only an endpoint override.

Package `garbagebin`, binary `gb`. See [TASKS.md](TASKS.md) for status and the [design doc](docs/design.md)
for rationale.

## Usage

### Docker

The image is not published yet; build it first.

```sh
docker build -t garbagebin .
docker run --rm -p 9000:9000 garbagebin

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket
```

Any credentials work unless strict auth is on (`S3_STRICT_AUTH=true` checks them against `S3_ACCESS_KEY` /
`S3_SECRET_KEY`). Use path-style addressing, or set `S3_VHOST_DOMAIN` for virtual-host style. `GET /health`
returns 200 once the server is ready.

### docker-compose

```yaml
services:
  s3:
    image: garbagebin
    ports: ["9000:9000"]
    environment:
      S3_INIT_BUCKETS: uploads,exports
      # S3_BACKEND: fs          # keep data across restarts; mount a volume at /data
    # volumes: ["./s3-data:/data"]
  app:
    build: .
    depends_on:
      s3:
        condition: service_healthy  # the image's built-in HEALTHCHECK polls /health
    environment:
      AWS_ENDPOINT_URL_S3: http://s3:9000
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
```

### Bare binary

No Docker needed, e.g. on CI runners:

```sh
cargo build --release
target/release/gb --backend memory
target/release/gb --backend fs --data-dir ./data --listen 127.0.0.1:9000
```

### Configuration

Environment variables, or the matching flags (`gb --help`); there is no config file. See the configuration
table in [docs/design.md](docs/design.md#container-and-configuration) for every setting and its default.

### Testcontainers

Modules for Go, Python, Java and JS live in [testcontainers/](testcontainers/) (not yet published to package
registries). Each starts the `garbagebin` image (configurable), waits on `/health`, takes options for the
backend, init buckets and credentials, and exposes the endpoint, access key and secret key:

```go
ctr, err := garbagebin.Run(ctx, garbagebin.WithInitBuckets("uploads"))
defer ctr.Terminate(ctx)
// ctr.Endpoint(), ctr.AccessKey(), ctr.SecretKey()
```

- Python: `GarbageBinContainer(init_buckets=[...])`, then `.endpoint()`
- Java: `new GarbageBinContainer().withInitBuckets(...)`, then `.getEndpoint()`
- JS: `await new GarbageBinContainer().withInitBuckets(...).start()`, then `.endpoint()`

## Development

The toolchain is pinned in `rust-toolchain.toml`; rustup installs it on first use.

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
docker build -t garbagebin .
```

## Testing

| Command | What it does |
| --- | --- |
| `cargo test` | Unit and integration tests |
| `scripts/smoke.sh [--backend memory\|fs] [sdk...]` | SDK smoke tests in [tests/smoke/](tests/smoke/) (boto3, aws-sdk-go-v2, aws-sdk-js v3, aws-sdk-java v2, AWS CLI, rclone), in parallel. Builds and starts `gb` on a free port unless `S3_ENDPOINT` is set. The merge gate. |
| `scripts/s3-tests.sh` | Ceph s3-tests against `S3_ENDPOINT`; failures must be listed in [tests/s3-tests/allowlist.txt](tests/s3-tests/allowlist.txt), which can only shrink |
| `scripts/mint.sh` | MinIO mint (Docker) against `S3_ENDPOINT` |
| `uv run tests/differential/diff.py [-v] [scenario.json]` | Replays [tests/differential/scenarios/](tests/differential/scenarios/) against side A (real S3, using `AWS_*` credentials) and side B (`DIFF_B_ENDPOINT`, default `http://localhost:9000`) and diffs the responses, ignoring request ids and dates. `DIFF_A_ENDPOINT` points side A at another server instead |
| `go test ./...`, `uv run pytest`, `npm ci && npm test`, `./gradlew test` | Testcontainers module tests, run in each `testcontainers/<lang>/` directory against the local `garbagebin` image (`GB_TEST_IMAGE` overrides) |

`s3-tests.sh` and `mint.sh` need a running server (e.g. `target/release/gb`); `mint.sh` also needs Docker.
Smoke tests need uv, Go, Node, Java, the AWS CLI v2 and rclone. Every script reads `S3_ENDPOINT`,
`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (default `test`/`test`), so they also run against any other
S3 endpoint. CI runs fmt, clippy, tests and smoke tests on every push ([ci.yml](.github/workflows/ci.yml)),
conformance suites on push and PR ([conformance.yml](.github/workflows/conformance.yml)), and the
differential tests, multi-arch image build and Testcontainers module tests nightly
([nightly.yml](.github/workflows/nightly.yml)).

## License

Copyright (C) 2026 Cameron Conn

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
General Public License as published by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version. See [LICENSE](LICENSE) for the full text.
