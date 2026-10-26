# Garbage Bin

A single-process, S3-compatible object store that runs as one Docker container for local development and testing.

Package `garbagebin`, binary `gb`.

## Usage

### Docker

[`camconn/garbagebin:latest`](https://hub.docker.com/r/camconn/garbagebin) (amd64, arm64). `edge` tracks `master`.

```sh
docker run --rm -p 9000:9000 camconn/garbagebin:latest

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://my-bucket/
aws --endpoint-url http://localhost:9000 s3 ls s3://my-bucket
```

Any credentials work unless `S3_STRICT_AUTH=true`. Path-style by default; set `S3_VHOST_DOMAIN` for
virtual-host style.

### docker-compose

```yaml
services:
  s3:
    image: camconn/garbagebin:latest
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

```sh
cargo build --release
target/release/gb --backend memory # run in-memory
target/release/gb --backend fs --data-dir ./data --listen 127.0.0.1:9000 # run and store on fs
```

### Configuration

Environment variables or flags (`gb --help`). All settings:
[docs/design.md](docs/design.md#container-and-configuration).

### Testcontainers

Modules for Go, Python, Java and JS in [testcontainers/](testcontainers/), defaulting to
`camconn/garbagebin:latest`:

```go
ctr, err := garbagebin.Run(ctx, garbagebin.WithInitBuckets("uploads"))
defer ctr.Terminate(ctx)
// ctr.Endpoint(), ctr.AccessKey(), ctr.SecretKey()
```

- Python: `GarbageBinContainer(init_buckets=[...])`, then `.endpoint()`
- Java: `new GarbageBinContainer().withInitBuckets(...)`, then `.getEndpoint()`
- JS: `await new GarbageBinContainer().withInitBuckets(...).start()`, then `.endpoint()`

## Development

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
docker build -t garbagebin .
```

## Testing

```sh
cargo test
scripts/smoke.sh   # SDK smoke tests; the merge gate
```

Conformance suites, differential tests, prerequisites and CI: [docs/testing.md](docs/testing.md).

## License

Copyright (C) 2026 Cameron Conn

This program is distributed under the GNU Affero General Public License, version 3 or (at your option) any
later version. See [LICENSE](LICENSE) for the full text.
