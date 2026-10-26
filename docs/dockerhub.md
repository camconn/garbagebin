# Garbage Bin

A single-process, S3-compatible object store that runs as one Docker container for local development and testing.

Source, docs and issues: <https://github.com/camconn/garbagebin>

## Tags

Use `camconn/garbagebin:latest` unless you need a pinned version.

- `latest`: newest release
- `X.Y.Z`, `X.Y`: releases
- `edge`: tracks `master`
- `sha-<commit>`: a single build

Every tag covers `linux/amd64` and `linux/arm64`.

## Quick start

```sh
docker run --rm -p 9000:9000 camconn/garbagebin:latest

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
aws --endpoint-url http://localhost:9000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:9000 s3 cp README.md s3://my-bucket/
```

Use path-style addressing, or set `S3_VHOST_DOMAIN` for virtual-host style. `GET /health` returns 200 once the
server is ready; the image's `HEALTHCHECK` polls it.

## docker-compose

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
        condition: service_healthy
    environment:
      AWS_ENDPOINT_URL_S3: http://s3:9000
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
```

## Configuration

| Env var | Default | Notes |
| --- | --- | --- |
| `S3_LISTEN` | `0.0.0.0:9000` | HTTP only |
| `S3_BACKEND` | `memory` | `memory` or `fs` |
| `S3_DATA_DIR` | `/data` | `fs` backend only; a `VOLUME` |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | `test` / `test` | Checked only with strict auth |
| `S3_STRICT_AUTH` | `false` | Reject requests whose SigV4 signature doesn't verify |
| `S3_REGION` | `us-east-1` | |
| `S3_INIT_BUCKETS` | (none) | Comma-separated, created at startup |
| `S3_VHOST_DOMAIN` | (none) | e.g. `localhost` makes `mybucket.localhost` address `mybucket` |
| `S3_LOG` | `info` | JSON to stdout |
| `S3_NOW` | (system time) | RFC 3339 instant that fixes the clock for Object Lock, to permit manual clocks |

Full details in the [design doc](https://github.com/camconn/garbagebin/blob/master/docs/design.md#container-and-configuration).

## Testcontainers

Modules for Go, Python, Java and JS default to `camconn/garbagebin:latest`; see
[testcontainers/](https://github.com/camconn/garbagebin/tree/master/testcontainers).

## License

AGPL-3.0-or-later
