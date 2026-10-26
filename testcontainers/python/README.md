# testcontainers-garbagebin

[Testcontainers](https://testcontainers-python.readthedocs.io/) module for
[Garbage Bin](https://github.com/camconn/garbagebin), a single-process, S3-compatible object store for tests.

## Install

Not on PyPI yet; install from git:

```sh
uv add --dev "testcontainers-garbagebin @ git+https://github.com/camconn/garbagebin@v0.5.1#subdirectory=testcontainers/python"
# or
pip install "testcontainers-garbagebin @ git+https://github.com/camconn/garbagebin@v0.5.1#subdirectory=testcontainers/python"
```

Requires Python >= 3.10 and Docker.

## Usage

```python
import boto3
from testcontainers_garbagebin import GarbageBinContainer

with GarbageBinContainer(init_buckets=["uploads", "exports"]) as gb:
    s3 = boto3.client(
        "s3",
        endpoint_url=gb.endpoint(),
        region_name="us-east-1",
        aws_access_key_id=gb.access_key(),
        aws_secret_access_key=gb.secret_key(),
    )
    ...
```

Entering the `with` block waits until `GET /health` returns 200.

## Options

`GarbageBinContainer(...)` keyword arguments:

| Argument | Default | Effect |
| --- | --- | --- |
| `image` | `camconn/garbagebin:latest` | |
| `backend` | `memory` | `memory` or `fs` |
| `init_buckets` | `None` | Buckets created at startup |
| `access_key`, `secret_key` | `test` / `test` | Only checked with `S3_STRICT_AUTH=true` |

It is a `DockerContainer`, so `.with_env("S3_STRICT_AUTH", "true")` and the rest of its API work too. Server
settings: see the
[configuration table](https://github.com/camconn/garbagebin/blob/master/docs/design.md#container-and-configuration).

## License

AGPL-3.0-or-later
