# Garbage Bin

A single-process, S3-compatible object store that runs as one Docker container for local development and
integration tests. Unmodified AWS SDKs work against it with only an endpoint override.

Package `garbagebin`, binary `gb`. Status: scaffolding only. See [TASKS.md](TASKS.md) for the plan and the
[design doc](docs/design.md) for rationale.

## Development

The toolchain is pinned in `rust-toolchain.toml`; rustup installs it on first use.

```sh
cargo build
cargo test
cargo clippy --all-targets -- -D warnings
docker build -t garbagebin .
```

## License

Copyright (C) 2026 Cameron Conn

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
General Public License as published by the Free Software Foundation, either version 3 of the License, or (at
your option) any later version. See [LICENSE](LICENSE) for the full text.
