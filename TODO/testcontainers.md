# Testcontainers modules

Modules in [testcontainers/](../testcontainers/) pass their tests against the locally built `garbagebin` image (strict
auth on, so credentials are checked end to end); the `testcontainers` job in `merge.yml` runs them in CI.

- Publish: PyPI, Maven Central (Go: `testcontainers/go/v*` tags; npm: `testcontainers/js/v*` tags, see `npm.yml`)
