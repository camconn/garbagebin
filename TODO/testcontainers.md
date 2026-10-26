# Testcontainers modules

Modules in [testcontainers/](../testcontainers/) pass their tests against the locally built `garbagebin` image (strict
auth on, so credentials are checked end to end); the `testcontainers` job in `merge.yml` runs them in CI.

- Publish: Go module tag, PyPI, Maven Central, npm
