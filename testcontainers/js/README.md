# @camconn/testcontainers-garbagebin

[Testcontainers](https://node.testcontainers.org/) module for
[Garbage Bin](https://github.com/camconn/garbagebin), a single-process, S3-compatible object store for tests.

## Install

```sh
npm install --save-dev @camconn/testcontainers-garbagebin testcontainers
```

`testcontainers` (>= 12) is a peer dependency. Docker must be available.

## Usage

```js
import { S3Client } from "@aws-sdk/client-s3";
import { GarbageBinContainer } from "@camconn/testcontainers-garbagebin";

const gb = await new GarbageBinContainer()
  .withInitBuckets("uploads", "exports")
  .start();

const s3 = new S3Client({
  endpoint: gb.endpoint(),
  region: "us-east-1",
  forcePathStyle: true,
  credentials: { accessKeyId: gb.accessKey(), secretAccessKey: gb.secretKey() },
});

// ...

await gb.stop();
```

`start()` resolves once `GET /health` returns 200.

## API

`new GarbageBinContainer(image?)`: image defaults to `camconn/garbagebin:latest`. Extends `GenericContainer`,
so its methods (`withEnvironment`, `withReuse`, ...) work too.

| Method | Effect |
| --- | --- |
| `withBackend("memory" \| "fs")` | Storage backend; default `memory` |
| `withInitBuckets(...names)` | Buckets created at startup |
| `withCredentials(accessKey, secretKey)` | Default `test` / `test`; only checked with `S3_STRICT_AUTH=true` |

The started container exposes `endpoint()`, `accessKey()`, `secretKey()`, `stop()`, and the underlying
`StartedTestContainer` as `.container`.

Other server settings are environment variables, e.g. `.withEnvironment({ S3_STRICT_AUTH: "true" })`; see the
[configuration table](https://github.com/camconn/garbagebin/blob/master/docs/design.md#container-and-configuration).

## License

AGPL-3.0-or-later
