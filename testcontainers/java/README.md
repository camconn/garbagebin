# Garbage Bin for Testcontainers Java

[Testcontainers](https://java.testcontainers.org/) module for
[Garbage Bin](https://github.com/camconn/garbagebin), a single-process, S3-compatible object store for tests.

## Install

Not on Maven Central yet; install to your local Maven repository from source (needs JDK 25):

```sh
git clone --branch v0.5.1 https://github.com/camconn/garbagebin
cd garbagebin/testcontainers/java
./gradlew publishToMavenLocal
```

Then depend on it, with `mavenLocal()` in your repositories:

```groovy
testImplementation 'io.github.camconn:testcontainers-garbagebin:0.5.1'
```

It brings in `org.testcontainers:testcontainers` as an API dependency. Docker must be available.

## Usage

```java
import io.github.camconn.garbagebin.GarbageBinContainer;
import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

try (var gb = new GarbageBinContainer().withInitBuckets("uploads", "exports")) {
    gb.start();
    var s3 = S3Client.builder()
            .endpointOverride(URI.create(gb.getEndpoint()))
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(gb.getAccessKey(), gb.getSecretKey())))
            .build();
    // ...
}
```

`start()` returns once `GET /health` returns 200.

## Options

| Method | Effect |
| --- | --- |
| `new GarbageBinContainer(image)` | Default `camconn/garbagebin:latest` (`DEFAULT_IMAGE`) |
| `withBackend("memory" \| "fs")` | Storage backend; default `memory` |
| `withInitBuckets(names...)` | Buckets created at startup |
| `withCredentials(accessKey, secretKey)` | Default `test` / `test`; only checked with `S3_STRICT_AUTH=true` |

It extends `GenericContainer`, so `.withEnv("S3_STRICT_AUTH", "true")` and the rest of its API work too. Server
settings: see the
[configuration table](https://github.com/camconn/garbagebin/blob/master/docs/design.md#container-and-configuration).

## License

AGPL-3.0-or-later
