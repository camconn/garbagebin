# Garbage Bin for testcontainers-go

[Testcontainers](https://golang.testcontainers.org/) module for
[Garbage Bin](https://github.com/camconn/garbagebin), a single-process, S3-compatible object store for tests.

## Install

```sh
go get github.com/camconn/garbagebin/testcontainers/go
```

Docker must be available.

## Usage

```go
import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	garbagebin "github.com/camconn/garbagebin/testcontainers/go"
)

func TestS3(t *testing.T) {
	ctx := context.Background()
	ctr, err := garbagebin.Run(ctx, garbagebin.WithInitBuckets("uploads", "exports"))
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })
	if err != nil {
		t.Fatal(err)
	}

	client := s3.New(s3.Options{
		BaseEndpoint: aws.String(ctr.Endpoint()),
		Region:       "us-east-1",
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(ctr.AccessKey(), ctr.SecretKey(), ""),
	})
	// ...
}
```

`Run` returns once `GET /health` returns 200. `Terminate` is safe to call on a failed `Run`.

## Options

| Option | Effect |
| --- | --- |
| `WithImage(image)` | Default `camconn/garbagebin:latest` (`DefaultImage`) |
| `WithBackend("memory" \| "fs")` | Storage backend; default `memory` |
| `WithInitBuckets(names...)` | Buckets created at startup |
| `WithCredentials(accessKey, secretKey)` | Default `test` / `test`; only checked with `S3_STRICT_AUTH=true` |
| `WithEnv(key, value)` | Any other server setting, e.g. `WithEnv("S3_STRICT_AUTH", "true")` |

`*Container` exposes `Endpoint()`, `AccessKey()`, `SecretKey()`, `Terminate(ctx)`, and the underlying
`testcontainers.Container` as `.Container`. Server settings: see the
[configuration table](https://github.com/camconn/garbagebin/blob/master/docs/design.md#container-and-configuration).

## License

AGPL-3.0-or-later
