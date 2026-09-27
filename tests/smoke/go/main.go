// aws-sdk-go-v2 smoke test. Env: S3_ENDPOINT, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func check(cond bool, format string, args ...any) {
	if !cond {
		panic(fmt.Errorf(format, args...))
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "FAIL go: %v\n", r)
			os.Exit(1)
		}
	}()
	run(context.Background())
	fmt.Println("PASS go")
}

func run(ctx context.Context) {
	cfg := must(config.LoadDefaultConfig(ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			env("AWS_ACCESS_KEY_ID", "test"), env("AWS_SECRET_ACCESS_KEY", "test"), "")),
	))
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(env("S3_ENDPOINT", "http://localhost:9000"))
		o.UsePathStyle = true
	})

	suffix := make([]byte, 6)
	must(rand.Read(suffix))
	bucket := aws.String("gb-smoke-go-" + hex.EncodeToString(suffix))
	small := []byte("hello, garbage bin\n")
	big := make([]byte, 20<<20)
	must(rand.Read(big))

	must(client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucket}))

	must(client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: bucket, Key: aws.String("dir/a/small.txt"), Body: bytes.NewReader(small),
	}))

	// The uploader switches to multipart above its part size.
	must(manager.NewUploader(client).Upload(ctx, &s3.PutObjectInput{
		Bucket: bucket, Key: aws.String("dir/b/big.bin"), Body: bytes.NewReader(big),
	}))
	head := must(client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: bucket, Key: aws.String("dir/b/big.bin")}))
	check(strings.Contains(aws.ToString(head.ETag), "-"), "big object ETag is not multipart: %s", aws.ToString(head.ETag))
	check(aws.ToInt64(head.ContentLength) == int64(len(big)), "big object size %d", aws.ToInt64(head.ContentLength))
	got := must(client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("dir/b/big.bin")}))
	check(bytes.Equal(must(io.ReadAll(got.Body)), big), "big object body mismatch")

	list := must(client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: bucket, Prefix: aws.String("dir/"), Delimiter: aws.String("/"),
	}))
	var prefixes []string
	for _, p := range list.CommonPrefixes {
		prefixes = append(prefixes, aws.ToString(p.Prefix))
	}
	slices.Sort(prefixes)
	check(slices.Equal(prefixes, []string{"dir/a/", "dir/b/"}), "CommonPrefixes: %v", prefixes)
	check(len(list.Contents) == 0, "unexpected Contents: %d", len(list.Contents))

	rng := must(client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: bucket, Key: aws.String("dir/b/big.bin"), Range: aws.String("bytes=100-199"),
	}))
	check(aws.ToString(rng.ContentRange) == fmt.Sprintf("bytes 100-199/%d", len(big)), "Content-Range: %s", aws.ToString(rng.ContentRange))
	check(bytes.Equal(must(io.ReadAll(rng.Body)), big[100:200]), "range get bytes mismatch")

	must(client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: bucket, Key: aws.String("copy/small.txt"), CopySource: aws.String(*bucket + "/dir/a/small.txt"),
	}))
	cp := must(client.GetObject(ctx, &s3.GetObjectInput{Bucket: bucket, Key: aws.String("copy/small.txt")}))
	check(bytes.Equal(must(io.ReadAll(cp.Body)), small), "copied object mismatch")

	req := must(s3.NewPresignClient(client).PresignGetObject(ctx,
		&s3.GetObjectInput{Bucket: bucket, Key: aws.String("copy/small.txt")},
		s3.WithPresignExpires(5*time.Minute)))
	resp := must(http.Get(req.URL))
	body := must(io.ReadAll(resp.Body))
	resp.Body.Close()
	check(resp.StatusCode == 200, "presigned get status %d: %s", resp.StatusCode, body)
	check(bytes.Equal(body, small), "presigned get mismatch")

	var ids []types.ObjectIdentifier
	for _, k := range []string{"dir/a/small.txt", "dir/b/big.bin", "copy/small.txt"} {
		ids = append(ids, types.ObjectIdentifier{Key: aws.String(k)})
	}
	del := must(client.DeleteObjects(ctx, &s3.DeleteObjectsInput{Bucket: bucket, Delete: &types.Delete{Objects: ids}}))
	check(len(del.Errors) == 0 && len(del.Deleted) == 3, "DeleteObjects: deleted %d, errors %d", len(del.Deleted), len(del.Errors))
	left := must(client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{Bucket: bucket}))
	check(aws.ToInt32(left.KeyCount) == 0, "bucket not empty after batch delete")

	must(client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucket}))
	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucket})
	var nf *types.NotFound
	check(errors.As(err, &nf), "HeadBucket after delete: %v", err)
}
