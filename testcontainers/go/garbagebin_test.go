package garbagebin_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	garbagebin "github.com/camconn/garbagebin/testcontainers/go"
)

func TestPutGet(t *testing.T) {
	ctx := context.Background()
	opts := []garbagebin.Option{
		garbagebin.WithInitBuckets("tc-bucket"),
		garbagebin.WithCredentials("gbaccess", "gbsecret123"),
		garbagebin.WithEnv("S3_STRICT_AUTH", "true"), // proves the credentials reach the server
	}
	img := os.Getenv("GB_TEST_IMAGE")
	if img == "" {
		img = "garbagebin:latest"
	}
	opts = append(opts, garbagebin.WithImage(img))
	ctr, err := garbagebin.Run(ctx, opts...)
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })
	if err != nil {
		t.Fatal(err)
	}

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(ctr.Endpoint()),
		UsePathStyle: true,
		Credentials:  credentials.NewStaticCredentialsProvider(ctr.AccessKey(), ctr.SecretKey(), ""),
	})
	if _, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("tc-bucket"), Key: aws.String("hello.txt"), Body: strings.NewReader("hello"),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("tc-bucket"), Key: aws.String("hello.txt")})
	if err != nil {
		t.Fatal(err)
	}
	defer out.Body.Close()
	body, _ := io.ReadAll(out.Body)
	if string(body) != "hello" {
		t.Fatalf("got %q", body)
	}
}
