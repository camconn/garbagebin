// Package garbagebin is a Testcontainers module for the Garbage Bin S3 server.
package garbagebin

import (
	"context"
	"fmt"
	"strings"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	DefaultImage = "garbagebin:latest"
	port         = "9000/tcp"
)

// Container is a running Garbage Bin instance. The underlying container is
// not embedded because its Endpoint method would clash with ours.
type Container struct {
	Container testcontainers.Container
	endpoint  string
	accessKey string
	secretKey string
}

type config struct {
	image, backend, accessKey, secretKey string
	buckets                              []string
	env                                  map[string]string
}

type Option func(*config)

func WithImage(image string) Option     { return func(c *config) { c.image = image } }
func WithBackend(backend string) Option { return func(c *config) { c.backend = backend } }
func WithInitBuckets(b ...string) Option {
	return func(c *config) { c.buckets = append(c.buckets, b...) }
}

// WithEnv sets any other server env var, e.g. S3_STRICT_AUTH.
func WithEnv(key, value string) Option {
	return func(c *config) {
		if c.env == nil {
			c.env = map[string]string{}
		}
		c.env[key] = value
	}
}
func WithCredentials(accessKey, secretKey string) Option {
	return func(c *config) { c.accessKey, c.secretKey = accessKey, secretKey }
}

// Run starts a container and waits for GET /health to return 200.
func Run(ctx context.Context, opts ...Option) (*Container, error) {
	cfg := config{image: DefaultImage, backend: "memory", accessKey: "test", secretKey: "test"}
	for _, o := range opts {
		o(&cfg)
	}
	env := map[string]string{
		"S3_BACKEND":    cfg.backend,
		"S3_ACCESS_KEY": cfg.accessKey,
		"S3_SECRET_KEY": cfg.secretKey,
	}
	if len(cfg.buckets) > 0 {
		env["S3_INIT_BUCKETS"] = strings.Join(cfg.buckets, ",")
	}
	for k, v := range cfg.env {
		env[k] = v
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        cfg.image,
			ExposedPorts: []string{port},
			Env:          env,
			WaitingFor:   wait.ForHTTP("/health").WithPort(port), // default matcher is 200
		},
		Started: true,
	})
	c := &Container{Container: ctr, accessKey: cfg.accessKey, secretKey: cfg.secretKey}
	if err != nil {
		return c, err
	}
	host, err := ctr.Host(ctx)
	if err != nil {
		return c, err
	}
	p, err := ctr.MappedPort(ctx, port)
	if err != nil {
		return c, err
	}
	c.endpoint = fmt.Sprintf("http://%s:%s", host, p.Port())
	return c, nil
}

// Terminate stops and removes the container.
func (c *Container) Terminate(ctx context.Context) error {
	if c == nil || c.Container == nil {
		return nil
	}
	return c.Container.Terminate(ctx)
}

func (c *Container) Endpoint() string  { return c.endpoint }
func (c *Container) AccessKey() string { return c.accessKey }
func (c *Container) SecretKey() string { return c.secretKey }
