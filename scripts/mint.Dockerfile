# Slim MinIO mint: upstream sources (build context), but only the suites whose toolchains are Go and Python.
# Upstream's Dockerfile installs Java, Gradle, .NET, Node, PHP and Ruby for every suite and takes far too long.
# SUITES and EXCLUDE come from scripts/mint.sh.
FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive LANG=C.UTF-8 MINT_ROOT_DIR=/mint \
    GOROOT=/usr/local/go GOPATH=/usr/local/gopath PATH=/usr/local/go/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl git jq openssl python3-pip python-is-python3 wget \
    && rm -rf /var/lib/apt/lists/*

COPY . /mint
WORKDIR /mint
RUN ./create-data-files.sh

ARG SUITES
ARG EXCLUDE
# Excluding a test deletes the line calling it from the suite's driver; the Go suites are then rebuilt.
# awscli is pinned to SigV4: its `s3 presign` otherwise signs with SigV2, which gb (like AWS for buckets
# created since 2020) does not accept.
RUN . ./source.sh \
    && wget -qO- "https://dl.google.com/go/go${GO_VERSION}.linux-amd64.tar.gz" | tar -C /usr/local -xzf - \
    && for s in $SUITES; do echo "building $s"; "build/$s/install.sh"; done \
    && sed -i '/^aws configure set default.region/a aws configure set default.s3.signature_version s3v4' \
         run/core/awscli/run.sh \
    && for e in $EXCLUDE; do \
         suite=${e%%:*} test=${e#*:}; \
         case $suite in \
           awscli|s3cmd) f=run/core/$suite/test.sh ;; aws-sdk-go-v2|minio-go) f=run/core/$suite/main.go ;; \
           minio-py) f=run/core/$suite/tests.py ;; versioning) f=build/versioning/main.go ;; \
         esac; \
         grep -qE "^\s*$test(\(|:| |\$)" "$f" || { echo "exclude $e: no call found"; exit 1; }; \
         sed -i -E "/^\s*$test(\(|:| |\$)/d" "$f"; \
       done \
    && (cd run/core/aws-sdk-go-v2 && CGO_ENABLED=0 go build -ldflags "-s -w") \
    && (cd run/core/minio-go && CGO_ENABLED=0 go build -ldflags "-s -w" -o minio-go main.go) \
    && (cd build/versioning && CGO_ENABLED=0 go build -ldflags "-s -w" -o /mint/run/core/versioning/tests) \
    && rm -rf /usr/local/go "$GOPATH" /root/.cache

ENTRYPOINT ["/mint/entrypoint.sh"]
