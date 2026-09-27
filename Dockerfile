# Static musl binary on scratch. The build stage runs on the build host and
# cross-compiles for TARGETARCH so multi-arch builds need no emulation.
FROM --platform=$BUILDPLATFORM rust:1.96-alpine AS build
ARG TARGETARCH
RUN apk add --no-cache musl-dev
# Cross targets link with rust-lld; native builds keep the default linker for proc macros.
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-musl ;; \
        arm64) echo aarch64-unknown-linux-musl ;; \
        *) echo "unsupported arch: $TARGETARCH" >&2; exit 1 ;; \
    esac > /target \
    && rustup target add "$(cat /target)" \
    && if [ "$(cat /target)" != "$(rustc -vV | sed -n 's/^host: //p')" ]; then \
        printf '[target.%s]\nlinker = "rust-lld"\n' "$(cat /target)" >> "$CARGO_HOME/config.toml"; \
    fi
WORKDIR /src

# Dependencies only, cached until Cargo.toml or Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked --target "$(cat /target)" \
    && rm -rf src

COPY src src
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --target "$(cat /target)" \
    && cp "target/$(cat /target)/release/gb" /gb

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /gb /gb
VOLUME /data
EXPOSE 9000
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --start-interval=1s --retries=3 \
    CMD ["/gb", "healthcheck"]
ENTRYPOINT ["/gb"]
