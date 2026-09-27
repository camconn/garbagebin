# Build a fully static musl binary, then ship it on scratch.
# Targets: image < 20 MB, cold start < 500 ms, idle RSS < 30 MB.
FROM rust:1.96-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/gb /gb
VOLUME /data
EXPOSE 9000
# TODO(TASKS: Container): HEALTHCHECK against /health once the binary serves it.
ENTRYPOINT ["/gb"]
