# Build a fully static musl binary, then ship it on scratch.
FROM rust:1.96-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY . .
RUN cargo build --release --locked

FROM scratch
COPY --from=build /src/target/release/gb /gb
VOLUME /data
EXPOSE 9000
ENTRYPOINT ["/gb"]
