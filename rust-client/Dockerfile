# Build and pack the load client in this image. The runtime stage has no JDK.
FROM rust:1.85-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
RUN cargo test --locked && cargo build --release --locked

FROM debian:bookworm-slim
COPY --from=build /src/target/release/ws-load-client /usr/local/bin/ws-load-client
EXPOSE 9100
USER nobody
ENTRYPOINT ["/usr/local/bin/ws-load-client"]
