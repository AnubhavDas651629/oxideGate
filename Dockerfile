# ---- build ----
FROM rust:1-slim AS builder

WORKDIR /app

# Dependencies first, so edits to src/ don't re-download the world.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

COPY src ./src
# Cargo skips rebuilds on unchanged mtimes; force the real binary to build.
RUN touch src/main.rs && cargo build --release

# ---- run ----
FROM debian:stable-slim

# rustls needs the trust store; nothing else is required.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/oxidegate /usr/local/bin/oxidegate

# Listen on all interfaces: loopback is unreachable from outside the container.
ENV OXIDEGATE_BIND=0.0.0.0:8000
ENV RUST_LOG=info

EXPOSE 8000

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8000/health || exit 1

CMD ["oxidegate"]