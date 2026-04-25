FROM rust:1.95 AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    libclang-dev \
    clang \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY build.rs ./
COPY proto/ proto/
RUN mkdir src && echo "fn main() {}" > src/main.rs && echo "" > src/lib.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build actual binary
COPY src/ src/
RUN touch src/main.rs src/lib.rs
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY --from=builder /app/target/release/rocksqueue /usr/local/bin/rocksqueue

RUN mkdir -p /data/rocksqueue /wal/rocksqueue-wal /data/checkpoints

EXPOSE 50051 9090

CMD ["/usr/local/bin/rocksqueue"]
