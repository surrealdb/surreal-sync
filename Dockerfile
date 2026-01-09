# Build stage
FROM rust:1.86-bookworm AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    libssl-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src

# Build the binary in release mode
RUN cargo build --release --bin surreal-sync

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    # For environment verification
    procps \
    # For database connectivity testing
    netcat-openbsd \
    # For healthchecks
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /app/target/release/surreal-sync /usr/local/bin/

# Create directories for config and data
RUN mkdir -p /config /data

# Set environment variables
ENV RUST_LOG=info

ENTRYPOINT ["surreal-sync"]
CMD ["--help"]
