# Multi-stage build for Gorilla TSDB
# Optimized for production deployment with minimal image size

# Stage 1: Builder
FROM rust:slim-buster AS BUILDER

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy dependency manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy main to cache dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies (cached layer)
RUN cargo build --release &&  rm -rf src target/release/.fingerprint/gorilla-tsdb-*

# Copy actual source code
COPY . .

# Build the application
RUN cargo build --release --bin tsdb-server

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash tsdb

# Create data directory
RUN mkdir -p /data/gorilla-tsdb && \
    chown -R tsdb:tsdb /data

# Copy binary from builder
COPY --from=builder /app/target/release/tsdb-server /usr/local/bin/

# Switch to non-root user
USER tsdb
WORKDIR /home/tsdb

# Expose default port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD timeout 2s bash -c '</dev/tcp/localhost/8080' || exit 1

# Set default environment variables
ENV RUST_LOG=info
ENV TSDB_DATA_DIR=/data/gorilla-tsdb
ENV TSDB_PORT=8080

# Run the server
CMD ["tsdb-server"]
