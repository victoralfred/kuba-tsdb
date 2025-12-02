# Multi-stage build for Gorilla TSDB
# Optimized for production deployment with minimal image size
# Runtime: Debian 13 (Trixie) Slim

# ============================================================
# Stage 1: BUILDER
# ============================================================
FROM rust:1.85-slim-bookworm AS builder

# Define app name for clarity and re-use
ARG APP_NAME=tsdb-server
ENV CARGO_HOME=/usr/local/cargo \
    CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# Install build dependencies with no-install-recommends for smaller layer
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Setup Workdir and Copy Manifests
WORKDIR /app
COPY Cargo.toml Cargo.lock ./

# Dependency Caching Layer
# Create dummy files to force cargo to only compile dependencies
# Also create placeholder bench files referenced in Cargo.toml
RUN mkdir -p src benches src/bin/server \
    && echo "fn main() {}" > src/bin/server/main.rs \
    && echo "pub fn dummy() {}" > src/lib.rs \
    && echo "fn main() {}" > benches/compression.rs \
    && echo "fn main() {}" > benches/ingestion.rs \
    && echo "fn main() {}" > benches/mmap_performance.rs

# Build dependencies (fills target/release with cached dependency artifacts)
RUN cargo build --release --lib

# Clean up temporary build artifacts
RUN rm -f target/release/${APP_NAME} || true \
    && rm -rf src benches target/release/.fingerprint/gorilla* target/release/deps/gorilla*

# Final Build Layer - Copy actual source code
COPY src/ src/
COPY benches/ benches/

# Final release build of the application executable
RUN cargo build --release --bin ${APP_NAME} \
    && strip --strip-all target/release/${APP_NAME}

# ============================================================
# Stage 2: RUNTIME
# Base: Debian 13 (Trixie) Slim for latest security patches
# ============================================================
FROM debian:13.2-slim
# Define app name for clarity and re-use
ENV APP_NAME=tsdb-server
# Define app name for clarity and re-use
ARG APP_NAME=tsdb-server
ARG APP_USER=tsdb
ARG APP_UID=1000

# OCI image labels
LABEL org.opencontainers.image.title="Gorilla TSDB" \
      org.opencontainers.image.description="High-performance time-series database with Gorilla compression" \
      org.opencontainers.image.licenses="MIT"
# 1. Install Runtime Dependencies
RUN apt update && apt install -y \
    ca-certificates \
    libssl3 \
    bash \
    procps \
    # Clean up the apt cache and lists
    && rm -rf /var/lib/apt/lists/*

# 2. Setup User and Directories
# Create non-root user with a fixed UID (good for volume permissions)
RUN useradd -m -u 1000 -s /bin/bash tsdb

# Create data directory and set ownership
RUN mkdir -p /data/gorilla-tsdb \
    && chown -R ${APP_USER}:${APP_USER} /data \
    && chmod 750 /data/gorilla-tsdb

# Copy Final Binary from builder stage
COPY --from=builder --chown=${APP_USER}:${APP_USER} \
    /app/target/release/${APP_NAME} /usr/local/bin/

# Configure Runtime Environment
WORKDIR /home/${APP_USER}
USER ${APP_USER}

# Environment variables
ENV RUST_LOG=info \
    RUST_BACKTRACE=0 \
    TSDB_DATA_DIR=/data/gorilla-tsdb \
    TSDB_HOST=0.0.0.0 \
    TSDB_PORT=8080

# Document exposed port
EXPOSE 8080

# Define volume for persistent data
VOLUME ["/data/gorilla-tsdb"]

# Health Check - TCP connection test without external tools
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD timeout 3 sh -c '</dev/tcp/localhost/8080' || exit 1

# Use tini as init for proper signal handling and zombie reaping
ENTRYPOINT ["/usr/bin/tini", "--"]

# Run the server
CMD ["tsdb-server"]
