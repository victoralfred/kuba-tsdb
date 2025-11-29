# Multi-stage build for Gorilla TSDB
# Optimized for production deployment with minimal image size

# ============================================================
# Stage 1: BUILDER
# ============================================================
FROM  rust:buster AS builder

# Define app name for clarity and re-use
ENV APP_NAME=tsdb-server
ENV CARGO_HOME=/usr/local/cargo

# Overwrite the sources list to point to the archive servers
RUN echo "deb http://archive.debian.org/debian buster main" > /etc/apt/sources.list && \
    echo "deb http://archive.debian.org/debian-security buster/updates main" >> /etc/apt/sources.list && \
    apt update && apt install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

# 2. Setup Workdir and Copy Manifests
WORKDIR /app
COPY Cargo.toml Cargo.lock ./

# 3. Dependency Caching Layer
# Create dummy files to force cargo to only compile dependencies
RUN mkdir src && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn dummy() {}" > src/lib.rs

# Build dependencies (This fills the target/debug or target/release folder with cached dependency artifacts)
RUN cargo build --release

# Clean up temporary build artifacts (this must be a separate layer or the shell fails silently)
RUN rm -f target/release/${APP_NAME} || true \
    && rm -rf src target/release/.fingerprint target/release/deps/*.d

# 4. Final Build Layer
# Copy actual source code, overwriting dummy files
COPY . .

# Final release build of the application executable
RUN cargo build --release --bin ${APP_NAME}

# ============================================================
# Stage 2: RUNTIME
# ============================================================
FROM debian:bookworm-slim
# Define app name for clarity and re-use
ENV APP_NAME=tsdb-server
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
RUN mkdir -p /data/gorilla-tsdb && \
    chown -R tsdb:tsdb /data

# 3. Copy Final Binary
# Copy only the compiled, stripped binary from the builder stage
COPY --from=builder /app/target/release/${APP_NAME} /usr/local/bin/

# 4. Configure Runtime Environment
WORKDIR /home/tsdb
USER tsdb

# Environment variables
ENV RUST_LOG=info
ENV TSDB_DATA_DIR=/data/gorilla-tsdb
ENV TSDB_PORT=8080

# Metadata
EXPOSE 8080

# Robust Health Check
# Uses 'sh -c' instead of bash and curl/wget is avoided for minimal image size
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD timeout 2s sh -c '</dev/tcp/localhost/8080' || exit 1

# Run the server
CMD ["tsdb-server"]