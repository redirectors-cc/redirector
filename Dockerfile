# Build stage
FROM rust:1.88-alpine AS builder

ARG GIT_COMMIT_SHORT=unknown

RUN apk add --no-cache musl-dev pkgconfig openssl-dev

WORKDIR /app

# Copy manifests and build script
COPY Cargo.toml Cargo.lock build.rs ./

# Create dummy sources to build dependencies
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/main.rs && \
    echo "pub fn lib() {}" > src/lib.rs && \
    echo "fn main() {}" > src/bin/event_consumer.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release --bin redirector --bin event_consumer && \
    rm -rf src

# Copy source code
COPY src ./src
COPY templates ./templates
COPY static ./static
COPY migrations ./migrations

# Build both binaries with git commit hash
RUN touch src/main.rs src/lib.rs src/bin/event_consumer.rs && \
    GIT_COMMIT_SHORT=${GIT_COMMIT_SHORT} cargo build --release --bin redirector --bin event_consumer

# Compress with UPX
RUN apk add --no-cache upx && \
    upx --best --lzma /app/target/release/redirector && \
    upx --best --lzma /app/target/release/event_consumer

# Runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

# Copy binaries and static assets from builder
COPY --from=builder /app/target/release/redirector /app/redirector
COPY --from=builder /app/target/release/event_consumer /app/event_consumer
COPY --from=builder /app/static /app/static

# Copy config example (actual config should be mounted)
COPY config.yaml.example /app/config.yaml.example

# Create non-root user
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/healthz || exit 1

# Run
CMD ["./redirector"]
