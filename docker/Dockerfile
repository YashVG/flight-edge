# ==============================================================================
# FlightEdge - Multi-stage Docker Build
# Optimized for edge deployment with minimal runtime footprint
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set build environment
ENV CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

WORKDIR /src

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download && go mod verify

# Copy source code
COPY . .

# Build with optimizations
# -ldflags: strip debug info, reduce binary size
# -trimpath: remove file system paths from binary
RUN go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo 'dev')" \
    -trimpath \
    -o /bin/flightedge \
    ./cmd/flightedge/

# Verify binary
RUN /bin/flightedge --help 2>/dev/null || true

# ------------------------------------------------------------------------------
# Stage 2: Runtime (minimal)
# ------------------------------------------------------------------------------
FROM alpine:3.19 AS runtime

# Labels
LABEL org.opencontainers.image.title="FlightEdge" \
      org.opencontainers.image.description="Real-time flight ontology engine for edge computing" \
      org.opencontainers.image.vendor="FlightEdge" \
      org.opencontainers.image.source="https://github.com/yash/flightedge"

# Install runtime dependencies
RUN apk add --no-cache \
    ca-certificates \
    tzdata \
    curl \
    && rm -rf /var/cache/apk/*

# Create non-root user for security
RUN addgroup -g 1000 flightedge && \
    adduser -u 1000 -G flightedge -s /bin/sh -D flightedge

# Create directories
RUN mkdir -p /app/config /app/data && \
    chown -R flightedge:flightedge /app

# Copy binary from builder
COPY --from=builder /bin/flightedge /usr/local/bin/flightedge

# Copy timezone data
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Set working directory
WORKDIR /app

# Switch to non-root user
USER flightedge

# Environment variables with defaults
ENV HTTP_ADDR=0.0.0.0 \
    HTTP_PORT=8080 \
    ENABLE_INGESTION=true \
    POLL_INTERVAL=10s \
    GOMAXPROCS=1 \
    GOGC=50 \
    GOMEMLIMIT=450MiB

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Entry point
ENTRYPOINT ["flightedge"]

# ------------------------------------------------------------------------------
# Stage 3: Debug (optional, for troubleshooting)
# ------------------------------------------------------------------------------
FROM runtime AS debug

USER root

# Install debug tools
RUN apk add --no-cache \
    busybox-extras \
    net-tools \
    procps \
    strace

USER flightedge

# ------------------------------------------------------------------------------
# Default target
# ------------------------------------------------------------------------------
FROM runtime
