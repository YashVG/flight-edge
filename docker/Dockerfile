# ==============================================================================
# FlightEdge - Multi-stage Docker Build
# Optimized for edge deployment with minimal runtime footprint
# ==============================================================================

# ------------------------------------------------------------------------------
# Stage 1: Builder
# ------------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

ARG VERSION=dev

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
    -ldflags="-s -w -X main.version=${VERSION}" \
    -trimpath \
    -o /bin/flightedge \
    ./cmd/flightedge/

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

# Copy the dashboard served by the application.
COPY --from=builder --chown=flightedge:flightedge /src/web /app/web

# Copy timezone data
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo

# Set working directory
WORKDIR /app

# Switch to non-root user
USER flightedge

# Environment variables with defaults
ENV HTTP_ADDR=0.0.0.0 \
    HTTP_PORT=8080 \
    GRPC_ADDR=0.0.0.0 \
    GRPC_PORT=9091 \
    ENABLE_INGESTION=true \
    POLL_INTERVAL=10s \
    GOMAXPROCS=1 \
    GOGC=50 \
    GOMEMLIMIT=450MiB

# Expose port
EXPOSE 8080 9091

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
