# FlightEdge Edge Deployment Guide

This document describes FlightEdge's edge deployment controls and the validation needed before using them on constrained hardware. The repository includes a 512MB memory gate and a scoped query-latency gate; these are test scenarios, not guarantees for every workload or device.

## Overview

The `internal/edge` package provides a comprehensive set of tools for deploying FlightEdge on edge devices with limited resources:

- **Configuration Presets**: Normal, Reduced, and Aggressive memory modes
- **Memory Monitoring**: Real-time tracking with pressure alerts
- **Data Expiration**: Automatic cleanup of old data based on retention policies
- **Node Limits**: Hard caps on graph size with oldest-first eviction
- **Compression utilities**: Tested property/byte compression helpers; not wired into the live graph path yet
- **Startup utilities**: Phase tracking and lazy-index helpers; not wired into application startup yet

## Configuration

### Memory Modes

| Mode | Memory | GC% | Use Case |
|------|--------|-----|----------|
| Normal | 512MB | 100 | Standard edge deployment |
| Reduced | 384MB | 50 | Resource-constrained edge |
| Aggressive | 256MB | 20 | Minimal memory environments |

### Environment Variables

```bash
# Memory mode (normal, reduced, aggressive)
MEMORY_MODE=normal

# Memory limits
MEMORY_LIMIT_MB=512
SOFT_LIMIT_MB=400  # Trigger warnings at this threshold

# Garbage collection
GC_PERCENT=100  # Lower = more frequent GC, less memory

# Data retention
DATA_RETENTION_HOURS=6  # 0 = unlimited
MAX_NODES=10000         # 0 = unlimited

# Features
ENABLE_COMPRESSION=false
ENABLE_DEGRADATION=true
```

## Trade-offs

### GC Percent (Lower Values)

| Pros | Cons |
|------|------|
| + Lower memory usage | - Higher CPU usage |
| + More predictable memory | - More GC pauses |
| | - Slightly higher P99 latency |

**Recommendation**: Start with 100%, reduce to 50% if memory is tight, 20% only in emergencies.

### Small Buffers

| Pros | Cons |
|------|------|
| + Faster startup | - More allocations as data grows |
| + Lower initial memory | - Slightly higher memory churn |

**Recommendation**: Enable in Reduced/Aggressive modes.

### Data Retention (Limited)

| Pros | Cons |
|------|------|
| + Bounded memory usage | - Historical queries limited |
| + Predictable growth | - Background cleanup adds CPU |

**Recommendation**: Set based on business requirements. 6 hours is good for real-time tracking.

### Max Nodes (Capped)

| Pros | Cons |
|------|------|
| + Guaranteed memory ceiling | - Data loss when limit reached |
| + Prevents OOM | - Requires degradation strategy |

**Recommendation**: Calculate based on: `max_nodes * ~200 bytes/node < memory_limit * 0.5`

### Compression

| Pros | Cons |
|------|------|
| + Can reduce repetitive payload size | - CPU overhead |
| + More data in memory | - Slightly higher query latency |

**Current boundary**: `ENABLE_COMPRESSION` is parsed by the configuration package, but the live ontology does not invoke compression. Treat it as roadmap configuration until that integration has its own end-to-end tests.

### Lazy Indexing

| Pros | Cons |
|------|------|
| + Faster cold start (<5s) | - First queries slower |
| + Lower startup memory | - Temporary latency spike |

**Current boundary**: `LAZY_INDEXING` is parsed and the helper is unit-tested, but application startup does not currently use it.

### Graceful Degradation

| Pros | Cons |
|------|------|
| + System stays responsive | - May drop data |
| + Prevents crashes | - Query completeness not guaranteed |

**Recommendation**: Always enable in production edge deployments.

## Graceful Degradation Actions

When memory pressure exceeds the soft limit:

1. **Drop Oldest**: Remove oldest flight nodes
2. **Reject New**: Stop accepting new data (maintains existing data)
3. **Compact**: Run GC and compact data structures

## Startup Optimization

The startup package can measure a target of <5s cold start. Validate the packaged application on the target hardware before adopting that target as an SLO.

The startup optimizer runs phases in parallel where possible:

1. **Config**: Load configuration (~1ms)
2. **Memory**: Initialize memory monitoring (~1ms)
3. **Seed**: Seed static data (airports) (~10ms)
4. **Indexes**: Build query indexes (lazy or immediate)
5. **Services**: Start HTTP server, ingestion (~100ms)
6. **Ready**: Mark as ready

### Tips for Fast Startup

- Use lazy indexing (`LAZY_INDEXING=true`)
- Reduce initial buffer sizes (`MEMORY_MODE=aggressive`)
- Defer non-critical initialization to background tasks

## Memory Monitoring

The memory monitor tracks:

- **Heap Allocation**: Current heap memory
- **Usage Ratio**: Percentage of soft limit used
- **Memory State**: Normal, Warning, Critical, Emergency

### States and Actions

| State | Threshold | Actions |
|-------|-----------|---------|
| Normal | <80% soft limit | None |
| Warning | ≥80% soft limit | Log warning |
| Critical | ≥soft limit | Evict old data |
| Emergency | ≥95% hard limit | Force GC, reject new data |

## API Endpoints

### Health Check

```bash
curl http://localhost:8080/health
```

### Stats (includes edge metrics)

```bash
curl http://localhost:8080/api/v1/stats
```

Response includes:

```json
{
  "edge": {
    "memory_mode": "normal",
    "memory_limit_mb": 512,
    "memory_state": "normal",
    "heap_mb": 45.2,
    "usage_ratio": 0.11,
    "tracked_nodes": 1234,
    "expired_total": 56
  }
}
```

## Docker Configuration

The Docker configuration includes resource limits matching edge constraints:

```yaml
services:
  flightedge:
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: '0.5'
```

## Monitoring

Prometheus metrics are exposed at `/metrics`:

- `go_memstats_heap_alloc_bytes`: Current heap allocation
- `flightedge_ontology_nodes`: Current ontology node count
- `flightedge_ingestion_errors_total`: OpenSky ingestion failures
- `flightedge_query_latency_seconds`: Query latency histogram

The Compose stack provisions Prometheus rules from `docker/alerts.yml` and a Grafana dashboard covering query latency/throughput, ontology size, ingestion, memory, and goroutines. The richer edge-state fields are currently available from `/api/v1/stats`, not as Prometheus series.

## Best Practices

1. **Start with Normal mode** and adjust based on observed memory usage
2. **Set retention hours** based on how far back queries need to look
3. **Enable degradation** to prevent OOM crashes
4. **Monitor `/api/v1/stats`** to understand memory pressure
5. **Use `MEMORY_MODE=aggressive`** only for truly constrained environments
6. **Test with realistic load** before deploying to edge

## Example Configurations

### IoT Gateway (256MB limit)

```bash
MEMORY_MODE=aggressive
MEMORY_LIMIT_MB=256
DATA_RETENTION_HOURS=1
MAX_NODES=2000
ENABLE_DEGRADATION=true
```

### Edge Server (512MB limit)

```bash
MEMORY_MODE=reduced
MEMORY_LIMIT_MB=512
DATA_RETENTION_HOURS=6
MAX_NODES=10000
ENABLE_DEGRADATION=true
```

### Development/Testing

```bash
MEMORY_MODE=normal
MEMORY_LIMIT_MB=1024
DATA_RETENTION_HOURS=24
ENABLE_DEGRADATION=false
```
