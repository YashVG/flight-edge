# FlightEdge

FlightEdge is a Go edge-to-core flight-state ingestion system. Collectors poll and filter live OpenSky state vectors, batch them into an ordered gRPC stream, and send them to a core process that maintains a bounded in-memory query model.

The project is deliberately scoped around one systems question: **how does a core safely accept at-least-once, streaming updates from unreliable edge collectors without silently losing or double-applying batches?**

## What it demonstrates

- Go concurrency: concurrent reads, indexed state, bounded retention, and race-tested mutation paths
- gRPC: a versioned bidirectional streaming contract with typed acknowledgements and client-side health checks
- Delivery semantics: session-scoped sequences, retry-safe duplicate acknowledgement, atomic in-flight sequence reservation, maximum batch size, and bounded collector queues
- Load protection: one in-flight batch per collector session, a bounded core admission limit, explicit overload acknowledgements, and jittered exponential retry only for retryable failures
- Docker: independently deployable core and collector images, health checks, non-root runtime containers, internal gRPC networking, and resource limits
- Operations: Prometheus metrics for core ingestion and collector queue, retry, delivery, and drop behaviour

## Architecture

```text
OpenSky API
   │
   ▼
collector containers ── ordered gRPC streams ──► core container ──► HTTP query API
  bounded queue       ◄── acknowledgement ────┘        │
       │                                                └──► Prometheus / Grafana
       └── queue, retry, drop metrics
```

Each collector has a stable `source_id`, a random `session_id` generated on startup, and monotonically increasing sequences within that session. The core accepts only the next sequence for a session. It atomically reserves that sequence while ingesting; a retry after a lost acknowledgement receives a typed duplicate acknowledgement instead of applying state twice. A collector restart starts a new session at sequence one, rather than silently discarding new batches as duplicates of an old process.

The gRPC ingress acts as a small data-plane gateway: it admits a bounded number of concurrent batches to the single core writer and returns `OVERLOADED` with a retry hint when capacity is exhausted. The collector uses capped exponential backoff with full jitter for `OVERLOADED`, `IN_FLIGHT`, `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, and deadline failures. Invalid or out-of-order batches are never retried blindly.

Current guarantee: delivery is **at-least-once while a collector process is running**, with idempotent core application. The collector queue is in memory and bounded; when it fills or retries are exhausted, the batch is intentionally dropped and counted. This is not durable offline delivery.

## Run locally

Requirements: Go 1.22+.

```bash
# Terminal 1: core HTTP API plus gRPC ingestion endpoint
make run-core

# Terminal 2: an edge collector
FLIGHTEDGE_CORE_GRPC_ADDR=localhost:9091 make run-collector
```

Core endpoints:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/api/v1/flights
curl http://localhost:8080/api/v1/stats
curl http://localhost:8080/metrics
```

The collector exposes `http://localhost:9092/health` and `/metrics` by default. Its delivery settings are:

```bash
OUTBOUND_QUEUE_CAPACITY=64  # batches waiting for core acknowledgement
DELIVERY_TIMEOUT=10s        # includes retry attempts for one batch
BATCH_SIZE=100              # must not exceed the core maximum of 1000

# Core admission controls
GRPC_MAX_CONCURRENT_STREAMS=128
GRPC_MAX_IN_FLIGHT_BATCHES=32
```

## Run the Docker topology

The default stack starts a core and Prometheus. It disables core-side polling so the ingestion path remains unambiguous.

```bash
docker compose -f docker/docker-compose.yml up --build
```

Start three edge collectors against that core:

```bash
docker compose -f docker/docker-compose.yml --profile collectors up --build --scale collector=3
```

The core’s gRPC port is bound only to localhost on the host and remains reachable by collectors on the internal Compose network. The current transport is plaintext for local/private-network development; do not expose it publicly without adding mTLS.

## Validation

```bash
make test              # race-tested unit and integration suite
make test-performance  # scoped memory and query performance checks
make proto             # Buf lint plus generated gRPC stubs
make docker-build      # core image
docker build --target collector -t flightedge-collector:latest -f docker/Dockerfile .
```

The gRPC suite covers malformed and oversized batches, unavailable cores, duplicate retry handling, session restart at sequence one, concurrent duplicate reservation, overload shedding with retry hints, and fan-in from 16 collectors sending 25 ordered batches each.

## Boundaries

- The core is one in-memory writer; it is not a replicated datastore. Do not scale core replicas until sequence ownership and ontology state are partitioned or made durable.
- OpenSky state vectors provide aircraft position, not trusted schedules, delays, or arrival predictions.
- No trained model, database, Kubernetes control plane, durable queue, service mesh, or mTLS deployment is claimed or included. The Compose transport is plaintext for local/private-network development only.

## Repository layout

| Path | Responsibility |
|---|---|
| `cmd/flightedge` | Core: HTTP query API, state ingestion, gRPC server |
| `cmd/flightedge-collector` | Edge collector: poll, bounded queue, retry, gRPC stream, metrics |
| `proto/flightedge/v1` | Versioned streaming contract |
| `internal/rpc/ingest` | Validation, sequencing, acknowledgement, idempotency |
| `internal/ontology`, `internal/query` | Concurrent in-memory state and indexed queries |
| `docker` | Core/collector images, Compose topology, Prometheus, Grafana |

## License

[MIT](LICENSE)
