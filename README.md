# FlightEdge

FlightEdge is a Go and gRPC ingestion gateway. It moves flight-state batches from unreliable edge collectors into one bounded in-memory core.

**Problem:** collectors reconnect, retry after lost acknowledgements, and can burst faster than the core can process data.

**Current result:** FlightEdge preserves order within a collector session, acknowledges duplicates safely, and tells collectors to back off when the core is full.

## See it work

Start a core without its own OpenSky poller:

```bash
make run-core
```

In another terminal, run a collector. It uses `OPENSKY_CLIENT_ID` and `OPENSKY_CLIENT_SECRET` when available, then streams batches to the core:

```bash
FLIGHTEDGE_CORE_GRPC_ADDR=localhost:9091 make run-collector
```

Query the state and metrics:

```bash
curl http://localhost:8080/api/v1/stats
curl http://localhost:8080/metrics
```

## How it works

```text
OpenSky API → collector → bidirectional gRPC stream → core → HTTP query API
                 queue        acknowledgements          metrics
```

Each collector creates a `session_id` on startup and numbers batches within that session. The core accepts only the next sequence. A lost acknowledgement can be retried without applying the batch twice.

| If this happens | FlightEdge does this |
| --- | --- |
| An acknowledgement is lost | Returns `DUPLICATE`; the collector advances safely |
| A collector restarts | Uses a new session and starts at sequence `1` |
| The core is busy | Returns `OVERLOADED` with a retry hint |
| A request is malformed | Returns `INVALID`; the collector does not retry it |
| The delivery queue fills | Drops the batch deliberately and records a metric |

## Why gRPC here

- Protobuf defines one versioned edge-to-core contract.
- A bidirectional stream provides an explicit acknowledgement for each batch.
- Typed acknowledgements drive retry policy; strings do not.
- Client health checks, deadlines, admission limits, and Prometheus metrics make failure behaviour visible.

## Run with Docker

Run the core and Prometheus:

```bash
docker compose -f docker/docker-compose.yml up --build
```

Start three independent collectors:

```bash
docker compose -f docker/docker-compose.yml --profile collectors up --build --scale collector=3
```

## Verify

```bash
make test              # race-tested unit and integration suite
make proto             # Buf lint and generated gRPC stubs
make docker-build      # core image
```

The gRPC tests cover duplicate retry, collector restart, overload shedding, malformed batches, and concurrent fan-in.

## Scope

Current: one in-memory core writer, bounded in-memory collector queues, local/private-network plaintext gRPC.

Not included: a replicated datastore, durable queue, Kubernetes control plane, service mesh, or mTLS deployment. Do not scale core replicas until state ownership is partitioned or durable.

## Where to look

| Path | Purpose |
| --- | --- |
| `cmd/flightedge` | Core HTTP API and gRPC server |
| `cmd/flightedge-collector` | Edge poller, bounded queue, retry logic |
| `proto/flightedge/v1` | Versioned streaming contract |
| `internal/rpc/ingest` | Validation, sequencing, acknowledgements |
| `docker` | Core/collector images, Compose, Prometheus, Grafana |

MIT licensed. See [LICENSE](LICENSE).
