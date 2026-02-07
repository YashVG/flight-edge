# FlightEdge

Real-time flight ontology engine optimized for edge computing constraints.

## Features

- **Flight Tracking**: Real-time ingestion from OpenSky Network API
- **Ontology Engine**: Graph-based data model for flights, airports, airlines, weather
- **Query Engine**: Sub-50ms P99 latency queries
- **Edge Optimized**: Runs within 512MB memory constraint
- **Prometheus Metrics**: Built-in monitoring and alerting

## Quick Start

### Using Docker

```bash
cd docker
docker-compose up -d
```

### Building from Source

```bash
# Build
go build -o flightedge ./cmd/flightedge

# Run
./flightedge
```

### Configuration

Set environment variables:

```bash
# Server
HTTP_ADDR=0.0.0.0
HTTP_PORT=8080

# OpenSky API (optional, for authenticated access)
OPENSKY_USERNAME=your_username
OPENSKY_PASSWORD=your_password

# Ingestion
POLL_INTERVAL=10s
ENABLE_INGESTION=true

# Edge optimization (see docs/EDGE_DEPLOYMENT.md)
MEMORY_MODE=normal
MEMORY_LIMIT_MB=512
DATA_RETENTION_HOURS=6
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        FlightEdge                                │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   OpenSky    │───▶│   Ingestion  │───▶│   Ontology   │      │
│  │   API        │    │   Processor  │    │   Engine     │      │
│  └──────────────┘    └──────────────┘    └──────┬───────┘      │
│                                                  │               │
│  ┌──────────────┐    ┌──────────────┐           │               │
│  │   HTTP API   │◀──▶│    Query     │◀──────────┘               │
│  │   /metrics   │    │    Engine    │                           │
│  └──────────────┘    └──────────────┘                           │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐      │
│  │   Memory     │    │   Data       │    │   Node       │      │
│  │   Monitor    │    │   Expiration │    │   Limiter    │      │
│  └──────────────┘    └──────────────┘    └──────────────┘      │
│                        Edge Management Layer                     │
└─────────────────────────────────────────────────────────────────┘
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/ready` | GET | Readiness check |
| `/live` | GET | Liveness check |
| `/metrics` | GET | Prometheus metrics |
| `/api/v1/flights` | GET | List flights |
| `/api/v1/flights/{id}` | GET | Get flight path |
| `/api/v1/airports/{code}` | GET | Get flights by airport |
| `/api/v1/delayed` | GET | Get delayed flights |
| `/api/v1/predict/{id}` | GET | Predict delay |
| `/api/v1/stats` | GET | System statistics |

## Testing

```bash
# Unit tests
go test ./...

# With verbose output
go test -v ./...

# Benchmarks
go test -bench=. ./benchmarks/...

# Full performance suite
go test -v -count=1 ./benchmarks/...
```

## Edge Deployment

FlightEdge is designed for edge computing with strict resource constraints:

- **Memory**: <512MB
- **CPU**: 0.5 cores
- **Latency**: <50ms P99

See [docs/EDGE_DEPLOYMENT.md](docs/EDGE_DEPLOYMENT.md) for detailed configuration.

## Project Structure

```
.
├── cmd/flightedge/     # Main application
├── internal/
│   ├── ontology/       # Graph engine
│   ├── query/          # Query engine
│   ├── ingestion/      # OpenSky API client
│   ├── metrics/        # Prometheus metrics
│   └── edge/           # Edge optimizations
├── pkg/models/         # Shared models
├── benchmarks/         # Performance tests
├── docker/             # Docker configuration
└── docs/               # Documentation
```

## License

MIT
