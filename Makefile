APP_NAME := flightedge
BUILD_DIR := bin
DOCKER_IMAGE := flightedge:latest
DOCKER_COMPOSE := docker/docker-compose.yml

.PHONY: build test bench docker clean lint run dev help

# Default target
.DEFAULT_GOAL := help

## Build
build: ## Build the application binary
	@echo "Building $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build -o $(BUILD_DIR)/$(APP_NAME) ./cmd/flightedge/

build-optimized: ## Build with optimizations (smaller binary)
	@echo "Building optimized $(APP_NAME)..."
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o $(BUILD_DIR)/$(APP_NAME) ./cmd/flightedge/

## Run
run: build ## Build and run the application
	./$(BUILD_DIR)/$(APP_NAME)

dev: ## Run with live reload (requires air: go install github.com/cosmtrek/air@latest)
	@command -v air >/dev/null 2>&1 || { echo "Installing air..."; go install github.com/cosmtrek/air@latest; }
	air

## Test
test: ## Run all tests
	go test -v -race -count=1 ./...

test-short: ## Run tests (skip long-running)
	go test -v -short -count=1 ./...

test-cover: ## Run tests with coverage report
	go test -v -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

## Benchmarks
bench: ## Run all benchmarks
	go test -bench=. -benchmem -run=^$$ -timeout=10m ./...

bench-ontology: ## Run ontology benchmarks
	go test -bench=. -benchmem -run=^$$ ./benchmarks/ontology_bench_test.go

bench-query: ## Run query engine benchmarks
	go test -bench=. -benchmem -run=^$$ ./internal/query/

bench-load: ## Run load generator benchmarks
	go test -bench=BenchmarkLoadGenerator -benchmem -run=^$$ -timeout=5m ./benchmarks/

bench-concurrent: ## Run concurrent query benchmarks
	go test -bench=BenchmarkConcurrentQueries -benchmem -run=^$$ -timeout=5m ./benchmarks/

bench-memory: ## Run memory constraint benchmarks
	go test -bench=BenchmarkMemory -benchmem -run=^$$ -timeout=5m ./benchmarks/

bench-latency: ## Run latency distribution benchmarks
	go test -bench=BenchmarkLatency -benchmem -run=^$$ -timeout=5m ./benchmarks/

bench-summary: ## Run comprehensive benchmark summary
	go test -v -run TestBenchmarkSummary -timeout=10m ./benchmarks/

bench-regression: ## Run performance regression tests
	go test -v -run TestPerformanceRegression -timeout=5m ./benchmarks/

## Profiling
profile-cpu: ## Generate CPU profile
	ENABLE_PROFILING=true go test -v -run TestCPUProfile -timeout=10m ./benchmarks/
	@echo "Analyze with: go tool pprof cpu.prof"

profile-mem: ## Generate memory profile
	ENABLE_PROFILING=true go test -v -run TestMemoryProfile -timeout=10m ./benchmarks/
	@echo "Analyze with: go tool pprof mem.prof"

profile-all: profile-cpu profile-mem ## Generate all profiles

## Lint
lint: ## Run linters
	go vet ./...
	@command -v staticcheck >/dev/null 2>&1 && staticcheck ./... || echo "staticcheck not installed"

fmt: ## Format code
	go fmt ./...

## Docker
docker-build: ## Build Docker image
	docker build -f docker/Dockerfile -t $(DOCKER_IMAGE) .

docker-build-debug: ## Build Docker debug image
	docker build -f docker/Dockerfile --target debug -t $(DOCKER_IMAGE)-debug .

docker-run: docker-build ## Run in Docker container
	docker run --rm -p 8080:8080 \
		--memory=512m --cpus=0.5 \
		-e ENABLE_INGESTION=false \
		$(DOCKER_IMAGE)

docker-shell: ## Shell into Docker container
	docker run --rm -it --entrypoint /bin/sh $(DOCKER_IMAGE)

## Docker Compose
up: ## Start all services with docker-compose
	docker-compose -f $(DOCKER_COMPOSE) up -d

up-monitoring: ## Start all services including Grafana
	docker-compose -f $(DOCKER_COMPOSE) --profile monitoring up -d

down: ## Stop all services
	docker-compose -f $(DOCKER_COMPOSE) down

logs: ## View logs
	docker-compose -f $(DOCKER_COMPOSE) logs -f

logs-app: ## View application logs only
	docker-compose -f $(DOCKER_COMPOSE) logs -f flightedge

ps: ## Show running containers
	docker-compose -f $(DOCKER_COMPOSE) ps

restart: ## Restart services
	docker-compose -f $(DOCKER_COMPOSE) restart

## Utilities
clean: ## Clean build artifacts
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	go clean -testcache

deps: ## Download dependencies
	go mod download
	go mod verify

update-deps: ## Update dependencies
	go get -u ./...
	go mod tidy

## Health checks
health: ## Check application health
	@curl -sf http://localhost:8080/health | jq . || echo "Service not running"

metrics: ## View Prometheus metrics
	@curl -sf http://localhost:8080/metrics | head -50 || echo "Service not running"

stats: ## View application stats
	@curl -sf http://localhost:8080/api/v1/stats | jq . || echo "Service not running"

## Help
help: ## Show this help message
	@echo "FlightEdge - Real-time Flight Ontology Engine"
	@echo ""
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
