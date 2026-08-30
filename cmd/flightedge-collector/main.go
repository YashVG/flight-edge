// flightedge-collector polls OpenSky at an edge site and forwards the filtered
// state stream to a FlightEdge core over gRPC.
package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	flightedgev1 "github.com/yash/flightedge/gen/flightedge/v1"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/internal/metrics"
	"github.com/yash/flightedge/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health" // Enable client-side gRPC health checking.
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type config struct {
	coreAddr        string
	collectorID     string
	credentialsFile string
	openSkyClientID string
	openSkySecret   string
	pollInterval    time.Duration
	batchSize       int
	queueCapacity   int
	deliveryTimeout time.Duration
	metricsAddr     string
}

func loadConfig() config {
	collectorID := env("COLLECTOR_ID", "")
	if collectorID == "" {
		hostname, err := os.Hostname()
		if err != nil {
			hostname = "unknown-collector"
		}
		collectorID = hostname
	}

	cfg := config{
		coreAddr:        env("FLIGHTEDGE_CORE_GRPC_ADDR", "localhost:9091"),
		collectorID:     collectorID,
		credentialsFile: env("CREDENTIALS_FILE", "credentials.json"),
		openSkyClientID: env("OPENSKY_CLIENT_ID", ""),
		openSkySecret:   env("OPENSKY_CLIENT_SECRET", ""),
		pollInterval:    envDuration("POLL_INTERVAL", 10*time.Second),
		batchSize:       envInt("BATCH_SIZE", 100),
		queueCapacity:   envInt("OUTBOUND_QUEUE_CAPACITY", 64),
		deliveryTimeout: envDuration("DELIVERY_TIMEOUT", 10*time.Second),
		metricsAddr:     env("COLLECTOR_METRICS_ADDR", ":9092"),
	}
	if cfg.openSkyClientID == "" || cfg.openSkySecret == "" {
		if credentials, err := ingestion.LoadCredentials(cfg.credentialsFile); err == nil {
			cfg.openSkyClientID = credentials.ClientID
			cfg.openSkySecret = credentials.ClientSecret
		}
	}
	return cfg
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil && parsed > 0 {
			return parsed
		}
	}
	return fallback
}

// sender keeps one ordered gRPC stream per collector. The collector sends the
// same sequence again after a transport failure; the core de-duplicates a
// batch that was accepted before its acknowledgement was lost.
type sender struct {
	mu           sync.Mutex
	sourceID     string
	sessionID    string
	nextSeq      uint64
	client       flightedgev1.FlightIngestServiceClient
	conn         *grpc.ClientConn
	parentCtx    context.Context
	streamCancel context.CancelFunc
	stream       grpc.BidiStreamingClient[flightedgev1.StreamFlightStatesRequest, flightedgev1.StreamFlightStatesResponse]
}

const flightIngestHealthServiceConfig = `{"healthCheckConfig":{"serviceName":"flightedge.v1.FlightIngestService"}}`

func newSender(parentCtx context.Context, coreAddr, sourceID string) (*sender, error) {
	sessionID, err := newSessionID()
	if err != nil {
		return nil, fmt.Errorf("create collector session ID: %w", err)
	}
	return newSenderForSession(parentCtx, coreAddr, sourceID, sessionID)
}

func newSenderForSession(parentCtx context.Context, coreAddr, sourceID, sessionID string) (*sender, error) {
	// This client is intentionally plaintext for localhost and private-network
	// development. Production remote collectors must use mTLS before exposure.
	conn, err := grpc.NewClient(
		coreAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(flightIngestHealthServiceConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("dial FlightEdge core: %w", err)
	}
	return &sender{
		sourceID:  sourceID,
		sessionID: sessionID,
		nextSeq:   1,
		client:    flightedgev1.NewFlightIngestServiceClient(conn),
		conn:      conn,
		parentCtx: parentCtx,
	}, nil
}

func newSessionID() (string, error) {
	var id [16]byte
	if _, err := cryptorand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func (s *sender) Send(ctx context.Context, flights []models.Flight) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for attempt := 0; attempt < 3; attempt++ {
		if err := s.sendOnce(ctx, flights); err == nil {
			s.nextSeq++
			return nil
		} else if !isRetryableDeliveryError(err) {
			return fmt.Errorf("deliver batch %d: non-retryable failure: %w", s.nextSeq, err)
		} else if attempt == 2 {
			return fmt.Errorf("deliver batch %d after retries: %w", s.nextSeq, err)
		} else {
			s.closeStream()
			metrics.CollectorRetries.Inc()
			backoff := retryDelay(attempt, retryAfter(err))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
		}
	}
	return nil
}

type deliveryError struct {
	err        error
	retryable  bool
	retryAfter time.Duration
}

func (e *deliveryError) Error() string { return e.err.Error() }
func (e *deliveryError) Unwrap() error { return e.err }

func isRetryableDeliveryError(err error) bool {
	var deliveryErr *deliveryError
	if errors.As(err, &deliveryErr) {
		return deliveryErr.retryable
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func retryAfter(err error) time.Duration {
	var deliveryErr *deliveryError
	if errors.As(err, &deliveryErr) {
		return deliveryErr.retryAfter
	}
	return 0
}

// retryDelay uses capped exponential backoff with full jitter. It prevents a
// fleet of collectors from reconnecting in lockstep after a core outage.
func retryDelay(attempt int, minimum time.Duration) time.Duration {
	base := 250 * time.Millisecond
	for i := 0; i < attempt && base < 2*time.Second; i++ {
		base *= 2
	}
	if base > 2*time.Second {
		base = 2 * time.Second
	}
	if minimum > base {
		base = minimum
	}
	return minimum + time.Duration(rand.Int64N(int64(base)+1))
}

func (s *sender) sendOnce(ctx context.Context, flights []models.Flight) error {
	if s.stream == nil {
		streamCtx, cancel := context.WithCancel(s.parentCtx)
		stream, err := s.client.StreamFlightStates(streamCtx)
		if err != nil {
			cancel()
			return err
		}
		s.stream = stream
		s.streamCancel = cancel
	}

	now := time.Now().UTC()
	request := &flightedgev1.StreamFlightStatesRequest{
		SourceId:   s.sourceID,
		SessionId:  s.sessionID,
		Sequence:   s.nextSeq,
		ObservedAt: timestamppb.New(now),
		Flights:    make([]*flightedgev1.FlightState, 0, len(flights)),
	}
	for _, flight := range flights {
		request.Flights = append(request.Flights, &flightedgev1.FlightState{
			Icao24:        flight.ICAO24,
			Callsign:      flight.Callsign,
			OriginCountry: flight.Origin,
			Longitude:     flight.Longitude,
			Latitude:      flight.Latitude,
			Altitude:      flight.Altitude,
			Velocity:      flight.Velocity,
			Heading:       flight.Heading,
			OnGround:      flight.OnGround,
			LastContact:   timestamppb.New(flight.LastContact),
		})
	}

	stream := s.stream
	if err := waitForRPC(ctx, s.abortStream, func() error { return stream.Send(request) }); err != nil {
		return err
	}
	var ack *flightedgev1.StreamFlightStatesResponse
	err := waitForRPC(ctx, s.abortStream, func() error {
		var recvErr error
		ack, recvErr = stream.Recv()
		return recvErr
	})
	if err != nil {
		return err
	}
	if ack.GetSequence() != s.nextSeq {
		return fmt.Errorf("unexpected acknowledgement sequence %d", ack.GetSequence())
	}
	switch ack.GetDisposition() {
	case flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED:
		if ack.GetAccepted() == uint32(len(flights)) && ack.GetRejected() == 0 {
			return nil
		}
		return &deliveryError{err: fmt.Errorf("core partially acknowledged batch %d: %s", s.nextSeq, ack.GetReason())}
	case flightedgev1.BatchDisposition_BATCH_DISPOSITION_DUPLICATE:
		return nil
	case flightedgev1.BatchDisposition_BATCH_DISPOSITION_IN_FLIGHT,
		flightedgev1.BatchDisposition_BATCH_DISPOSITION_OVERLOADED:
		return &deliveryError{
			err:        fmt.Errorf("core deferred batch %d: %s", s.nextSeq, ack.GetReason()),
			retryable:  true,
			retryAfter: time.Duration(ack.GetRetryAfterMs()) * time.Millisecond,
		}
	case flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID,
		flightedgev1.BatchDisposition_BATCH_DISPOSITION_OUT_OF_ORDER:
		return &deliveryError{err: fmt.Errorf("core rejected batch %d: %s", s.nextSeq, ack.GetReason())}
	default:
		return &deliveryError{err: fmt.Errorf("core returned unspecified acknowledgement for batch %d", s.nextSeq)}
	}
}

func (s *sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeStream()
	return s.conn.Close()
}

func (s *sender) closeStream() {
	if s.stream != nil {
		_ = s.stream.CloseSend()
	}
	if s.streamCancel != nil {
		s.streamCancel()
	}
	s.stream = nil
	s.streamCancel = nil
}

// abortStream cancels an in-flight RPC without concurrently calling CloseSend.
// The caller waits for the blocked Send or Recv to return before retrying.
func (s *sender) abortStream() {
	if s.streamCancel != nil {
		s.streamCancel()
	}
	s.stream = nil
	s.streamCancel = nil
}

func waitForRPC(ctx context.Context, cancelStream func(), call func() error) error {
	result := make(chan error, 1)
	go func() { result <- call() }()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		cancelStream()
		<-result
		return ctx.Err()
	}
}

var errDeliveryQueueFull = errors.New("collector delivery queue is full")

// deliveryQueue isolates OpenSky polling from core availability while keeping
// memory bounded. One worker preserves the collector's source/sequence order.
// A full queue is deliberately visible as a dropped batch, not hidden loss.
type deliveryQueue struct {
	sender  *sender
	batches chan []models.Flight
	timeout time.Duration
	done    chan struct{}
}

func newDeliveryQueue(sender *sender, capacity int, timeout time.Duration) *deliveryQueue {
	return &deliveryQueue{
		sender:  sender,
		batches: make(chan []models.Flight, capacity),
		timeout: timeout,
		done:    make(chan struct{}),
	}
}

func (q *deliveryQueue) Start(ctx context.Context) {
	go func() {
		defer close(q.done)
		for {
			select {
			case <-ctx.Done():
				return
			case flights := <-q.batches:
				metrics.CollectorQueueDepth.Dec()
				batchCtx, cancel := context.WithTimeout(ctx, q.timeout)
				err := q.sender.Send(batchCtx, flights)
				cancel()
				if err != nil {
					metrics.CollectorDroppedBatches.Inc()
					log.Printf("dropping queued batch of %d flights after delivery failure: %v", len(flights), err)
					continue
				}
				metrics.CollectorDeliveredBatches.Inc()
			}
		}
	}()
}

func (q *deliveryQueue) Enqueue(_ context.Context, flights []models.Flight) error {
	// The processor reuses its backing arrays, so queue ownership requires a copy.
	batch := append([]models.Flight(nil), flights...)
	select {
	case q.batches <- batch:
		metrics.CollectorQueuedBatches.Inc()
		metrics.CollectorQueueDepth.Inc()
		return nil
	default:
		metrics.CollectorDroppedBatches.Inc()
		return errDeliveryQueueFull
	}
}

func (q *deliveryQueue) Wait() {
	<-q.done
}

func startMetricsServer(ctx context.Context, addr string) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(metrics.Default().Export()))
	})

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("collector metrics server stopped: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	return server
}

func main() {
	cfg := loadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sender, err := newSender(ctx, cfg.coreAddr, cfg.collectorID)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sender.Close() }()

	queue := newDeliveryQueue(sender, cfg.queueCapacity, cfg.deliveryTimeout)
	queue.Start(ctx)
	defer queue.Wait()
	startMetricsServer(ctx, cfg.metricsAddr)

	options := []ingestion.ClientOption{}
	if cfg.openSkyClientID != "" && cfg.openSkySecret != "" {
		options = append(options, ingestion.WithClientCredentials(cfg.openSkyClientID, cfg.openSkySecret))
	}
	client := ingestion.NewClient(options...)
	processor := ingestion.NewProcessor(client, ingestion.ProcessorConfig{
		PollInterval: cfg.pollInterval,
		Filter:       ingestion.YVRFilter(),
		BatchSize:    cfg.batchSize,
		// A collector owns one ordered sequence, so its batches are serialized.
		Workers: 1,
	}, queue.Enqueue)

	log.Printf("FlightEdge collector %q forwarding to %s", cfg.collectorID, cfg.coreAddr)
	if count, err := processor.ProcessOnce(ctx); err != nil {
		log.Printf("initial collector poll failed: %v", err)
	} else {
		log.Printf("forwarded %d filtered flight states", count)
	}
	if err := processor.Start(ctx); err != nil {
		log.Fatal(err)
	}
	<-ctx.Done()
	processor.Stop()
}
