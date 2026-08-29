// flightedge-collector polls OpenSky at an edge site and forwards the filtered
// state stream to a FlightEdge core over gRPC.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	flightedgev1 "github.com/yash/flightedge/gen/flightedge/v1"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	mu       sync.Mutex
	sourceID string
	nextSeq  uint64
	client   flightedgev1.FlightIngestServiceClient
	conn     *grpc.ClientConn
	stream   grpc.BidiStreamingClient[flightedgev1.StreamFlightStatesRequest, flightedgev1.StreamFlightStatesResponse]
}

func newSender(coreAddr, sourceID string) (*sender, error) {
	// This client is intentionally plaintext for localhost and private-network
	// development. Production remote collectors must use mTLS before exposure.
	conn, err := grpc.Dial(coreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial FlightEdge core: %w", err)
	}
	return &sender{
		sourceID: sourceID,
		nextSeq:  1,
		client:   flightedgev1.NewFlightIngestServiceClient(conn),
		conn:     conn,
	}, nil
}

func (s *sender) Send(ctx context.Context, flights []models.Flight) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for attempt := 0; attempt < 3; attempt++ {
		if err := s.sendOnce(ctx, flights); err == nil {
			s.nextSeq++
			return nil
		} else if attempt == 2 {
			return fmt.Errorf("deliver batch %d after retries: %w", s.nextSeq, err)
		}

		s.stream = nil
		backoff := time.Duration(attempt+1) * 250 * time.Millisecond
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}
	return nil
}

func (s *sender) sendOnce(ctx context.Context, flights []models.Flight) error {
	if s.stream == nil {
		stream, err := s.client.StreamFlightStates(ctx)
		if err != nil {
			return err
		}
		s.stream = stream
	}

	now := time.Now().UTC()
	request := &flightedgev1.StreamFlightStatesRequest{
		SourceId:   s.sourceID,
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

	if err := s.stream.Send(request); err != nil {
		return err
	}
	ack, err := s.stream.Recv()
	if err != nil {
		return err
	}
	if ack.GetSequence() != s.nextSeq {
		return fmt.Errorf("unexpected acknowledgement sequence %d", ack.GetSequence())
	}
	if ack.GetRejected() > 0 {
		return fmt.Errorf("core rejected %d flights: %s", ack.GetRejected(), ack.GetReason())
	}
	return nil
}

func (s *sender) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stream != nil {
		_ = s.stream.CloseSend()
	}
	return s.conn.Close()
}

func main() {
	cfg := loadConfig()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sender, err := newSender(cfg.coreAddr, cfg.collectorID)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = sender.Close() }()

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
	}, sender.Send)

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
