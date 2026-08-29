// Package ingest implements FlightEdge's versioned edge-to-core gRPC boundary.
package ingest

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	flightedgev1 "github.com/yash/flightedge/gen/flightedge/v1"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/internal/metrics"
	"github.com/yash/flightedge/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FlightIngestor is the core operation shared by local polling and gRPC
// collectors. It reports accepted/rejected counts so collectors can decide
// whether a batch should be retried.
type FlightIngestor func(context.Context, []models.Flight) (ingestion.BatchOutcome, error)

// Server accepts ordered state batches from one or more FlightEdge collectors.
// Sequence tracking is intentionally in-memory because FlightEdge's ontology is
// itself in-memory; after a core restart, collectors may safely resend data.
type Server struct {
	flightedgev1.UnimplementedFlightIngestServiceServer

	ingest FlightIngestor
	ready  func() bool

	mu           sync.Mutex
	lastSequence map[string]uint64
}

// NewServer creates the FlightEdge ingestion RPC service.
func NewServer(ingest FlightIngestor, ready func() bool) *Server {
	return &Server{
		ingest:       ingest,
		ready:        ready,
		lastSequence: make(map[string]uint64),
	}
}

// Register adds the service to a gRPC server.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	flightedgev1.RegisterFlightIngestServiceServer(registrar, s)
}

// StreamFlightStates accepts one ordered sequence from a collector. A response
// is sent for every valid request so the collector has a durable protocol-level
// acknowledgement before it discards its local batch.
func (s *Server) StreamFlightStates(stream grpc.BidiStreamingServer[flightedgev1.StreamFlightStatesRequest, flightedgev1.StreamFlightStatesResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		started := time.Now()
		ack := &flightedgev1.StreamFlightStatesResponse{Sequence: req.GetSequence()}
		if err := s.validateRequest(req); err != nil {
			ack.Rejected = uint32(len(req.GetFlights()))
			ack.Reason = err.Error()
			metrics.GRPCIngestRejected.Add(int64(ack.Rejected))
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		if !s.ready() {
			return status.Error(codes.Unavailable, "flightedge core is not ready")
		}

		if s.isDuplicateOrOutOfOrder(req.GetSourceId(), req.GetSequence()) {
			ack.Reason = "duplicate or out-of-order sequence already acknowledged"
			if err := stream.Send(ack); err != nil {
				return err
			}
			continue
		}

		flights, err := flightsFromRequest(req)
		if err != nil {
			ack.Rejected = uint32(len(req.GetFlights()))
			ack.Reason = err.Error()
			metrics.GRPCIngestRejected.Add(int64(ack.Rejected))
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		outcome, err := s.ingest(stream.Context(), flights)
		if err != nil {
			return status.Errorf(codes.Unavailable, "ingesting batch %d: %v", req.GetSequence(), err)
		}

		ack.Accepted = uint32(outcome.Accepted)
		ack.Rejected = uint32(outcome.Rejected)
		ack.Reason = outcome.Reason
		if outcome.Rejected == 0 {
			s.markAcknowledged(req.GetSourceId(), req.GetSequence())
		}

		metrics.GRPCIngestBatches.Inc()
		metrics.GRPCIngestFlights.Add(int64(outcome.Accepted))
		metrics.GRPCIngestRejected.Add(int64(outcome.Rejected))
		metrics.GRPCIngestLatency.Observe(time.Since(started).Seconds())

		if err := stream.Send(ack); err != nil {
			return err
		}
	}
}

func (s *Server) validateRequest(req *flightedgev1.StreamFlightStatesRequest) error {
	if req.GetSourceId() == "" {
		return fmt.Errorf("source_id is required")
	}
	if req.GetSequence() == 0 {
		return fmt.Errorf("sequence must start at 1")
	}
	if len(req.GetFlights()) == 0 {
		return fmt.Errorf("at least one flight is required")
	}
	if observedAt := req.GetObservedAt(); observedAt != nil && !observedAt.IsValid() {
		return fmt.Errorf("observed_at is invalid")
	}
	return nil
}

func (s *Server) isDuplicateOrOutOfOrder(sourceID string, sequence uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sequence <= s.lastSequence[sourceID]
}

func (s *Server) markAcknowledged(sourceID string, sequence uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSequence[sourceID] = sequence
}

func flightsFromRequest(req *flightedgev1.StreamFlightStatesRequest) ([]models.Flight, error) {
	flights := make([]models.Flight, 0, len(req.GetFlights()))
	for _, state := range req.GetFlights() {
		if state.GetIcao24() == "" {
			return nil, fmt.Errorf("flight icao24 is required")
		}
		if lastContact := state.GetLastContact(); lastContact != nil && !lastContact.IsValid() {
			return nil, fmt.Errorf("flight %q has an invalid last_contact", state.GetIcao24())
		}

		flight := models.Flight{
			ICAO24:    state.GetIcao24(),
			Callsign:  state.GetCallsign(),
			Origin:    state.GetOriginCountry(),
			Longitude: state.GetLongitude(),
			Latitude:  state.GetLatitude(),
			Altitude:  state.GetAltitude(),
			Velocity:  state.GetVelocity(),
			Heading:   state.GetHeading(),
			OnGround:  state.GetOnGround(),
		}
		if lastContact := state.GetLastContact(); lastContact != nil {
			flight.LastContact = lastContact.AsTime()
		}
		flights = append(flights, flight)
	}
	return flights, nil
}
