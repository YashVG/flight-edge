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

// MaxFlightsPerBatch limits memory and CPU committed to one collector message.
// Collectors may use smaller batches, but the core never accepts an unbounded one.
const MaxFlightsPerBatch = 1000

const defaultMaxInFlightBatches = 32

// ServerOption configures admission behaviour at the edge-to-core boundary.
type ServerOption func(*Server)

// WithMaxInFlightBatches bounds concurrent calls into the core. Each delivery
// session is already restricted to one in-flight sequence; this global bound
// prevents a large collector fleet from exhausting the core during a burst.
func WithMaxInFlightBatches(max int) ServerOption {
	return func(s *Server) {
		if max > 0 {
			s.admission = make(chan struct{}, max)
		}
	}
}

// WithRetryAfter sets the advisory delay returned when the core deliberately
// sheds a batch. The client still applies jitter to avoid synchronized retries.
func WithRetryAfter(delay time.Duration) ServerOption {
	return func(s *Server) {
		if delay > 0 {
			s.retryAfter = delay
		}
	}
}

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
	lastSequence map[streamKey]uint64
	inFlight     map[streamKey]uint64
	admission    chan struct{}
	retryAfter   time.Duration
}

type streamKey struct {
	sourceID  string
	sessionID string
}

// NewServer creates the FlightEdge ingestion RPC service.
func NewServer(ingest FlightIngestor, ready func() bool, options ...ServerOption) *Server {
	s := &Server{
		ingest:       ingest,
		ready:        ready,
		lastSequence: make(map[streamKey]uint64),
		inFlight:     make(map[streamKey]uint64),
		admission:    make(chan struct{}, defaultMaxInFlightBatches),
		retryAfter:   250 * time.Millisecond,
	}
	for _, option := range options {
		option(s)
	}
	return s
}

// Register adds the service to a gRPC server.
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	flightedgev1.RegisterFlightIngestServiceServer(registrar, s)
}

// StreamFlightStates accepts one ordered sequence from a collector. A response
// is sent for every valid request so the collector has an explicit
// protocol-level acknowledgement before it discards its local batch.
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
			ack.Disposition = flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID
			metrics.GRPCIngestRejected.Add(int64(ack.Rejected))
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		if !s.ready() {
			return status.Error(codes.Unavailable, "flightedge core is not ready")
		}

		key := streamKey{sourceID: req.GetSourceId(), sessionID: req.GetSessionId()}
		decision := s.reserveSequence(key, req.GetSequence())
		if decision != sequenceReserved {
			ack.Reason = decision.reason()
			ack.Disposition = decision.disposition()
			if decision.retryable() {
				ack.RetryAfterMs = uint32(s.retryAfter.Milliseconds())
			}
			if err := stream.Send(ack); err != nil {
				return err
			}
			continue
		}
		flights, err := flightsFromRequest(req)
		if err != nil {
			s.releaseSequence(key, req.GetSequence())
			ack.Rejected = uint32(len(req.GetFlights()))
			ack.Reason = err.Error()
			ack.Disposition = flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID
			metrics.GRPCIngestRejected.Add(int64(ack.Rejected))
			if sendErr := stream.Send(ack); sendErr != nil {
				return sendErr
			}
			continue
		}

		if !s.acquireAdmission() {
			s.releaseSequence(key, req.GetSequence())
			ack.Disposition = flightedgev1.BatchDisposition_BATCH_DISPOSITION_OVERLOADED
			ack.Reason = "core admission limit reached"
			ack.RetryAfterMs = uint32(s.retryAfter.Milliseconds())
			metrics.GRPCIngestOverloaded.Inc()
			if err := stream.Send(ack); err != nil {
				return err
			}
			continue
		}

		metrics.GRPCIngestActive.Inc()
		outcome, err := s.ingest(stream.Context(), flights)
		s.releaseAdmission()
		metrics.GRPCIngestActive.Dec()
		if err != nil {
			s.releaseSequence(key, req.GetSequence())
			return status.Errorf(codes.Unavailable, "ingesting batch %d: %v", req.GetSequence(), err)
		}

		ack.Accepted = uint32(outcome.Accepted)
		ack.Rejected = uint32(outcome.Rejected)
		ack.Reason = outcome.Reason
		if outcome.Rejected == 0 {
			s.markAcknowledged(key, req.GetSequence())
			ack.Disposition = flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED
		} else {
			s.releaseSequence(key, req.GetSequence())
			ack.Disposition = flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID
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
	if req.GetSessionId() == "" {
		return fmt.Errorf("session_id is required")
	}
	if req.GetSequence() == 0 {
		return fmt.Errorf("sequence must start at 1")
	}
	if len(req.GetFlights()) == 0 {
		return fmt.Errorf("at least one flight is required")
	}
	if len(req.GetFlights()) > MaxFlightsPerBatch {
		return fmt.Errorf("batch exceeds maximum of %d flights", MaxFlightsPerBatch)
	}
	if observedAt := req.GetObservedAt(); observedAt != nil && !observedAt.IsValid() {
		return fmt.Errorf("observed_at is invalid")
	}
	return nil
}

type sequenceDecision uint8

const (
	sequenceReserved sequenceDecision = iota
	sequenceDuplicate
	sequenceInFlight
	sequenceOutOfOrder
)

func (d sequenceDecision) reason() string {
	switch d {
	case sequenceDuplicate:
		return "duplicate sequence already acknowledged"
	case sequenceInFlight:
		return "sequence is already being ingested"
	case sequenceOutOfOrder:
		return "out-of-order sequence"
	default:
		return ""
	}
}

func (d sequenceDecision) disposition() flightedgev1.BatchDisposition {
	switch d {
	case sequenceDuplicate:
		return flightedgev1.BatchDisposition_BATCH_DISPOSITION_DUPLICATE
	case sequenceInFlight:
		return flightedgev1.BatchDisposition_BATCH_DISPOSITION_IN_FLIGHT
	case sequenceOutOfOrder:
		return flightedgev1.BatchDisposition_BATCH_DISPOSITION_OUT_OF_ORDER
	default:
		return flightedgev1.BatchDisposition_BATCH_DISPOSITION_UNSPECIFIED
	}
}

func (d sequenceDecision) retryable() bool {
	return d == sequenceInFlight
}

// reserveSequence atomically grants one stream ownership of the next sequence
// for a source. The reservation is released if ingestion fails.
func (s *Server) reserveSequence(key streamKey, sequence uint64) sequenceDecision {
	s.mu.Lock()
	defer s.mu.Unlock()

	last := s.lastSequence[key]
	if sequence <= last {
		return sequenceDuplicate
	}
	if pending, ok := s.inFlight[key]; ok {
		if pending == sequence {
			return sequenceInFlight
		}
		return sequenceOutOfOrder
	}
	if sequence != last+1 {
		return sequenceOutOfOrder
	}
	s.inFlight[key] = sequence
	return sequenceReserved
}

func (s *Server) markAcknowledged(key streamKey, sequence uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastSequence[key] = sequence
	delete(s.inFlight, key)
}

func (s *Server) releaseSequence(key streamKey, sequence uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFlight[key] == sequence {
		delete(s.inFlight, key)
	}
}

func (s *Server) acquireAdmission() bool {
	select {
	case s.admission <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *Server) releaseAdmission() {
	<-s.admission
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
