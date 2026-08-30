package ingest

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	flightedgev1 "github.com/yash/flightedge/gen/flightedge/v1"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const bufferSize = 1024 * 1024

func newTestClient(t *testing.T, ready func() bool, ingest FlightIngestor, options ...ServerOption) (flightedgev1.FlightIngestServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	NewServer(ingest, ready, options...).Register(server)
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	return flightedgev1.NewFlightIngestServiceClient(conn), func() {
		require.NoError(t, conn.Close())
		server.Stop()
		require.NoError(t, listener.Close())
	}
}

func request(sourceID string, sequence uint64) *flightedgev1.StreamFlightStatesRequest {
	now := timestamppb.New(time.Now().UTC())
	return &flightedgev1.StreamFlightStatesRequest{
		SourceId:   sourceID,
		SessionId:  "session-a",
		Sequence:   sequence,
		ObservedAt: now,
		Flights: []*flightedgev1.FlightState{{
			Icao24:        "abc123",
			Callsign:      "ACA101",
			OriginCountry: "Canada",
			Latitude:      49.1947,
			Longitude:     -123.1792,
			LastContact:   now,
		}},
	}
}

func TestStreamFlightStatesAcknowledgesAndDeduplicates(t *testing.T) {
	var calls atomic.Int64
	var received models.Flight
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		received = flights[0]
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	stream, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(request("collector-yvr", 1)))

	ack, err := stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 1, ack.GetSequence())
	require.EqualValues(t, 1, ack.GetAccepted())
	require.Zero(t, ack.GetRejected())
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, ack.GetDisposition())
	require.Equal(t, "abc123", received.ICAO24)
	require.Equal(t, "ACA101", received.Callsign)

	// Retrying an acknowledged sequence is safe and does not mutate the core.
	require.NoError(t, stream.Send(request("collector-yvr", 1)))
	duplicate, err := stream.Recv()
	require.NoError(t, err)
	require.Zero(t, duplicate.GetAccepted())
	require.Zero(t, duplicate.GetRejected())
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_DUPLICATE, duplicate.GetDisposition())
	require.EqualValues(t, 1, calls.Load())
	require.NoError(t, stream.CloseSend())
}

func TestStreamFlightStatesRejectsMalformedBatchWithoutCallingCore(t *testing.T) {
	var calls atomic.Int64
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	stream, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(request("", 1)))

	ack, err := stream.Recv()
	require.NoError(t, err)
	require.EqualValues(t, 1, ack.GetRejected())
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID, ack.GetDisposition())
	require.Zero(t, calls.Load())
	require.NoError(t, stream.CloseSend())
}

func TestStreamFlightStatesReportsUnavailableCore(t *testing.T) {
	client, cleanup := newTestClient(t, func() bool { return false }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	stream, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(request("collector-yvr", 1)))

	_, err = stream.Recv()
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestStreamFlightStatesReservesSequenceAcrossConcurrentStreams(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int64
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		close(started)
		<-release
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	first, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, first.Send(request("collector-yvr", 1)))
	firstAck := make(chan *flightedgev1.StreamFlightStatesResponse, 1)
	firstErr := make(chan error, 1)
	go func() {
		ack, recvErr := first.Recv()
		firstAck <- ack
		firstErr <- recvErr
	}()

	<-started
	second, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, second.Send(request("collector-yvr", 1)))
	inFlightAck, err := second.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_IN_FLIGHT, inFlightAck.GetDisposition())
	require.NotZero(t, inFlightAck.GetRetryAfterMs())
	require.Zero(t, inFlightAck.GetAccepted())
	require.EqualValues(t, 1, calls.Load())

	close(release)
	require.NoError(t, <-firstErr)
	require.EqualValues(t, 1, (<-firstAck).GetAccepted())

	third, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, third.Send(request("collector-yvr", 1)))
	duplicateAck, err := third.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_DUPLICATE, duplicateAck.GetDisposition())
	require.EqualValues(t, 1, calls.Load())
}

func TestStreamFlightStatesRejectsOversizedBatch(t *testing.T) {
	var calls atomic.Int64
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	oversized := request("collector-yvr", 1)
	oversized.Flights = make([]*flightedgev1.FlightState, MaxFlightsPerBatch+1)
	for i := range oversized.Flights {
		oversized.Flights[i] = &flightedgev1.FlightState{Icao24: "abc123"}
	}

	stream, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(oversized))
	ack, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID, ack.GetDisposition())
	require.EqualValues(t, MaxFlightsPerBatch+1, ack.GetRejected())
	require.Zero(t, calls.Load())
}

func TestStreamFlightStatesHandlesConcurrentCollectorFanIn(t *testing.T) {
	const collectors = 16
	const batchesPerCollector = 25

	var calls atomic.Int64
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	errs := make(chan error, collectors)
	for collector := 0; collector < collectors; collector++ {
		go func(collector int) {
			stream, err := client.StreamFlightStates(context.Background())
			if err != nil {
				errs <- err
				return
			}
			defer func() { _ = stream.CloseSend() }()

			sourceID := fmt.Sprintf("collector-%02d", collector)
			for sequence := 1; sequence <= batchesPerCollector; sequence++ {
				if err := stream.Send(request(sourceID, uint64(sequence))); err != nil {
					errs <- err
					return
				}
				ack, err := stream.Recv()
				if err != nil {
					errs <- err
					return
				}
				if ack.GetAccepted() != 1 || ack.GetRejected() != 0 {
					errs <- fmt.Errorf("collector %d sequence %d: accepted=%d rejected=%d reason=%q", collector, sequence, ack.GetAccepted(), ack.GetRejected(), ack.GetReason())
					return
				}
			}
			errs <- nil
		}(collector)
	}

	for collector := 0; collector < collectors; collector++ {
		require.NoError(t, <-errs)
	}
	require.EqualValues(t, collectors*batchesPerCollector, calls.Load())
}

func TestStreamFlightStatesAcceptsNewCollectorSessionAtSequenceOne(t *testing.T) {
	var calls atomic.Int64
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		calls.Add(1)
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	})
	defer cleanup()

	first, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, first.Send(request("collector-yvr", 1)))
	ack, err := first.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, ack.GetDisposition())
	require.NoError(t, first.CloseSend())

	second, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	restarted := request("collector-yvr", 1)
	restarted.SessionId = "session-b"
	require.NoError(t, second.Send(restarted))
	ack, err = second.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, ack.GetDisposition())
	require.EqualValues(t, 2, calls.Load())
}

func TestStreamFlightStatesShedsWhenAdmissionLimitReached(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var startedOnce sync.Once
	client, cleanup := newTestClient(t, func() bool { return true }, func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		startedOnce.Do(func() { close(started) })
		<-release
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	}, WithMaxInFlightBatches(1), WithRetryAfter(10*time.Millisecond))
	defer cleanup()

	first, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	require.NoError(t, first.Send(request("collector-yvr", 1)))
	firstAck := make(chan error, 1)
	go func() {
		_, recvErr := first.Recv()
		firstAck <- recvErr
	}()
	<-started

	second, err := client.StreamFlightStates(context.Background())
	require.NoError(t, err)
	overloaded := request("collector-yul", 1)
	overloaded.SessionId = "session-b"
	require.NoError(t, second.Send(overloaded))
	ack, err := second.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_OVERLOADED, ack.GetDisposition())
	require.EqualValues(t, 10, ack.GetRetryAfterMs())

	close(release)
	require.NoError(t, <-firstAck)
	// The shed request did not consume its sequence reservation and can retry.
	require.NoError(t, second.Send(overloaded))
	ack, err = second.Recv()
	require.NoError(t, err)
	require.Equal(t, flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, ack.GetDisposition())
}
