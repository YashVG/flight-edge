package ingest

import (
	"context"
	"net"
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

func newTestClient(t *testing.T, ready func() bool, ingest FlightIngestor) (flightedgev1.FlightIngestServiceClient, func()) {
	t.Helper()

	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	NewServer(ingest, ready).Register(server)
	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.DialContext(context.Background(), "bufnet",
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
	require.Equal(t, "abc123", received.ICAO24)
	require.Equal(t, "ACA101", received.Callsign)

	// Retrying an acknowledged sequence is safe and does not mutate the core.
	require.NoError(t, stream.Send(request("collector-yvr", 1)))
	duplicate, err := stream.Recv()
	require.NoError(t, err)
	require.Zero(t, duplicate.GetAccepted())
	require.Zero(t, duplicate.GetRejected())
	require.Contains(t, duplicate.GetReason(), "duplicate")
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
	require.Contains(t, ack.GetReason(), "source_id")
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
