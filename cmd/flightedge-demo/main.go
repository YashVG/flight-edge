// flightedge-demo exercises the production ingestion RPC with synthetic data.
// It starts an ephemeral loopback server and never contacts OpenSky.
package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync/atomic"
	"time"

	flightedgev1 "github.com/yash/flightedge/gen/flightedge/v1"
	"github.com/yash/flightedge/internal/ingestion"
	"github.com/yash/flightedge/internal/rpc/ingest"
	"github.com/yash/flightedge/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(output io.Writer) error {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()

	var applied atomic.Int64
	server := grpc.NewServer()
	ingest.NewServer(func(_ context.Context, flights []models.Flight) (ingestion.BatchOutcome, error) {
		applied.Add(int64(len(flights)))
		return ingestion.BatchOutcome{Accepted: len(flights)}, nil
	}, func() bool { return true }).Register(server)
	defer server.Stop()
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := flightedgev1.NewFlightIngestServiceClient(conn).StreamFlightStates(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = stream.CloseSend() }()

	if _, err := fmt.Fprintln(output, "Synthetic batches through the production gRPC handler; counting sink, no external feed."); err != nil {
		return err
	}
	cases := []struct {
		label       string
		session     string
		sequence    uint64
		icao24      string
		disposition flightedgev1.BatchDisposition
		applied     int64
	}{
		{"first delivery", "session-a", 1, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, 1},
		{"retry after lost ACK", "session-a", 1, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_DUPLICATE, 1},
		{"skip a sequence", "session-a", 3, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_OUT_OF_ORDER, 1},
		{"next ordered batch", "session-a", 2, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, 2},
		{"collector restart", "session-b", 1, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, 3},
		{"malformed flight", "session-b", 2, "", flightedgev1.BatchDisposition_BATCH_DISPOSITION_INVALID, 3},
		{"corrected retry", "session-b", 2, "abc123", flightedgev1.BatchDisposition_BATCH_DISPOSITION_ACCEPTED, 4},
	}
	for _, scenario := range cases {
		request := &flightedgev1.StreamFlightStatesRequest{
			SourceId: "demo-collector", SessionId: scenario.session, Sequence: scenario.sequence,
			Flights: []*flightedgev1.FlightState{{Icao24: scenario.icao24, Callsign: "DEMO001"}},
		}
		if err := stream.Send(request); err != nil {
			return fmt.Errorf("%s: %w", scenario.label, err)
		}
		ack, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("%s: %w", scenario.label, err)
		}
		if ack.GetDisposition() != scenario.disposition || ack.GetSequence() != scenario.sequence || applied.Load() != scenario.applied {
			return fmt.Errorf("%s: unexpected acknowledgement %v; applied=%d", scenario.label, ack, applied.Load())
		}
		if _, err := fmt.Fprintf(output, "%-23s %-40s applied=%d\n", scenario.label, ack.GetDisposition(), applied.Load()); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(output, "PASS: 7 deliveries, 4 applied; duplicates and invalid/out-of-order batches did not reach the sink.")
	return err
}
