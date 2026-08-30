package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryClassification(t *testing.T) {
	require.True(t, isRetryableDeliveryError(status.Error(codes.Unavailable, "core unavailable")))
	require.True(t, isRetryableDeliveryError(status.Error(codes.ResourceExhausted, "overloaded")))
	require.True(t, isRetryableDeliveryError(context.DeadlineExceeded))
	require.False(t, isRetryableDeliveryError(status.Error(codes.InvalidArgument, "bad batch")))
	require.False(t, isRetryableDeliveryError(&deliveryError{err: errors.New("out of order")}))
	require.True(t, isRetryableDeliveryError(&deliveryError{err: errors.New("admission full"), retryable: true}))
}

func TestRetryDelayHonoursServerHintAndCap(t *testing.T) {
	for attempt := 0; attempt < 4; attempt++ {
		delay := retryDelay(attempt, 750*time.Millisecond)
		require.GreaterOrEqual(t, delay, time.Duration(0))
		ceiling := 250 * time.Millisecond
		for i := 0; i < attempt && ceiling < 2*time.Second; i++ {
			ceiling *= 2
		}
		if ceiling < 750*time.Millisecond {
			ceiling = 750 * time.Millisecond
		}
		require.GreaterOrEqual(t, delay, 750*time.Millisecond)
		require.LessOrEqual(t, delay, 750*time.Millisecond+ceiling)
	}

	// A large server hint is a minimum wait. The jittered portion prevents a
	// fleet of collectors from retrying in lockstep when that delay expires.
	delay := retryDelay(1, 3*time.Second)
	require.GreaterOrEqual(t, delay, 3*time.Second)
	require.LessOrEqual(t, delay, 6*time.Second)
}

func TestNewSessionIDsAreDistinct(t *testing.T) {
	first, err := newSessionID()
	require.NoError(t, err)
	second, err := newSessionID()
	require.NoError(t, err)
	require.Len(t, first, 32)
	require.NotEqual(t, first, second)
}
