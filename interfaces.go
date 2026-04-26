package main

import (
	"context"
	"net/http"
	"time"
)

// TokenProvider manages the OAuth access token lifecycle.
// All implementations must be safe for concurrent use.
type TokenProvider interface {
	// GetAccessToken returns the current cached access token.
	GetAccessToken() string
	// ForceRefresh forces a token refresh, typically triggered by a 401 response.
	ForceRefresh(ctx context.Context) error
	// TokenExpiresIn returns the duration until the next scheduled renewal.
	TokenExpiresIn() time.Duration
}

// Limiter is the single contract for any rate limiting scope.
// All implementations must be safe for concurrent use.
type Limiter interface {
	// Acquire blocks until a token is available or ctx is cancelled.
	Acquire(ctx context.Context) error
	// UpdateFromHeaders adjusts internal state from Twitch response headers.
	// Implementations that are driven by local counters only should no-op.
	UpdateFromHeaders(h http.Header)
	// Status returns a snapshot of current state for the /status endpoint.
	Status() LimiterStatus
}

// LimiterStatus holds a point-in-time snapshot of a Limiter's state.
type LimiterStatus struct {
	Name            string
	TokensRemaining int
	ResetIn         time.Duration
}

// ResponseBodyHandler is an optional interface for Limiters that need to
// inspect the upstream response body to determine their next state.
// The proxy performs a type assertion and calls this after a successful
// response when the endpoint limiter implements it.
type ResponseBodyHandler interface {
	HandleResponseBody(body []byte)
}

// ctxKey is an unexported type for context keys in this package.
type ctxKey int

const (
	// ctxKeyRequestID carries a per-request trace ID through the context.
	ctxKeyRequestID ctxKey = iota
	// ctxKeyHTTPRequest carries the original *http.Request for Limiters
	// that need to inspect query parameters (e.g. DailyUniqueCounterLimiter).
	ctxKeyHTTPRequest
)
