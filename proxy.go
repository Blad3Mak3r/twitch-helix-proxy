package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// TwitchProxy handles proxying requests to the Twitch Helix API with
// authentication injection, global and per-endpoint rate limiting, and
// automatic retry logic.
type TwitchProxy struct {
	auth      TokenProvider
	limiter   Limiter
	registry  *EndpointRegistry
	clientID  string
	client    *http.Client
	targetURL *url.URL
}

// NewTwitchProxy creates a proxy with injected dependencies.
// clientID is stored separately so it can be injected into request headers
// without exposing it through the TokenProvider interface.
func NewTwitchProxy(
	auth TokenProvider,
	limiter Limiter,
	registry *EndpointRegistry,
	clientID string,
) *TwitchProxy {
	targetURL, err := url.Parse("https://api.twitch.tv")
	if err != nil {
		// url.Parse only fails on truly malformed strings; this is a programmer error.
		panic(fmt.Sprintf("failed to parse Twitch target URL: %v", err))
	}
	return &TwitchProxy{
		auth:      auth,
		limiter:   limiter,
		registry:  registry,
		clientID:  clientID,
		client:    &http.Client{Timeout: 30 * time.Second},
		targetURL: targetURL,
	}
}

// HealthCheck endpoint returns {"status":"ok"}.
func HealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok"}`)
}

// StatusHandler returns current proxy status without exposing internal mutexes.
func StatusHandler(proxy *TwitchProxy, igdbProxy *IgdbProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		helix := proxy.limiter.Status()
		igdb := igdbProxy.rateLimiter.Status()
		tokenRenewalIn := proxy.auth.TokenExpiresIn()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w,
			`{"helix":{"tokens_remaining":%d,"reset_in_seconds":%.1f},"igdb":{"tokens_remaining":%d,"reset_in_seconds":%.1f},"token_renewal_in_seconds":%.1f}`,
			helix.TokensRemaining, helix.ResetIn.Seconds(), igdb.TokensRemaining, igdb.ResetIn.Seconds(), tokenRenewalIn.Seconds())
	}
}

// ServeHTTP implements http.Handler. It acquires global and per-endpoint rate
// limit tokens, forwards the request to Twitch with managed credentials, and
// retries on 401/429/503 per Twitch API documentation.
func (tp *TwitchProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := generateRequestID()
	ctx := context.WithValue(r.Context(), ctxKeyRequestID, reqID)
	// Store the original request so per-request Limiters (e.g. DailyUniqueCounterLimiter)
	// can inspect query parameters via the context.
	ctx = context.WithValue(ctx, ctxKeyHTTPRequest, r)

	// Acquire global token first — blocks until capacity is available.
	if err := tp.limiter.Acquire(ctx); err != nil {
		http.Error(w, "Request cancelled", http.StatusRequestTimeout)
		return
	}

	// Acquire per-endpoint token if this path has a dedicated limiter.
	endpointLimiter := tp.registry.LimiterFor(r.Method, r.URL.Path)
	if endpointLimiter != nil {
		if err := endpointLimiter.Acquire(ctx); err != nil {
			if errors.Is(err, ErrDailyLimitExceeded) {
				http.Error(w, "Daily unique recipient limit exceeded", http.StatusTooManyRequests)
				return
			}
			http.Error(w, "Request cancelled", http.StatusRequestTimeout)
			return
		}
	}

	// Build the upstream URL preserving path and query string.
	targetURL := *tp.targetURL
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery

	logger.InfoContext(ctx, "proxying request",
		"request_id", reqID,
		"method", r.Method,
		"path", r.URL.Path)

	// Buffer the body once so it can be replayed on retries.
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Separate retry counters so a cascade of 401→refresh→401 cannot burn
	// through the 429 budget, and vice-versa.
	const maxAuthRetries = 2
	const maxRateRetries = 3
	authRetries := 0

	for attempt := 0; attempt <= maxRateRetries; attempt++ {
		proxyReq, err := http.NewRequestWithContext(ctx, r.Method, targetURL.String(), nil)
		if err != nil {
			http.Error(w, "Error creating upstream request", http.StatusInternalServerError)
			return
		}

		if len(bodyBytes) > 0 {
			proxyReq.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			proxyReq.ContentLength = int64(len(bodyBytes))
		}

		// Forward original headers, replacing auth with our managed credentials.
		for key, values := range r.Header {
			if key == "Host" || key == "Authorization" || key == "Client-Id" {
				continue
			}
			for _, value := range values {
				proxyReq.Header.Add(key, value)
			}
		}
		proxyReq.Header.Set("Client-Id", tp.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+tp.auth.GetAccessToken())

		startTime := time.Now()
		resp, err := tp.client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Error connecting to Twitch", http.StatusBadGateway)
			return
		}
		latency := time.Since(startTime)

		// Update global rate limiter from Twitch headers on every response.
		tp.limiter.UpdateFromHeaders(resp.Header)

		if rem := resp.Header.Get("Ratelimit-Remaining"); rem != "" {
			remaining, _ := strconv.Atoi(rem)
			logger.InfoContext(ctx, "upstream response",
				"request_id", reqID,
				"status", resp.StatusCode,
				"latency_ms", latency.Milliseconds(),
				"rate_remaining", remaining,
				"rate_limit", resp.Header.Get("Ratelimit-Limit"),
				"rate_reset", resp.Header.Get("Ratelimit-Reset"))
			if remaining < 100 {
				logger.WarnContext(ctx, "global rate limit low",
					"request_id", reqID,
					"tokens_remaining", remaining)
			}
		}

		switch resp.StatusCode {

		case http.StatusUnauthorized:
			// 401: token was rejected — refresh once, then retry.
			resp.Body.Close()
			if authRetries >= maxAuthRetries {
				logger.ErrorContext(ctx, "authentication failed after max retries",
					"request_id", reqID,
					"attempts", maxAuthRetries)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			authRetries++
			logger.WarnContext(ctx, "token rejected (401), refreshing",
				"request_id", reqID,
				"attempt", authRetries)
			if err := tp.auth.ForceRefresh(ctx); err != nil {
				logger.ErrorContext(ctx, "token refresh failed",
					"request_id", reqID,
					"error", err)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			// Do not increment attempt — auth retries do not count against the rate budget.
			attempt--
			continue

		case http.StatusTooManyRequests:
			// 429: back off until Twitch's reset timestamp, then retry.
			resp.Body.Close()
			if attempt >= maxRateRetries {
				logger.ErrorContext(ctx, "rate limit exceeded after max retries",
					"request_id", reqID,
					"attempts", maxRateRetries)
				w.Header().Set("Retry-After", resp.Header.Get("Ratelimit-Reset"))
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			waitDuration := backoffFromReset(resp.Header.Get("Ratelimit-Reset"), attempt)
			logger.WarnContext(ctx, "rate limited (429), waiting before retry",
				"request_id", reqID,
				"wait_seconds", waitDuration.Seconds(),
				"attempt", attempt+1,
				"max_attempts", maxRateRetries)

			timer := time.NewTimer(waitDuration)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				http.Error(w, "Request cancelled", http.StatusRequestTimeout)
				return
			}
			continue

		case http.StatusServiceUnavailable:
			// 503: Twitch docs say "retry once".
			resp.Body.Close()
			if attempt >= 1 {
				logger.ErrorContext(ctx, "Twitch service unavailable after retry",
					"request_id", reqID)
				http.Error(w, "Twitch service unavailable", http.StatusBadGateway)
				return
			}
			logger.WarnContext(ctx, "Twitch 503, retrying once",
				"request_id", reqID)
			continue

		default:
			// Success or non-retryable error — stream the response back.
			defer resp.Body.Close()

			// If the endpoint limiter can update itself from the response body
			// (e.g. CooldownLimiter reading retry_after from Start Commercial),
			// buffer the body, call the handler, then stream the buffered bytes.
			if endpointLimiter != nil {
				if rbh, ok := endpointLimiter.(ResponseBodyHandler); ok {
					respBody, readErr := io.ReadAll(resp.Body)
					if readErr == nil {
						rbh.HandleResponseBody(respBody)
						resp.Body = io.NopCloser(bytes.NewReader(respBody))
					}
				}
			}

			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				// Headers already written; can only log.
				logger.ErrorContext(ctx, "error streaming response body",
					"request_id", reqID,
					"error", err)
			}
			return
		}
	}
}

// backoffFromReset calculates how long to wait before the next retry after a 429.
// It uses the Ratelimit-Reset header (Unix epoch) when available, falling back to
// exponential backoff (2s, 4s, 8s…).
func backoffFromReset(rateLimitReset string, attempt int) time.Duration {
	if rateLimitReset != "" {
		if resetUnix, err := strconv.ParseInt(rateLimitReset, 10, 64); err == nil {
			d := time.Until(time.Unix(resetUnix, 0))
			if d > 0 {
				return d
			}
		}
	}
	// Exponential backoff: 2s, 4s, 8s…
	return time.Second * time.Duration(2<<uint(attempt))
}

// generateRequestID returns a 16-character hex string for request tracing.
func generateRequestID() string {
	return fmt.Sprintf("%016x", rand.Uint64())
}
