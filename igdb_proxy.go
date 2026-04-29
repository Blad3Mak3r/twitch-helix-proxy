package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type IgdbProxy struct {
	auth        TokenProvider
	clientID    string
	rateLimiter Limiter
	client      *http.Client
	targetURL   *url.URL
}

func NewIgdbProxy(auth TokenProvider, clientID string, rateLimiter Limiter) *IgdbProxy {
	targetURL, err := url.Parse("https://api.igdb.com/v4")
	if err != nil {
		panic(fmt.Sprintf("failed to parse IGDB target URL: %v", err))
	}

	return &IgdbProxy{
		auth:        auth,
		clientID:    clientID,
		rateLimiter: rateLimiter,
		client:      &http.Client{Timeout: 30 * time.Second},
		targetURL:   targetURL,
	}
}

func (ip *IgdbProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqID := generateRequestID()
	ctx := context.WithValue(r.Context(), ctxKeyRequestID, reqID)
	targetURL := *ip.targetURL
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery

	logger.InfoContext(ctx, "proxying IGDB request",
		"request_id", reqID,
		"method", r.Method,
		"path", r.URL.Path)

	if err := ip.rateLimiter.Acquire(ctx); err != nil {
		http.Error(w, "Request cancelled", http.StatusRequestTimeout)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

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

		for key, values := range r.Header {
			if key == "Host" || key == "Authorization" || key == "Client-Id" {
				continue
			}
			for _, value := range values {
				proxyReq.Header.Add(key, value)
			}
		}

		proxyReq.Header.Set("Client-Id", ip.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+ip.auth.GetAccessToken())

		startTime := time.Now()
		resp, err := ip.client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Error connecting to IGDB", http.StatusBadGateway)
			return
		}
		latency := time.Since(startTime)

		ip.rateLimiter.UpdateFromHeaders(resp.Header)

		if rem := resp.Header.Get("X-RateLimit-Remaining"); rem != "" {
			remaining, _ := strconv.Atoi(rem)
			logger.InfoContext(ctx, "IGDB upstream response",
				"request_id", reqID,
				"status", resp.StatusCode,
				"latency_ms", latency.Milliseconds(),
				"rate_remaining", remaining,
				"rate_limit", resp.Header.Get("X-RateLimit-Limit"),
				"rate_reset", resp.Header.Get("X-RateLimit-Reset"))
		}

		switch resp.StatusCode {

		case http.StatusUnauthorized:
			resp.Body.Close()
			if authRetries >= maxAuthRetries {
				logger.ErrorContext(ctx, "IGDB authentication failed after max retries",
					"request_id", reqID,
					"attempts", maxAuthRetries)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			authRetries++
			logger.WarnContext(ctx, "IGDB token rejected (401), refreshing",
				"request_id", reqID,
				"attempt", authRetries)
			if err := ip.auth.ForceRefresh(ctx); err != nil {
				logger.ErrorContext(ctx, "IGDB token refresh failed",
					"request_id", reqID,
					"error", err)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			attempt--
			continue

		case http.StatusTooManyRequests:
			resp.Body.Close()
			if attempt >= maxRateRetries {
				logger.ErrorContext(ctx, "IGDB rate limit exceeded after max retries",
					"request_id", reqID,
					"attempts", maxRateRetries)
				retryAfter := resp.Header.Get("X-RateLimit-Reset")
				if retryAfter == "" {
					retryAfter = "1"
				}
				w.Header().Set("Retry-After", retryAfter)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			waitDuration := backoffFromReset(resp.Header.Get("X-RateLimit-Reset"), attempt)
			logger.WarnContext(ctx, "IGDB rate limited (429), waiting before retry",
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

		default:
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				logger.ErrorContext(ctx, "error streaming IGDB response body",
					"request_id", reqID,
					"error", err)
			}
			return
		}
	}
}