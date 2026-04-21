package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// TwitchProxy handles proxying requests to the Twitch Helix API with
// authentication injection, rate limiting, and automatic retry logic.
type TwitchProxy struct {
	authManager *TwitchAuthManager
	rateLimiter *TwitchRateLimiter
	client      *http.Client
	targetURL   *url.URL
}

func NewTwitchProxy(clientID, clientSecret string) *TwitchProxy {
	targetURL, err := url.Parse("https://api.twitch.tv")
	if err != nil {
		log.Fatalf("❌ Failed to parse target URL: %v", err)
	}

	return &TwitchProxy{
		authManager: NewTwitchAuthManager(serverContext, clientID, clientSecret),
		rateLimiter: NewTwitchRateLimiter(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
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
func StatusHandler(proxy *TwitchProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		remaining, resetIn := proxy.rateLimiter.GetStatus()
		tokenRenewalIn := proxy.authManager.TokenExpiresIn()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"tokens_remaining":%d,"reset_in_seconds":%.1f,"token_renewal_in_seconds":%.1f}`,
			remaining, resetIn.Seconds(), tokenRenewalIn.Seconds())
	}
}

func (tp *TwitchProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Acquire a rate limit token before touching Twitch. This blocks until a
	// token is available or the client disconnects (ctx cancelled).
	if err := tp.rateLimiter.Acquire(ctx); err != nil {
		http.Error(w, "Request cancelled", http.StatusRequestTimeout)
		return
	}

	// Build Twitch URL preserving path and query string.
	targetURL := *tp.targetURL
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery

	log.Printf("🔄 %s %s", r.Method, targetURL.String())

	// Buffer the body once so we can replay it on retries.
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
			http.Error(w, "Error creating request", http.StatusInternalServerError)
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

		proxyReq.Header.Set("Client-Id", tp.authManager.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+tp.authManager.GetAccessToken())

		startTime := time.Now()
		resp, err := tp.client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Error connecting to Twitch", http.StatusBadGateway)
			return
		}
		latency := time.Since(startTime)

		rateLimitLimit := resp.Header.Get("Ratelimit-Limit")
		rateLimitRemaining := resp.Header.Get("Ratelimit-Remaining")
		rateLimitReset := resp.Header.Get("Ratelimit-Reset")

		tp.rateLimiter.UpdateFromHeaders(rateLimitRemaining, rateLimitLimit, rateLimitReset)

		if rateLimitRemaining != "" {
			remaining, _ := strconv.Atoi(rateLimitRemaining)
			log.Printf("📊 [%dms] Rate limit: %s/%s tokens (reset: %s)",
				latency.Milliseconds(), rateLimitRemaining, rateLimitLimit, rateLimitReset)
			if remaining < 100 {
				log.Printf("⚠️  Only %d tokens remaining", remaining)
			}
		}

		switch resp.StatusCode {

		case http.StatusUnauthorized:
			// 401: token was rejected — refresh once, then retry.
			resp.Body.Close()
			if authRetries >= maxAuthRetries {
				log.Printf("❌ Token refresh failed after %d attempts", maxAuthRetries)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			authRetries++
			log.Printf("🔑 Invalid token (401), renewing (attempt %d/%d)...", authRetries, maxAuthRetries)
			if err := tp.authManager.ForceTokenRefresh(ctx); err != nil {
				log.Printf("❌ Error renewing token: %v", err)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}
			// Do not increment attempt — this retry doesn't count against the rate limit budget.
			attempt--
			continue

		case http.StatusTooManyRequests:
			// 429: back off until Twitch's reset timestamp, then retry.
			resp.Body.Close()
			if attempt >= maxRateRetries {
				log.Printf("❌ Rate limit exceeded after %d retries - giving up", maxRateRetries)
				w.Header().Set("Retry-After", rateLimitReset)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			waitDuration := backoffFromReset(rateLimitReset, attempt)
			log.Printf("⏳ Rate limited (429) - waiting %.1fs before retry %d/%d",
				waitDuration.Seconds(), attempt+1, maxRateRetries)

			tp.rateLimiter.UpdateFromHeaders("0", rateLimitLimit, rateLimitReset)

			// Respect context cancellation during the wait.
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
				log.Printf("❌ Twitch service unavailable after retry")
				http.Error(w, "Twitch service unavailable", http.StatusBadGateway)
				return
			}
			log.Printf("⚠️  Twitch 503 - retrying once per API docs")
			continue

		default:
			// Success or non-retryable error — stream the response back.
			defer resp.Body.Close()
			for key, values := range resp.Header {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
			w.WriteHeader(resp.StatusCode)
			if _, err := io.Copy(w, resp.Body); err != nil {
				// Headers already written; log but don't write another error.
				log.Printf("⚠️  Error streaming response body: %v", err)
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
	// Exponential backoff: 2<<0=2, 2<<1=4, 2<<2=8 seconds.
	return time.Second * time.Duration(2<<uint(attempt))
}
