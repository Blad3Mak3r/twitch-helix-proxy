package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TwitchRateLimiter with continuous refill token bucket algorithm
type TwitchRateLimiter struct {
	mu              sync.RWMutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int
	refillRate      float64 // tokens per second (800/60 = 13.33)

	// Tracking to detect stale responses
	bucketID        string // Unique identifier for current bucket (reset timestamp)
	lowestRemaining int    // Lowest value seen in current bucket
	lastUpdate      time.Time
}

func NewTwitchRateLimiter() *TwitchRateLimiter {
	now := time.Now()
	return &TwitchRateLimiter{
		tokensRemaining: 800,
		resetTime:       now.Add(time.Minute),
		bucketCapacity:  800,
		minBuffer:       50,
		refillRate:      800.0 / 60.0, // ~13.33 tokens/second
		bucketID:        fmt.Sprintf("%d", now.Unix()),
		lowestRemaining: 800,
		lastUpdate:      now,
	}
}

// UpdateFromHeaders updates only if data is more recent
func (rl *TwitchRateLimiter) UpdateFromHeaders(remaining, limit, reset string) {
	if remaining == "" || reset == "" {
		return
	}

	rem, err := strconv.Atoi(remaining)
	if err != nil {
		return
	}

	// Ratelimit-Reset is a Unix epoch timestamp
	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Calculate reset time from Unix timestamp
	newResetTime := time.Unix(resetUnix, 0)

	// Generate bucket ID from reset timestamp
	newBucketID := fmt.Sprintf("%d", resetUnix)

	// Update last update time
	rl.lastUpdate = time.Now()

	// Case 1: New bucket detected (different reset timestamp)
	if newBucketID != rl.bucketID {
		// Check if this is actually a newer bucket
		if newResetTime.After(rl.resetTime) {
			log.Printf("🔄 New bucket detected: reset %s → %s",
				rl.resetTime.Format("15:04:05"), newResetTime.Format("15:04:05"))
			rl.resetTime = newResetTime
			rl.bucketID = newBucketID
			rl.tokensRemaining = rem
			rl.lowestRemaining = rem

			if limit != "" {
				if lim, err := strconv.Atoi(limit); err == nil {
					rl.bucketCapacity = lim
					// Update refill rate based on bucket capacity
					rl.refillRate = float64(lim) / 60.0
				}
			}

			log.Printf("📊 Bucket reset: %d/%d tokens available (refill: %.2f/s)",
				rem, rl.bucketCapacity, rl.refillRate)
			return
		}

		// Old bucket response, ignore
		log.Printf("⏪ Old bucket response ignored (reset %s < current %s)",
			newResetTime.Format("15:04:05"), rl.resetTime.Format("15:04:05"))
		return
	}

	// Case 2: Same bucket, more recent response (lower remaining)
	if rem < rl.lowestRemaining {
		log.Printf("🔽 Valid update: tokens %d → %d (same bucket)",
			rl.tokensRemaining, rem)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem
		return
	}

	// Case 3: Stale response (higher remaining than minimum seen)
	if rem > rl.lowestRemaining {
		log.Printf("⏪ Stale response ignored: remaining=%d (current=%d)",
			rem, rl.lowestRemaining)
		return
	}
}

// Acquire uses RWMutex to allow concurrent reads
func (rl *TwitchRateLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.RLock()

		// Check if bucket has reset
		now := time.Now()
		if now.After(rl.resetTime) {
			rl.mu.RUnlock()

			// Upgrade to write lock to reset
			rl.mu.Lock()
			// Double-check after acquiring write lock
			if time.Now().After(rl.resetTime) {
				rl.tokensRemaining = rl.bucketCapacity
				rl.resetTime = time.Now().Add(time.Minute)
				rl.bucketID = fmt.Sprintf("%d", rl.resetTime.Unix())
				rl.lowestRemaining = rl.bucketCapacity
				rl.lastUpdate = time.Now()
				log.Printf("🔄 Bucket auto-reset: %d tokens", rl.bucketCapacity)
			}
			rl.mu.Unlock()
			continue
		}

		// Simulate continuous refill based on time elapsed
		// This is an optimistic local estimation between Twitch updates
		elapsed := now.Sub(rl.lastUpdate).Seconds()
		estimatedRefill := int(elapsed * rl.refillRate)
		estimatedTokens := rl.tokensRemaining + estimatedRefill
		if estimatedTokens > rl.bucketCapacity {
			estimatedTokens = rl.bucketCapacity
		}

		// If estimated tokens are sufficient, allow
		if estimatedTokens > rl.minBuffer {
			rl.mu.RUnlock()

			// Decrement with write lock
			rl.mu.Lock()
			// Recalculate with write lock held
			elapsed = time.Since(rl.lastUpdate).Seconds()
			estimatedRefill = int(elapsed * rl.refillRate)
			estimatedTokens = rl.tokensRemaining + estimatedRefill
			if estimatedTokens > rl.bucketCapacity {
				estimatedTokens = rl.bucketCapacity
			}

			if estimatedTokens > rl.minBuffer {
				// Don't update tokensRemaining here - let Twitch headers be the source of truth
				// Just record that we made a request
				rl.mu.Unlock()
				return nil
			}
			rl.mu.Unlock()
			continue
		}

		// No tokens, calculate wait time
		waitUntil := rl.resetTime
		tokensLeft := rl.tokensRemaining
		rl.mu.RUnlock()

		waitDuration := time.Until(waitUntil)
		if waitDuration < 0 {
			continue // Bucket should reset, retry
		}

		log.Printf("⏸️  Rate limit: %d tokens remaining (estimated), waiting %.1fs until reset (%s)",
			tokensLeft, waitDuration.Seconds(), waitUntil.Format("15:04:05"))

		// Wait with cancellation
		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// GetStatus returns current state
func (rl *TwitchRateLimiter) GetStatus() (remaining int, resetIn time.Duration) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.tokensRemaining, time.Until(rl.resetTime)
}

// TwitchProxy handles proxying with authentication and rate limiting
type TwitchProxy struct {
	authManager *TwitchAuthManager
	rateLimiter *TwitchRateLimiter
	client      *http.Client
	targetURL   *url.URL
}

func NewTwitchProxy(clientID, clientSecret string) *TwitchProxy {
	targetURL, _ := url.Parse("https://api.twitch.tv")

	return &TwitchProxy{
		authManager: NewTwitchAuthManager(clientID, clientSecret),
		rateLimiter: NewTwitchRateLimiter(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		targetURL: targetURL,
	}
}

func (tp *TwitchProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Rate limiting (concurrent - not serialized)
	if err := tp.rateLimiter.Acquire(r.Context()); err != nil {
		http.Error(w, "Request cancelled", http.StatusRequestTimeout)
		return
	}

	// Build Twitch URL
	targetURL := *tp.targetURL
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery

	log.Printf("🔄 %s %s", r.Method, targetURL.String())

	// Read body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Make request with retries
	maxRetries := 3
	for retry := 0; retry <= maxRetries; retry++ {
		proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), nil)
		if err != nil {
			http.Error(w, "Error creating request", http.StatusInternalServerError)
			return
		}

		if len(bodyBytes) > 0 {
			proxyReq.Body = io.NopCloser(strings.NewReader(string(bodyBytes)))
			proxyReq.ContentLength = int64(len(bodyBytes))
		}

		// Copy original headers (except Host, Authorization, Client-Id)
		for key, values := range r.Header {
			if key != "Host" && key != "Authorization" && key != "Client-Id" {
				for _, value := range values {
					proxyReq.Header.Add(key, value)
				}
			}
		}

		// Get a valid token
		token, err := tp.authManager.GetAccessToken()
		if err != nil {
			log.Printf("❌ Error getting access token: %v", err)
			http.Error(w, "Authentication error", http.StatusInternalServerError)
			return
		}

		// Inject current authentication
		proxyReq.Header.Set("Client-Id", tp.authManager.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+token)

		startTime := time.Now()
		resp, err := tp.client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Error connecting to Twitch", http.StatusBadGateway)
			return
		}
		latency := time.Since(startTime)

		// Read rate limit headers
		rateLimitLimit := resp.Header.Get("Ratelimit-Limit")
		rateLimitRemaining := resp.Header.Get("Ratelimit-Remaining")
		rateLimitReset := resp.Header.Get("Ratelimit-Reset")

		// Update rate limiter (with automatic version detection)
		tp.rateLimiter.UpdateFromHeaders(rateLimitRemaining, rateLimitLimit, rateLimitReset)

		// Log with latency
		if rateLimitRemaining != "" {
			remaining, _ := strconv.Atoi(rateLimitRemaining)
			log.Printf("📊 [%dms] Rate limit: %s/%s tokens (reset: %s)",
				latency.Milliseconds(), rateLimitRemaining, rateLimitLimit, rateLimitReset)

			if remaining < 100 {
				log.Printf("⚠️  Only %d tokens remaining", remaining)
			}
		}

		// If token invalid (401), renew and retry
		if resp.StatusCode == http.StatusUnauthorized {
			resp.Body.Close()
			log.Printf("🔑 Invalid token (401), renewing...")

			if err := tp.authManager.refreshToken(); err != nil {
				log.Printf("❌ Error renewing token: %v", err)
				http.Error(w, "Authentication failed", http.StatusUnauthorized)
				return
			}

			continue
		}

		// Handle 429
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()

			if retry >= maxRetries {
				log.Printf("❌ Rate limit 429 after %d retries", maxRetries)
				w.Header().Set("Retry-After", rateLimitReset)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			var waitDuration time.Duration
			if rateLimitReset != "" {
				resetUnix, err := strconv.ParseInt(rateLimitReset, 10, 64)
				if err == nil {
					waitUntil := time.Unix(resetUnix, 0)
					waitDuration = time.Until(waitUntil)
					if waitDuration < 0 {
						waitDuration = time.Second
					}
				} else {
					waitDuration = time.Second * time.Duration(2<<uint(retry))
				}
			} else {
				waitDuration = time.Second * time.Duration(2<<uint(retry))
			}

			log.Printf("❌ 429 - Waiting %.1fs", waitDuration.Seconds())
			tp.rateLimiter.UpdateFromHeaders("0", rateLimitLimit, rateLimitReset)

			time.Sleep(waitDuration)
			continue
		}

		// Successful response
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}

		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		resp.Body.Close()

		return
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok"}`)
}

func statusHandler(proxy *TwitchProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		remaining, resetIn := proxy.rateLimiter.GetStatus()

		proxy.authManager.mu.RLock()
		tokenRenewalIn := time.Until(proxy.authManager.expiresAt)
		proxy.authManager.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"tokens_remaining":%d,"reset_in_seconds":%.1f,"token_renewal_in_seconds":%.1f}`,
			remaining, resetIn.Seconds(), tokenRenewalIn.Seconds())
	}
}

func main() {
	clientID := os.Getenv("TWITCH_CLIENT_ID")
	clientSecret := os.Getenv("TWITCH_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		log.Fatal("❌ TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set")
	}

	proxy := NewTwitchProxy(clientID, clientSecret)

	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/status", statusHandler(proxy))
	http.Handle("/helix/", proxy)

	addr := ":3000"
	log.Printf("🚀 Twitch proxy running on http://localhost%s", addr)
	log.Printf("📡 Endpoint: http://localhost%s/helix/...", addr)
	log.Printf("📊 Status: http://localhost%s/status", addr)
	log.Printf("🔑 Auth: Client Credentials OAuth with automatic renewal")
	log.Printf("⚡ Concurrent rate limiting with version detection")

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
