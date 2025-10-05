package main

import (
	"context"
	"encoding/json"
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

// TwitchAuthManager handles authentication and token renewal
type TwitchAuthManager struct {
	mu           sync.RWMutex
	clientID     string
	clientSecret string
	accessToken  string
	expiresAt    time.Time
	client       *http.Client
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

func NewTwitchAuthManager(clientID, clientSecret string) *TwitchAuthManager {
	am := &TwitchAuthManager{
		clientID:     clientID,
		clientSecret: clientSecret,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Get initial token
	if err := am.refreshToken(); err != nil {
		log.Fatalf("❌ Error obtaining initial token: %v", err)
	}

	// Start automatic renewal goroutine
	go am.autoRefresh()

	return am
}

// refreshToken obtains a new access token
func (am *TwitchAuthManager) refreshToken() error {
	log.Printf("🔑 Requesting new access token...")

	data := url.Values{}
	data.Set("client_id", am.clientID)
	data.Set("client_secret", am.clientSecret)
	data.Set("grant_type", "client_credentials")

	resp, err := am.client.PostForm("https://id.twitch.tv/oauth2/token", data)
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	am.mu.Lock()
	am.accessToken = tokenResp.AccessToken
	// Renew 10 minutes before expiry (safety buffer)
	renewBuffer := 10 * time.Minute
	expiryDuration := time.Duration(tokenResp.ExpiresIn) * time.Second

	// If token expires in less than 10 minutes, renew at 20% remaining time
	if expiryDuration < renewBuffer {
		renewBuffer = expiryDuration * 20 / 100
	}

	am.expiresAt = time.Now().Add(expiryDuration - renewBuffer)
	am.mu.Unlock()

	log.Printf("✅ Token obtained (expires in %d seconds, renewal in %.1f minutes)",
		tokenResp.ExpiresIn, (expiryDuration - renewBuffer).Minutes())
	return nil
}

// autoRefresh automatically renews the token before it expires
func (am *TwitchAuthManager) autoRefresh() {
	for {
		am.mu.RLock()
		timeUntilExpiry := time.Until(am.expiresAt)
		am.mu.RUnlock()

		if timeUntilExpiry <= 30*time.Second {
			// Token is about to expire or already expired, renew immediately
			if err := am.refreshToken(); err != nil {
				log.Printf("❌ Error renewing token: %v. Retrying in 10s...", err)
				time.Sleep(10 * time.Second)
				continue
			}
		} else {
			// Wait until it's time to renew
			log.Printf("⏰ Next token renewal in %.1f minutes", timeUntilExpiry.Minutes())
			time.Sleep(timeUntilExpiry)
		}
	}
}

// GetAccessToken returns the current token (thread-safe)
func (am *TwitchAuthManager) GetAccessToken() string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.accessToken
}

// ValidateToken verifies if the current token is valid
func (am *TwitchAuthManager) ValidateToken() error {
	am.mu.RLock()
	token := am.accessToken
	am.mu.RUnlock()

	req, err := http.NewRequest("GET", "https://id.twitch.tv/oauth2/validate", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "OAuth "+token)

	resp, err := am.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid token: status %d", resp.StatusCode)
	}

	log.Printf("✅ Token validated successfully")
	return nil
}

// TwitchRateLimiter with version detection based on headers
type TwitchRateLimiter struct {
	mu              sync.RWMutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int

	// Tracking to detect stale responses
	lastResetTime   int64 // Unix timestamp of last seen Ratelimit-Reset
	lowestRemaining int   // Lowest value seen in current bucket
}

func NewTwitchRateLimiter() *TwitchRateLimiter {
	now := time.Now()
	return &TwitchRateLimiter{
		tokensRemaining: 800,
		resetTime:       now.Add(time.Minute),
		bucketCapacity:  800,
		minBuffer:       50,
		lastResetTime:   now.Add(time.Minute).Unix(),
		lowestRemaining: 800,
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

	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Case 1: New rate limit window (reset time changed)
	if resetUnix > rl.lastResetTime {
		log.Printf("🔄 New bucket detected: reset %d → %d", rl.lastResetTime, resetUnix)
		rl.lastResetTime = resetUnix
		rl.resetTime = time.Unix(resetUnix, 0)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem

		if limit != "" {
			if lim, err := strconv.Atoi(limit); err == nil {
				rl.bucketCapacity = lim
			}
		}

		log.Printf("📊 Bucket reset: %d/%d tokens available", rem, rl.bucketCapacity)
		return
	}

	// Case 2: Same window, but more recent response (lower remaining)
	if resetUnix == rl.lastResetTime && rem < rl.lowestRemaining {
		log.Printf("🔽 Valid update: tokens %d → %d (same bucket)",
			rl.tokensRemaining, rem)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem
		return
	}

	// Case 3: Stale response (higher remaining than minimum seen)
	if resetUnix == rl.lastResetTime && rem > rl.lowestRemaining {
		log.Printf("⏪ Stale response ignored: remaining=%d (current=%d)",
			rem, rl.lowestRemaining)
		return
	}

	// Case 4: Response from previous bucket (resetUnix < rl.lastResetTime)
	if resetUnix < rl.lastResetTime {
		log.Printf("⏪ Old bucket response ignored (reset %d < %d)",
			resetUnix, rl.lastResetTime)
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
				rl.lastResetTime = rl.resetTime.Unix()
				rl.lowestRemaining = rl.bucketCapacity
				log.Printf("🔄 Bucket auto-reset: %d tokens", rl.bucketCapacity)
			}
			rl.mu.Unlock()
			continue
		}

		// If enough tokens available, allow
		if rl.tokensRemaining > rl.minBuffer {
			rl.mu.RUnlock()

			// Decrement with write lock
			rl.mu.Lock()
			// Double-check tokens are still available
			if rl.tokensRemaining > rl.minBuffer {
				rl.tokensRemaining--
				rl.lowestRemaining = rl.tokensRemaining
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

		log.Printf("⏸️  Rate limit: %d tokens remaining, waiting %.1fs until reset (%s)",
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

		// Inject current authentication
		proxyReq.Header.Set("Client-Id", tp.authManager.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+tp.authManager.GetAccessToken())

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
				resetTime, err := strconv.ParseInt(rateLimitReset, 10, 64)
				if err == nil {
					waitUntil := time.Unix(resetTime, 0)
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
		tokenExpiry := time.Until(proxy.authManager.expiresAt)
		proxy.authManager.mu.RUnlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"tokens_remaining":%d,"reset_in_seconds":%.1f,"token_expires_in_seconds":%.1f}`,
			remaining, resetIn.Seconds(), tokenExpiry.Seconds())
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
