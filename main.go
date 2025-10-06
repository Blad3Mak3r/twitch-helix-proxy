package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

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
