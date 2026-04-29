package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// main initializes and starts the Twitch Helix API proxy server.
// Required environment variables: TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET
// Optional environment variables: PORT (default "3000"), LOG_FORMAT ("text" or "json")
func main() {
	initLogger()

	clientID := os.Getenv("TWITCH_CLIENT_ID")
	clientSecret := os.Getenv("TWITCH_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		logger.Error("TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set")
		os.Exit(1)
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}
	addr := ":" + port

	// Root context — cancelled on SIGINT/SIGTERM to trigger graceful shutdown
	// of background goroutines (token auto-refresh) and the HTTP server.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// --- Dependencies ---

	auth := NewTwitchAuthManager(ctx, clientID, clientSecret)
	globalLimiter := NewTwitchRateLimiter()
	registry := buildEndpointRegistry()

	proxy := NewTwitchProxy(auth, globalLimiter, registry, clientID)
	igdbProxy := NewIgdbProxy(auth, clientID, NewIgdbRateLimiter())

	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthCheck)
	mux.HandleFunc("/status", StatusHandler(proxy, igdbProxy))
	mux.Handle("/helix/", proxy)
	mux.Handle("/igdb/", igdbProxy)

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	logger.Info("twitch proxy starting",
		"addr", addr,
		"helix_endpoint", "http://localhost"+addr+"/helix/...",
		"igdb_endpoint", "http://localhost"+addr+"/igdb/...",
		"status_endpoint", "http://localhost"+addr+"/status")

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Block until SIGINT/SIGTERM.
	<-ctx.Done()
	logger.Info("shutdown signal received, draining requests")

	// Give in-flight requests up to 15 s to complete.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown error", "error", err)
	}

	logger.Info("server stopped cleanly")
}

// buildEndpointRegistry registers per-endpoint rate limiters for all Twitch
// Helix API paths that carry documented limits beyond the global 800 req/min.
func buildEndpointRegistry() *EndpointRegistry {
	r := NewEndpointRegistry()

	// --- TokenBucketLimiters (fixed N requests per window) ---

	// POST /helix/clips — 100 req/min
	r.Register(http.MethodPost, "/helix/clips",
		NewTokenBucketLimiter("clips", 100, time.Minute))

	// Extension configuration service — 20 req/min
	r.Register("", "/helix/extensions/configurations",
		NewTokenBucketLimiter("ext-config", 20, time.Minute))

	// Extension PubSub — 100 req/min per combination
	r.Register(http.MethodPost, "/helix/extensions/pubsub",
		NewTokenBucketLimiter("ext-pubsub", 100, time.Minute))

	// Send Extension Chat Message — 12 req/min per channel
	r.Register(http.MethodPost, "/helix/extensions/chat",
		NewTokenBucketLimiter("ext-chat", 12, time.Minute))

	// Hold AutoMod Message — 5 req/min
	r.Register(http.MethodPost, "/helix/moderation/automod/message",
		NewTokenBucketLimiter("automod", 5, time.Minute))

	// --- SlidingWindowLimiters (rolling window) ---

	// Add/Remove Moderator — 10 per 10 s (shared across add and remove)
	r.Register(http.MethodPost, "/helix/moderation/moderators",
		NewSlidingWindowLimiter("add-moderators", 10, 10*time.Second))
	r.Register(http.MethodDelete, "/helix/moderation/moderators",
		NewSlidingWindowLimiter("remove-moderators", 10, 10*time.Second))

	// Add/Remove Channel VIP — 10 per 10 s
	r.Register(http.MethodPost, "/helix/channels/vips",
		NewSlidingWindowLimiter("add-vips", 10, 10*time.Second))
	r.Register(http.MethodDelete, "/helix/channels/vips",
		NewSlidingWindowLimiter("remove-vips", 10, 10*time.Second))

	// Send a Shoutout — 1 per 2 min (global), 1 per 60 s (per broadcaster)
	// We enforce the stricter global bound: 1 per 2 min.
	r.Register(http.MethodPost, "/helix/chat/shoutouts",
		NewSlidingWindowLimiter("shoutouts", 1, 2*time.Minute))

	// --- CooldownLimiter (dynamic cooldown from response body) ---

	// Start Commercial — cooldown duration comes from retry_after in response
	r.Register(http.MethodPost, "/helix/channels/commercial",
		NewCooldownLimiter("commercial"))

	// --- MultiBucketLimiter (multiple simultaneous constraints) ---

	// Send Whisper — 3/sec + 100/min + 40 unique recipients/day
	whispers := NewMultiBucketLimiter(
		"whispers",
		NewTokenBucketLimiter("whispers-per-sec", 3, time.Second),
		NewTokenBucketLimiter("whispers-per-min", 100, time.Minute),
		NewDailyUniqueCounterLimiter("whispers-daily-unique", 40, "to_id"),
	)
	r.Register(http.MethodPost, "/helix/whispers", whispers)

	return r
}
