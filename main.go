package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// serverContext is the application-level context. It is cancelled on SIGINT/SIGTERM
// to allow background goroutines (token auto-refresh) to shut down cleanly.
// It is a package-level variable so NewTwitchProxy can pass it to NewTwitchAuthManager
// without threading it through every constructor call.
var serverContext context.Context

// main initializes and starts the Twitch Helix API proxy server.
// It requires TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET environment variables.
// The server listens on port 3000 and provides three endpoints:
//   - /health: Health check endpoint
//   - /status: Proxy status with rate limit and token information
//   - /helix/*: Proxy to Twitch Helix API
func main() {
	clientID := os.Getenv("TWITCH_CLIENT_ID")
	clientSecret := os.Getenv("TWITCH_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		log.Fatal("❌ TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set")
	}

	// Root context — cancelled on OS signal to trigger graceful shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	serverContext = ctx

	proxy := NewTwitchProxy(clientID, clientSecret)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", HealthCheck)
	mux.HandleFunc("/status", StatusHandler(proxy))
	mux.Handle("/helix/", proxy)

	srv := &http.Server{
		Addr:    ":3000",
		Handler: mux,
	}

	log.Printf("🚀 Twitch proxy running on http://localhost%s", srv.Addr)
	log.Printf("📡 Endpoint: http://localhost%s/helix/...", srv.Addr)
	log.Printf("📊 Status: http://localhost%s/status", srv.Addr)
	log.Printf("🔑 Auth: Client Credentials OAuth with automatic renewal")
	log.Printf("⚡ Rate limiting synced with Twitch Ratelimit-* headers")

	// Run server in a goroutine so we can wait for shutdown signal.
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("❌ Server error: %v", err)
		}
	}()

	// Block until SIGINT/SIGTERM.
	<-ctx.Done()
	log.Printf("⏹️  Shutdown signal received, draining requests...")

	// Give in-flight requests up to 15 s to complete.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("❌ Graceful shutdown error: %v", err)
	}

	log.Printf("✅ Server stopped cleanly")
}
