package main

import (
	"log"
	"net/http"
	"os"
)

func main() {
	clientID := os.Getenv("TWITCH_CLIENT_ID")
	clientSecret := os.Getenv("TWITCH_CLIENT_SECRET")

	if clientID == "" || clientSecret == "" {
		log.Fatal("❌ TWITCH_CLIENT_ID and TWITCH_CLIENT_SECRET must be set")
	}

	proxy := NewTwitchProxy(clientID, clientSecret)

	http.HandleFunc("/health", HealthCheck)
	http.HandleFunc("/status", StatusHandler(proxy))
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
