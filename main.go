package main

import (
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	authToken         AuthToken
	clientCredentials ClientCredentials
	rateMu            sync.Mutex
	rateLimit         = 800 // default
	rateRemaining     = 800
	rateReset         time.Time
)

func init() {
	clientCredentials = ClientCredentials{
		clientId:     os.Getenv("TWITCH_CLIENT_ID"),
		clientSecret: os.Getenv("TWITCH_CLIENT_SECRET"),
	}

	if clientCredentials.clientId == "" || clientCredentials.clientSecret == "" {
		panic("Twitch client ID and secret must be set in environment variables")
	}
}

func main() {
	http.HandleFunc("/", handleProxyRequest)
}
