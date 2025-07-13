package main

import (
	"net/http"
	"os"
)

var (
	authToken         AuthToken
	clientCredentials ClientCredentials
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
	server := http.NewServeMux()
	server.HandleFunc("/helix/{path...}", func(w http.ResponseWriter, r *http.Request) {
		// Handle proxy request here
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Proxy endpoint is working"))
	})
}
