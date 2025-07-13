package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

var (
	state *AppState

	port int
)

func init() {
	err := godotenv.Load(".env")
	if err != nil {
		panic("Error loading .env")
	}
	if envPort, exists := os.LookupEnv("PORT"); exists == true {
		if parsedPort, err := strconv.Atoi(envPort); err != nil {
			panic("Error parsing port")
		} else {
			port = parsedPort
		}
	} else {
		port = 4200
	}

	state = &AppState{
		Credentials: ClientCredentials{
			ClientId:     os.Getenv("TWITCH_CLIENT_ID"),
			ClientSecret: os.Getenv("TWITCH_CLIENT_SECRET"),
		},
	}

	if state.Credentials.ClientId == "" || state.Credentials.ClientSecret == "" {
		panic("Twitch client ID and secret must be set in environment variables")
	}

	EnsureAccessToken()
}

func main() {
	http.HandleFunc("/", HandleProxyRequest)

	log.Printf("Proxy listening on :%d\n", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", port), nil))
}
