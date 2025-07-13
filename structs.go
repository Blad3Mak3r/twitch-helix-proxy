package main

import (
	"net/http"
	"sync"
	"time"
)

type AppState struct {
	AccessToken string
	RefreshAt   int64
	Credentials ClientCredentials
	RateLimit   RateLimit
}

type ClientCredentials struct {
	ClientId     string
	ClientSecret string
}

type ProxyRequest struct {
	Uri    string
	Client *http.Client
}

type RateLimit struct {
	Mutex         sync.Mutex
	RateLimit     int
	RateRemaining int
	RateReset     time.Time
}
