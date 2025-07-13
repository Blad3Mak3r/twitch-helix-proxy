package main

import "net/http"

type AuthToken struct {
	accessToken string
	refreshAt   int64
}

type ClientCredentials struct {
	clientId     string
	clientSecret string
}

type ProxyRequest struct {
	uri    string
	client *http.Client
}
