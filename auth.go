package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

func EnsureAccessToken() string {
	if state.AccessToken != "" && state.RefreshAt > time.Now().Unix() {
		return state.AccessToken
	}

	log.Println("Fetching AccessToken...")

	req, err := http.NewRequest("POST", "https://id.twitch.tv/oauth2/token", nil)
	if err != nil {
		panic("Failed to create request for access token: " + err.Error())
	}

	q := req.URL.Query()
	q.Add("client_id", state.Credentials.ClientId)
	q.Add("client_secret", state.Credentials.ClientSecret)
	q.Add("grant_type", "client_credentials")
	req.URL.RawQuery = q.Encode()

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic("Failed to request access token: " + err.Error())
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		panic("Failed to get access token: " + resp.Status)
	}
	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&tokenResponse); err != nil {
		panic("Failed to parse token response: " + err.Error())
	}

	state.AccessToken = tokenResponse.AccessToken
	state.RefreshAt = time.Now().Unix() + tokenResponse.ExpiresIn - 60 // Refresh 1 minute before expiry

	log.Printf("New AccessToken stored!\n")

	return state.AccessToken

}
