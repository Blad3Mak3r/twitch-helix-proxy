package main

import (
	"net/http"
	"time"
)

func ensureAccessToken() string {
	if authToken.accessToken != "" && authToken.refreshAt > time.Now().Unix() {
		return authToken.accessToken
	}

	req, err := http.NewRequest("POST", "https://id.twitch.tv/oauth2/token", nil)
	if err != nil {
		panic("Failed to create request for access token: " + err.Error())
	}

	q := req.URL.Query()
	q.Add("client_id", clientCredentials.clientId)
	q.Add("client_secret", clientCredentials.clientSecret)
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

	authToken.accessToken = tokenResponse.AccessToken
	authToken.refreshAt = time.Now().Unix() + tokenResponse.ExpiresIn - 60 // Refresh 1 minute before expiry
	return authToken.accessToken

}
