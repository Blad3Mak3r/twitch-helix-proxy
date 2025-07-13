package main

import (
	"io"
	"log"
	"net/http"
	"time"
)

func HandleProxyRequest(w http.ResponseWriter, r *http.Request) {
	state.RateLimit.Mutex.Lock()
	if time.Now().Before(state.RateLimit.RateReset) && state.RateLimit.RateRemaining <= 0 {
		wait := time.Until(state.RateLimit.RateReset)
		log.Printf("Rate limit exceeded. Waiting %v", wait)
		state.RateLimit.Mutex.Unlock()
		time.Sleep(wait)
		state.RateLimit.Mutex.Lock()
	}
	state.RateLimit.RateRemaining--
	state.RateLimit.Mutex.Unlock()

	twitchReq, err := http.NewRequest(r.Method, "https://api.twitch.tv"+r.URL.Path, r.Body)
	if err != nil {
		http.Error(w, "failed to create request", http.StatusInternalServerError)
		return
	}
	accessToken := EnsureAccessToken()
	copyHeaders(twitchReq.Header, r.Header)
	twitchReq.Header.Set("Client-Id", state.Credentials.ClientId)
	twitchReq.Header.Set("Authorization", "Bearer "+accessToken)

	resp, err := http.DefaultClient.Do(twitchReq)
	if err != nil {
		http.Error(w, "failed to contact twitch", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	updateRateLimit(resp.Header)

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)

	log.Printf("[%s] %s - %d\n", resp.Request.Method, r.URL.Path, resp.StatusCode)
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
