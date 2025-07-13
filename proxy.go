package main

import (
	"io"
	"log"
	"net/http"
	"time"
)

func handleProxyRequest(w http.ResponseWriter, r *http.Request) {
	rateMu.Lock()
	if time.Now().Before(rateReset) && rateRemaining <= 0 {
		wait := time.Until(rateReset)
		log.Printf("Rate limit exceeded. Waiting %v", wait)
		rateMu.Unlock()
		time.Sleep(wait)
		rateMu.Lock()
	}
	rateRemaining--
	rateMu.Unlock()

	twitchReq, err := http.NewRequest(r.Method, "https://api.twitch.tv"+r.URL.Path, r.Body)
	if err != nil {
		http.Error(w, "failed to create request", http.StatusInternalServerError)
		return
	}
	twitchReq.Header.Set("Client-Id", clientCredentials.clientId)
	twitchReq.Header.Set("Authorization", "Bearer "+authToken.accessToken)
	copyHeaders(twitchReq.Header, r.Header)

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
}

func copyHeaders(dst, src http.Header) {
	for k, vv := range src {
		for _, v := range vv {
			dst.Add(k, v)
		}
	}
}
