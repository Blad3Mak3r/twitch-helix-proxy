package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TwitchAuthManager handles OAuth client-credentials token lifecycle.
// It is safe for concurrent use — all exported methods acquire the mutex.
type TwitchAuthManager struct {
	mu           sync.RWMutex
	clientID     string
	clientSecret string
	accessToken  string
	expiresAt    time.Time
	client       *http.Client
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
	TokenType   string `json:"token_type"`
}

// TwitchError represents a specific Twitch API error.
type TwitchError struct {
	Status  int
	Message string
	Err     error
}

func (e *TwitchError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("twitch error (status %d): %s - %v", e.Status, e.Message, e.Err)
	}
	return fmt.Sprintf("twitch error (status %d): %s", e.Status, e.Message)
}

func (e *TwitchError) Unwrap() error {
	return e.Err
}

// NewTwitchAuthManager creates a new auth manager, fetches the initial token,
// and starts the background auto-refresh goroutine.
// The provided ctx controls the lifetime of the refresh goroutine — cancel it
// to perform a clean shutdown.
func NewTwitchAuthManager(ctx context.Context, clientID, clientSecret string) *TwitchAuthManager {
	am := &TwitchAuthManager{
		clientID:     clientID,
		clientSecret: clientSecret,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	if err := am.refreshToken(ctx); err != nil {
		log.Fatalf("❌ Error obtaining initial token: %v", err)
	}

	go am.autoRefresh(ctx)

	return am
}

// refreshToken fetches a fresh access token from the Twitch OAuth endpoint.
// It is safe to call concurrently — the mutex is held only while writing.
func (am *TwitchAuthManager) refreshToken(ctx context.Context) error {
	log.Printf("🔑 Requesting new access token...")

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	data := url.Values{}
	data.Set("client_id", am.clientID)
	data.Set("client_secret", am.clientSecret)
	data.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		"https://id.twitch.tv/oauth2/token", strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := am.client.Do(req)
	if err != nil {
		return fmt.Errorf("request error: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &TwitchError{Status: http.StatusInternalServerError, Message: "failed to read response body", Err: err}
	}

	if resp.StatusCode != http.StatusOK {
		var twitchErr struct {
			Status  int    `json:"status"`
			Message string `json:"message"`
		}
		if err := json.Unmarshal(body, &twitchErr); err == nil && twitchErr.Message != "" {
			return &TwitchError{Status: resp.StatusCode, Message: twitchErr.Message}
		}
		return &TwitchError{Status: resp.StatusCode, Message: string(body)}
	}

	var tokenResp tokenResponse
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return fmt.Errorf("received empty access token")
	}
	if tokenResp.ExpiresIn <= 0 {
		return fmt.Errorf("received invalid expiry time: %d", tokenResp.ExpiresIn)
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	expiryDuration := time.Duration(tokenResp.ExpiresIn) * time.Second
	actualExpiry := time.Now().Add(expiryDuration)

	var renewBuffer time.Duration
	if expiryDuration > time.Hour {
		renewBuffer = 30 * time.Minute
	} else {
		renewBuffer = max(expiryDuration/10, time.Minute)
	}

	am.accessToken = tokenResp.AccessToken
	am.expiresAt = actualExpiry.Add(-renewBuffer)

	timeUntilRenewal := time.Until(am.expiresAt)
	timeUntilExpiry := time.Until(actualExpiry)

	var expiryMsg, renewalMsg string
	switch {
	case timeUntilExpiry > time.Hour*24:
		expiryMsg = fmt.Sprintf("%.1f días", timeUntilExpiry.Hours()/24)
	case timeUntilExpiry > time.Hour:
		expiryMsg = fmt.Sprintf("%.1f horas", timeUntilExpiry.Hours())
	default:
		expiryMsg = fmt.Sprintf("%.1f minutos", timeUntilExpiry.Minutes())
	}

	if timeUntilRenewal > time.Hour {
		renewalMsg = fmt.Sprintf("%.1f horas", timeUntilRenewal.Hours())
	} else {
		renewalMsg = fmt.Sprintf("%.1f minutos", timeUntilRenewal.Minutes())
	}

	log.Printf("✅ Token obtained successfully")
	log.Printf("   Expires in: %s", expiryMsg)
	log.Printf("   Renewal in: %s", renewalMsg)

	return nil
}

// autoRefresh polls every 5 minutes and renews the token when it is within
// 10 minutes of the renewal deadline. It exits cleanly when ctx is cancelled.
func (am *TwitchAuthManager) autoRefresh(ctx context.Context) {
	const checkInterval = 5 * time.Minute

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	backoff := time.Second
	const maxBackoff = 5 * time.Minute

	for {
		select {
		case <-ctx.Done():
			log.Printf("🔑 Auth manager shutting down")
			return
		case <-ticker.C:
		}

		am.mu.RLock()
		timeUntilExpiry := time.Until(am.expiresAt)
		am.mu.RUnlock()

		if timeUntilExpiry > 10*time.Minute {
			if timeUntilExpiry > 24*time.Hour {
				log.Printf("⏰ Next token check in %.1f minutes (token expires in %.1f days)",
					checkInterval.Minutes(), timeUntilExpiry.Hours()/24)
			} else {
				log.Printf("⏰ Next token check in %.1f minutes (token expires in %.1f hours)",
					checkInterval.Minutes(), timeUntilExpiry.Hours())
			}
			continue
		}

		log.Printf("🔑 Token renewal triggered (expires in %.1f minutes)", timeUntilExpiry.Minutes())

		if err := am.refreshToken(ctx); err != nil {
			log.Printf("❌ Error renewing token: %v. Retrying in %v...", err, backoff)

			// Wait with backoff, but honour context cancellation.
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				log.Printf("🔑 Auth manager shutting down during backoff")
				return
			case <-timer.C:
			}

			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = time.Second
		log.Printf("✅ Token renewed successfully")
	}
}

// GetAccessToken returns the current cached token.
// Optimised for high-throughput — uses RLock only.
func (am *TwitchAuthManager) GetAccessToken() string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.accessToken
}

// TokenExpiresIn returns the duration until the token renewal deadline.
// Used by the /status endpoint to avoid exposing the mutex.
func (am *TwitchAuthManager) TokenExpiresIn() time.Duration {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return time.Until(am.expiresAt)
}

// ForceTokenRefresh forces a token refresh.
// Use this only when the Twitch API returns 401.
func (am *TwitchAuthManager) ForceTokenRefresh(ctx context.Context) error {
	log.Printf("🔑 Forcing token refresh due to API rejection...")
	return am.refreshToken(ctx)
}

// ValidateToken verifies the current token against the Twitch introspection endpoint.
func (am *TwitchAuthManager) ValidateToken(ctx context.Context) error {
	am.mu.RLock()
	token := am.accessToken
	am.mu.RUnlock()

	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "https://id.twitch.tv/oauth2/validate", nil)
	if err != nil {
		return fmt.Errorf("failed to create validation request: %w", err)
	}
	req.Header.Set("Authorization", "OAuth "+token)

	resp, err := am.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute validation request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read validation response: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		log.Printf("✅ Token validated successfully")
		return nil
	case http.StatusUnauthorized:
		return &TwitchError{Status: resp.StatusCode, Message: "token is invalid or expired"}
	case http.StatusForbidden:
		return &TwitchError{Status: resp.StatusCode, Message: "client credentials are invalid"}
	default:
		return &TwitchError{Status: resp.StatusCode, Message: string(body)}
	}
}
