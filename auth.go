package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// TwitchAuthManager handles OAuth client-credentials token lifecycle.
// It implements TokenProvider and is safe for concurrent use.
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

// TwitchError represents an error returned by the Twitch API.
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
// The provided ctx controls the lifetime of the refresh goroutine.
func NewTwitchAuthManager(ctx context.Context, clientID, clientSecret string) *TwitchAuthManager {
	am := &TwitchAuthManager{
		clientID:     clientID,
		clientSecret: clientSecret,
		client:       &http.Client{Timeout: 10 * time.Second},
	}

	if err := am.refreshToken(ctx); err != nil {
		logger.Error("failed to obtain initial token", "error", err)
		panic(fmt.Sprintf("fatal: %v", err))
	}

	go am.autoRefresh(ctx)
	return am
}

// refreshToken fetches a fresh access token from the Twitch OAuth endpoint.
func (am *TwitchAuthManager) refreshToken(ctx context.Context) error {
	logger.Info("requesting new access token")

	reqCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	data := url.Values{}
	data.Set("client_id", am.clientID)
	data.Set("client_secret", am.clientSecret)
	data.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		"https://id.twitch.tv/oauth2/token", strings.NewReader(data.Encode()))
	if err != nil {
		return fmt.Errorf("failed to create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := am.client.Do(req)
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return &TwitchError{Status: http.StatusInternalServerError, Message: "failed to read token response", Err: err}
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
		return fmt.Errorf("failed to decode token response: %w", err)
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

	logger.Info("access token obtained",
		"expires_in", expiryDuration.String(),
		"renewal_in", time.Until(am.expiresAt).Round(time.Second).String())

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
			logger.Info("auth manager shutting down")
			return
		case <-ticker.C:
		}

		am.mu.RLock()
		timeUntilRenewal := time.Until(am.expiresAt)
		am.mu.RUnlock()

		if timeUntilRenewal > 10*time.Minute {
			logger.Debug("token renewal not due yet",
				"renewal_in", timeUntilRenewal.Round(time.Minute).String())
			continue
		}

		logger.Info("token renewal triggered",
			"renewal_in", timeUntilRenewal.Round(time.Second).String())

		if err := am.refreshToken(ctx); err != nil {
			logger.Error("token renewal failed, retrying",
				"error", err,
				"backoff", backoff.String())

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				logger.Info("auth manager shutting down during backoff")
				return
			case <-timer.C:
			}

			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = time.Second
		logger.Info("token renewed successfully")
	}
}

// GetAccessToken implements TokenProvider. It returns the current cached token.
// Optimised for high-throughput — uses RLock only.
func (am *TwitchAuthManager) GetAccessToken() string {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return am.accessToken
}

// TokenExpiresIn implements TokenProvider. It returns the duration until the
// next scheduled renewal deadline.
func (am *TwitchAuthManager) TokenExpiresIn() time.Duration {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return time.Until(am.expiresAt)
}

// ForceRefresh implements TokenProvider. It forces an immediate token refresh,
// typically called after the Twitch API returns a 401.
func (am *TwitchAuthManager) ForceRefresh(ctx context.Context) error {
	logger.Info("forcing token refresh due to API rejection")
	return am.refreshToken(ctx)
}
