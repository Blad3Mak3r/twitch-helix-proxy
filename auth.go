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

// TwitchAuthManager handles authentication and token renewal
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

// TwitchError representa un error específico de la API de Twitch
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

// NewTwitchAuthManager creates a new auth manager instance
func NewTwitchAuthManager(clientID, clientSecret string) *TwitchAuthManager {
	am := &TwitchAuthManager{
		clientID:     clientID,
		clientSecret: clientSecret,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}

	// Get initial token
	if err := am.refreshToken(); err != nil {
		log.Fatalf("❌ Error obtaining initial token: %v", err)
	}

	// Start automatic renewal goroutine
	go am.autoRefresh()

	return am
}

// refreshToken obtains a new access token
func (am *TwitchAuthManager) refreshToken() error {
	log.Printf("🔑 Requesting new access token...")

	// Crear un contexto con timeout
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	data := url.Values{}
	data.Set("client_id", am.clientID)
	data.Set("client_secret", am.clientSecret)
	data.Set("grant_type", "client_credentials")

	req, err := http.NewRequestWithContext(ctx, "POST", "https://id.twitch.tv/oauth2/token", strings.NewReader(data.Encode()))
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
		// Intentar decodificar el error de Twitch
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

	// Validar la respuesta
	if tokenResp.AccessToken == "" {
		return fmt.Errorf("received empty access token")
	}
	if tokenResp.ExpiresIn <= 0 {
		return fmt.Errorf("received invalid expiry time: %d", tokenResp.ExpiresIn)
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	// Calculate actual expiry time
	expiryDuration := time.Duration(tokenResp.ExpiresIn) * time.Second
	actualExpiry := time.Now().Add(expiryDuration)

	// Set a minimum renewal buffer of 1 minute and maximum of 1 hour
	renewBuffer := expiryDuration / 10 // 10% del tiempo total
	if renewBuffer < time.Minute {
		renewBuffer = time.Minute
	} else if renewBuffer > time.Hour {
		renewBuffer = time.Hour
	}

	// Update token info
	am.accessToken = tokenResp.AccessToken
	am.expiresAt = actualExpiry.Add(-renewBuffer)

	timeUntilRenewal := time.Until(am.expiresAt)
	timeUntilExpiry := time.Until(actualExpiry)

	log.Printf("✅ Token obtained successfully")
	log.Printf("   Expires in:  %.1f minutes", timeUntilExpiry.Minutes())
	log.Printf("   Renewal in:  %.1f minutes (%.1f minutes before expiry)",
		timeUntilRenewal.Minutes(), renewBuffer.Minutes())

	return nil
}

// autoRefresh automatically renews the token before it expires
func (am *TwitchAuthManager) autoRefresh() {
	// Usar un ticker para verificaciones periódicas
	const checkInterval = 30 * time.Second
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	backoff := time.Second
	maxBackoff := time.Minute * 5

	for {
		<-ticker.C

		am.mu.RLock()
		timeUntilExpiry := time.Until(am.expiresAt)
		am.mu.RUnlock()

		if timeUntilExpiry <= checkInterval {
			// Token próximo a expirar o ya expirado
			if err := am.refreshToken(); err != nil {
				log.Printf("❌ Error renewing token: %v. Retrying in %v...", err, backoff)
				time.Sleep(backoff)

				// Incrementar backoff exponencialmente
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}

			// Éxito - resetear backoff
			backoff = time.Second
		} else {
			// Todo está bien, mostrar próxima renovación
			log.Printf("⏰ Next token check in %.1f minutes", checkInterval.Minutes())
		}
	}
}

// GetAccessToken returns the current valid token (thread-safe)
func (am *TwitchAuthManager) GetAccessToken() (string, error) {
	am.mu.RLock()
	token := am.accessToken
	am.mu.RUnlock()

	// Validate token before returning
	if err := am.ValidateToken(); err != nil {
		// Token is invalid, try to refresh
		if err := am.refreshToken(); err != nil {
			return "", fmt.Errorf("failed to refresh invalid token: %w", err)
		}
		// Get new token after refresh
		am.mu.RLock()
		token = am.accessToken
		am.mu.RUnlock()
	}

	return token, nil
}

// ValidateToken verifies if the current token is valid
func (am *TwitchAuthManager) ValidateToken() error {
	am.mu.RLock()
	token := am.accessToken
	am.mu.RUnlock()

	req, err := http.NewRequest("GET", "https://id.twitch.tv/oauth2/validate", nil)
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
