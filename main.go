package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"
)

// TwitchRateLimiter con detección de versión basada en headers
type TwitchRateLimiter struct {
	mu              sync.RWMutex
	tokensRemaining int
	resetTime       time.Time
	bucketCapacity  int
	minBuffer       int

	// Tracking para detectar respuestas viejas
	lastResetTime   int64 // Unix timestamp del último Ratelimit-Reset visto
	lowestRemaining int   // El valor más bajo visto en el bucket actual
}

func NewTwitchRateLimiter() *TwitchRateLimiter {
	now := time.Now()
	return &TwitchRateLimiter{
		tokensRemaining: 800,
		resetTime:       now.Add(time.Minute),
		bucketCapacity:  800,
		minBuffer:       50,
		lastResetTime:   now.Add(time.Minute).Unix(),
		lowestRemaining: 800,
	}
}

// UpdateFromHeaders actualiza solo si los datos son más recientes
func (rl *TwitchRateLimiter) UpdateFromHeaders(remaining, limit, reset string) {
	if remaining == "" || reset == "" {
		return
	}

	rem, err := strconv.Atoi(remaining)
	if err != nil {
		return
	}

	resetUnix, err := strconv.ParseInt(reset, 10, 64)
	if err != nil {
		return
	}

	rl.mu.Lock()
	defer rl.mu.Unlock()

	// Caso 1: Nueva ventana de rate limit (el reset time cambió)
	if resetUnix > rl.lastResetTime {
		log.Printf("🔄 Nuevo bucket detectado: reset %d → %d", rl.lastResetTime, resetUnix)
		rl.lastResetTime = resetUnix
		rl.resetTime = time.Unix(resetUnix, 0)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem

		if limit != "" {
			if lim, err := strconv.Atoi(limit); err == nil {
				rl.bucketCapacity = lim
			}
		}

		log.Printf("📊 Bucket reseteado: %d/%d tokens disponibles", rem, rl.bucketCapacity)
		return
	}

	// Caso 2: Misma ventana, pero respuesta de request más reciente (remaining menor)
	if resetUnix == rl.lastResetTime && rem < rl.lowestRemaining {
		log.Printf("🔽 Actualización válida: tokens %d → %d (mismo bucket)",
			rl.tokensRemaining, rem)
		rl.tokensRemaining = rem
		rl.lowestRemaining = rem
		return
	}

	// Caso 3: Respuesta vieja (remaining mayor que el mínimo visto)
	if resetUnix == rl.lastResetTime && rem > rl.lowestRemaining {
		log.Printf("⏪ Respuesta vieja ignorada: remaining=%d (actual=%d)",
			rem, rl.lowestRemaining)
		return
	}

	// Caso 4: Respuesta de bucket anterior (resetUnix < rl.lastResetTime)
	if resetUnix < rl.lastResetTime {
		log.Printf("⏪ Respuesta de bucket antiguo ignorada (reset %d < %d)",
			resetUnix, rl.lastResetTime)
		return
	}
}

// Acquire usa RWMutex para permitir lecturas concurrentes
func (rl *TwitchRateLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		rl.mu.RLock()

		// Check si el bucket se reseteó
		now := time.Now()
		if now.After(rl.resetTime) {
			rl.mu.RUnlock()

			// Upgrade a write lock para resetear
			rl.mu.Lock()
			// Double-check después de adquirir write lock
			if time.Now().After(rl.resetTime) {
				rl.tokensRemaining = rl.bucketCapacity
				rl.resetTime = time.Now().Add(time.Minute)
				rl.lastResetTime = rl.resetTime.Unix()
				rl.lowestRemaining = rl.bucketCapacity
				log.Printf("🔄 Bucket auto-reseteado: %d tokens", rl.bucketCapacity)
			}
			rl.mu.Unlock()
			continue
		}

		// Si hay tokens suficientes, permitir
		if rl.tokensRemaining > rl.minBuffer {
			rl.mu.RUnlock()

			// Decrementar con write lock
			rl.mu.Lock()
			// Double-check que todavía hay tokens
			if rl.tokensRemaining > rl.minBuffer {
				rl.tokensRemaining--
				rl.lowestRemaining = rl.tokensRemaining
				rl.mu.Unlock()
				return nil
			}
			rl.mu.Unlock()
			continue
		}

		// No hay tokens, calcular espera
		waitUntil := rl.resetTime
		tokensLeft := rl.tokensRemaining
		rl.mu.RUnlock()

		waitDuration := time.Until(waitUntil)
		if waitDuration < 0 {
			continue // El bucket debería resetearse, reintentar
		}

		log.Printf("⏸️  Rate limit: %d tokens restantes, esperando %.1fs hasta reset (%s)",
			tokensLeft, waitDuration.Seconds(), waitUntil.Format("15:04:05"))

		// Esperar con cancelación
		timer := time.NewTimer(waitDuration)
		select {
		case <-timer.C:
			continue
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// GetStatus retorna el estado actual
func (rl *TwitchRateLimiter) GetStatus() (remaining int, resetIn time.Duration) {
	rl.mu.RLock()
	defer rl.mu.RUnlock()
	return rl.tokensRemaining, time.Until(rl.resetTime)
}

// TwitchProxy maneja el proxying con rate limiting concurrente
type TwitchProxy struct {
	clientID    string
	token       string
	rateLimiter *TwitchRateLimiter
	client      *http.Client
	targetURL   *url.URL
}

func NewTwitchProxy(clientID, token string) *TwitchProxy {
	targetURL, _ := url.Parse("https://api.twitch.tv")

	return &TwitchProxy{
		clientID:    clientID,
		token:       token,
		rateLimiter: NewTwitchRateLimiter(),
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		targetURL: targetURL,
	}
}

func (tp *TwitchProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Rate limiting (CONCURRENTE - no serializado)
	if err := tp.rateLimiter.Acquire(r.Context()); err != nil {
		http.Error(w, "Request cancelled", http.StatusRequestTimeout)
		return
	}

	// Construir URL de Twitch
	targetURL := *tp.targetURL
	targetURL.Path = r.URL.Path
	targetURL.RawQuery = r.URL.RawQuery

	log.Printf("🔄 %s %s", r.Method, targetURL.String())

	// Leer body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Hacer petición con reintentos
	maxRetries := 3
	for retry := 0; retry <= maxRetries; retry++ {
		proxyReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL.String(), nil)
		if err != nil {
			http.Error(w, "Error creating request", http.StatusInternalServerError)
			return
		}

		if len(bodyBytes) > 0 {
			proxyReq.Body = io.NopCloser(io.Reader(io.LimitReader(io.MultiReader(), int64(len(bodyBytes)))))
			proxyReq.ContentLength = int64(len(bodyBytes))
		}

		// Copiar headers
		for key, values := range r.Header {
			if key != "Host" && key != "Authorization" && key != "Client-Id" {
				for _, value := range values {
					proxyReq.Header.Add(key, value)
				}
			}
		}

		// Autenticación
		proxyReq.Header.Set("Client-Id", tp.clientID)
		proxyReq.Header.Set("Authorization", "Bearer "+tp.token)

		// Ejecutar petición
		startTime := time.Now()
		resp, err := tp.client.Do(proxyReq)
		if err != nil {
			http.Error(w, "Error connecting to Twitch", http.StatusBadGateway)
			return
		}
		latency := time.Since(startTime)

		// Leer headers de rate limit
		rateLimitLimit := resp.Header.Get("Ratelimit-Limit")
		rateLimitRemaining := resp.Header.Get("Ratelimit-Remaining")
		rateLimitReset := resp.Header.Get("Ratelimit-Reset")

		// Actualizar rate limiter (con detección de versión automática)
		tp.rateLimiter.UpdateFromHeaders(rateLimitRemaining, rateLimitLimit, rateLimitReset)

		// Log con latencia
		if rateLimitRemaining != "" {
			remaining, _ := strconv.Atoi(rateLimitRemaining)
			log.Printf("📊 [%dms] Rate limit: %s/%s tokens (reset: %s)",
				latency.Milliseconds(), rateLimitRemaining, rateLimitLimit, rateLimitReset)

			if remaining < 100 {
				log.Printf("⚠️  Solo quedan %d tokens", remaining)
			}
		}

		// Manejo de 429
		if resp.StatusCode == http.StatusTooManyRequests {
			resp.Body.Close()

			if retry >= maxRetries {
				log.Printf("❌ Rate limit 429 después de %d reintentos", maxRetries)
				w.Header().Set("Retry-After", rateLimitReset)
				http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
				return
			}

			var waitDuration time.Duration
			if rateLimitReset != "" {
				resetTime, err := strconv.ParseInt(rateLimitReset, 10, 64)
				if err == nil {
					waitUntil := time.Unix(resetTime, 0)
					waitDuration = time.Until(waitUntil)
					if waitDuration < 0 {
						waitDuration = time.Second
					}
				} else {
					waitDuration = time.Second * time.Duration(2<<uint(retry))
				}
			} else {
				waitDuration = time.Second * time.Duration(2<<uint(retry))
			}

			log.Printf("❌ 429 (rate limiting preventivo falló) - Esperando %.1fs",
				waitDuration.Seconds())

			// Forzar actualización a 0
			tp.rateLimiter.UpdateFromHeaders("0", rateLimitLimit, rateLimitReset)

			time.Sleep(waitDuration)
			continue
		}

		// Respuesta exitosa
		for key, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}

		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		resp.Body.Close()

		return
	}
}

func healthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"status":"ok"}`)
}

func statusHandler(proxy *TwitchProxy) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		remaining, resetIn := proxy.rateLimiter.GetStatus()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"tokens_remaining":%d,"reset_in_seconds":%.1f}`,
			remaining, resetIn.Seconds())
	}
}

func main() {
	clientID := os.Getenv("TWITCH_CLIENT_ID")
	token := os.Getenv("TWITCH_TOKEN")

	if clientID == "" || token == "" {
		log.Fatal("❌ TWITCH_CLIENT_ID y TWITCH_TOKEN deben estar configurados")
	}

	proxy := NewTwitchProxy(clientID, token)

	http.HandleFunc("/health", healthCheck)
	http.HandleFunc("/status", statusHandler(proxy))
	http.Handle("/helix/", proxy)

	addr := ":3000"
	log.Printf("🚀 Proxy de Twitch corriendo en http://localhost%s", addr)
	log.Printf("📡 Endpoint: http://localhost%s/helix/...", addr)
	log.Printf("📊 Status: http://localhost%s/status", addr)
	log.Printf("⚡ Rate limiting CONCURRENTE con detección de versión")
	log.Printf("🛡️  Buffer: 50 tokens | Detección automática de respuestas viejas")

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}
