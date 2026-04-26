package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// ErrDailyLimitExceeded is returned by DailyUniqueCounterLimiter when the
// maximum number of unique recipients for the current UTC day has been reached.
// The proxy maps this to a 429 response rather than blocking indefinitely.
var ErrDailyLimitExceeded = errors.New("daily unique recipient limit exceeded")

// -----------------------------------------------------------------------------
// TokenBucketLimiter
// -----------------------------------------------------------------------------

// TokenBucketLimiter is a simple local token bucket with a fixed capacity and
// window. It does not use Twitch response headers — its state is entirely local.
// Use this for endpoints with a documented N-requests-per-window fixed limit.
type TokenBucketLimiter struct {
	mu       sync.Mutex
	name     string
	tokens   int
	capacity int
	window   time.Duration
	resetAt  time.Time
}

// NewTokenBucketLimiter creates a full bucket that resets every window.
func NewTokenBucketLimiter(name string, capacity int, window time.Duration) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		name:     name,
		tokens:   capacity,
		capacity: capacity,
		window:   window,
		resetAt:  time.Now().Add(window),
	}
}

// Acquire implements Limiter. It blocks until a token is available or ctx is cancelled.
func (t *TokenBucketLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		t.mu.Lock()
		now := time.Now()
		if now.After(t.resetAt) {
			t.tokens = t.capacity
			t.resetAt = now.Add(t.window)
		}
		if t.tokens > 0 {
			t.tokens--
			t.mu.Unlock()
			return nil
		}
		waitUntil := t.resetAt
		t.mu.Unlock()

		wait := time.Until(waitUntil)
		if wait <= 0 {
			continue
		}

		logger.Warn("endpoint rate limit exhausted, waiting for reset",
			"limiter", t.name,
			"wait_seconds", wait.Seconds())

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// UpdateFromHeaders implements Limiter as a no-op — this limiter is local-only.
func (t *TokenBucketLimiter) UpdateFromHeaders(_ http.Header) {}

// Status implements Limiter.
func (t *TokenBucketLimiter) Status() LimiterStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return LimiterStatus{
		Name:            t.name,
		TokensRemaining: t.tokens,
		ResetIn:         time.Until(t.resetAt),
	}
}

// -----------------------------------------------------------------------------
// SlidingWindowLimiter
// -----------------------------------------------------------------------------

// SlidingWindowLimiter enforces a maximum number of requests within a rolling
// time window using a timestamp slice. It is suitable for short windows (≤ 2 min).
// Use this for endpoints like Add/Remove Moderator (10 per 10 s) and Shoutout (1 per 2 min).
type SlidingWindowLimiter struct {
	mu         sync.Mutex
	name       string
	limit      int
	window     time.Duration
	timestamps []time.Time
}

// NewSlidingWindowLimiter creates a sliding window limiter.
func NewSlidingWindowLimiter(name string, limit int, window time.Duration) *SlidingWindowLimiter {
	return &SlidingWindowLimiter{
		name:       name,
		limit:      limit,
		window:     window,
		timestamps: make([]time.Time, 0, limit),
	}
}

// Acquire implements Limiter. It blocks until there is room in the window or ctx is cancelled.
func (s *SlidingWindowLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		s.mu.Lock()
		now := time.Now()
		cutoff := now.Add(-s.window)

		// Expire timestamps that have fallen outside the window.
		valid := s.timestamps[:0]
		for _, ts := range s.timestamps {
			if ts.After(cutoff) {
				valid = append(valid, ts)
			}
		}
		s.timestamps = valid

		if len(s.timestamps) < s.limit {
			s.timestamps = append(s.timestamps, now)
			s.mu.Unlock()
			return nil
		}

		// Wait until the oldest timestamp slides out of the window.
		oldest := s.timestamps[0]
		waitUntil := oldest.Add(s.window)
		s.mu.Unlock()

		wait := time.Until(waitUntil)
		if wait <= 0 {
			continue
		}

		logger.Warn("sliding window rate limit reached, waiting",
			"limiter", s.name,
			"wait_seconds", wait.Seconds())

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// UpdateFromHeaders implements Limiter as a no-op — this limiter is local-only.
func (s *SlidingWindowLimiter) UpdateFromHeaders(_ http.Header) {}

// Status implements Limiter.
func (s *SlidingWindowLimiter) Status() LimiterStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	remaining := s.limit - len(s.timestamps)
	if remaining < 0 {
		remaining = 0
	}
	var resetIn time.Duration
	if len(s.timestamps) > 0 {
		resetIn = time.Until(s.timestamps[0].Add(s.window))
	}
	return LimiterStatus{
		Name:            s.name,
		TokensRemaining: remaining,
		ResetIn:         resetIn,
	}
}

// -----------------------------------------------------------------------------
// CooldownLimiter
// -----------------------------------------------------------------------------

// CooldownLimiter blocks requests while a cooldown period is active.
// The cooldown duration is set externally after reading the upstream response
// body — it is not driven by Twitch rate-limit headers.
//
// Use this for endpoints where the cooldown comes from the response body, such
// as POST /helix/channels/commercial (retry_after field).
//
// CooldownLimiter implements both Limiter and ResponseBodyHandler.
type CooldownLimiter struct {
	mu        sync.Mutex
	name      string
	coolUntil time.Time
}

// NewCooldownLimiter creates a CooldownLimiter with no active cooldown.
func NewCooldownLimiter(name string) *CooldownLimiter {
	return &CooldownLimiter{name: name}
}

// SetCooldown activates a cooldown for the given duration from now.
func (c *CooldownLimiter) SetCooldown(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.coolUntil = time.Now().Add(d)
	logger.Info("cooldown activated",
		"limiter", c.name,
		"duration_seconds", d.Seconds())
}

// Acquire implements Limiter. It blocks while a cooldown is active.
func (c *CooldownLimiter) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		c.mu.Lock()
		now := time.Now()
		if now.After(c.coolUntil) {
			c.mu.Unlock()
			return nil
		}
		waitUntil := c.coolUntil
		c.mu.Unlock()

		wait := time.Until(waitUntil)
		if wait <= 0 {
			continue
		}

		logger.Warn("cooldown active, waiting",
			"limiter", c.name,
			"wait_seconds", wait.Seconds())

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		}
	}
}

// UpdateFromHeaders implements Limiter as a no-op.
func (c *CooldownLimiter) UpdateFromHeaders(_ http.Header) {}

// Status implements Limiter.
func (c *CooldownLimiter) Status() LimiterStatus {
	c.mu.Lock()
	defer c.mu.Unlock()
	resetIn := time.Until(c.coolUntil)
	if resetIn < 0 {
		resetIn = 0
	}
	return LimiterStatus{
		Name:    c.name,
		ResetIn: resetIn,
	}
}

// HandleResponseBody implements ResponseBodyHandler.
// It parses the retry_after field from a Start Commercial response and activates
// the cooldown accordingly.
func (c *CooldownLimiter) HandleResponseBody(body []byte) {
	var resp struct {
		Data []struct {
			RetryAfter int `json:"retry_after"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Data) == 0 {
		return
	}
	if secs := resp.Data[0].RetryAfter; secs > 0 {
		c.SetCooldown(time.Duration(secs) * time.Second)
	}
}

// -----------------------------------------------------------------------------
// DailyUniqueCounterLimiter
// -----------------------------------------------------------------------------

// DailyUniqueCounterLimiter tracks unique values of a query parameter within a
// UTC calendar day. If the number of unique values reaches the configured limit
// it returns ErrDailyLimitExceeded immediately (rather than blocking until midnight).
//
// Use this for POST /helix/whispers to enforce the 40 unique recipients/day limit.
type DailyUniqueCounterLimiter struct {
	mu        sync.Mutex
	name      string
	limit     int
	paramName string
	seen      map[string]struct{}
	resetAt   time.Time
}

// NewDailyUniqueCounterLimiter creates a limiter that tracks unique values of
// paramName in query strings, resetting at midnight UTC each day.
func NewDailyUniqueCounterLimiter(name string, limit int, paramName string) *DailyUniqueCounterLimiter {
	return &DailyUniqueCounterLimiter{
		name:      name,
		limit:     limit,
		paramName: paramName,
		seen:      make(map[string]struct{}),
		resetAt:   nextMidnightUTC(time.Now()),
	}
}

// Acquire implements Limiter. It extracts the tracked parameter from the
// *http.Request stored in ctx (via ctxKeyHTTPRequest) and checks the daily limit.
// Returns ErrDailyLimitExceeded if the limit is reached.
func (d *DailyUniqueCounterLimiter) Acquire(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := time.Now()
	if now.After(d.resetAt) {
		d.seen = make(map[string]struct{})
		d.resetAt = nextMidnightUTC(now)
		logger.Info("daily unique counter reset", "limiter", d.name)
	}

	r, _ := ctx.Value(ctxKeyHTTPRequest).(*http.Request)
	if r == nil {
		return nil
	}

	id := r.URL.Query().Get(d.paramName)
	if id == "" {
		return nil
	}

	if _, exists := d.seen[id]; exists {
		// Already seen this recipient — allow the request (not a new unique).
		return nil
	}

	if len(d.seen) >= d.limit {
		logger.Warn("daily unique limit reached",
			"limiter", d.name,
			"param", d.paramName,
			"limit", d.limit,
			"resets_at", d.resetAt.Format(time.RFC3339))
		return ErrDailyLimitExceeded
	}

	d.seen[id] = struct{}{}
	return nil
}

// UpdateFromHeaders implements Limiter as a no-op.
func (d *DailyUniqueCounterLimiter) UpdateFromHeaders(_ http.Header) {}

// Status implements Limiter.
func (d *DailyUniqueCounterLimiter) Status() LimiterStatus {
	d.mu.Lock()
	defer d.mu.Unlock()
	return LimiterStatus{
		Name:            d.name,
		TokensRemaining: d.limit - len(d.seen),
		ResetIn:         time.Until(d.resetAt),
	}
}

// nextMidnightUTC returns the next midnight boundary in UTC after t.
func nextMidnightUTC(t time.Time) time.Time {
	y, m, day := t.UTC().Date()
	return time.Date(y, m, day+1, 0, 0, 0, 0, time.UTC)
}

// -----------------------------------------------------------------------------
// MultiBucketLimiter
// -----------------------------------------------------------------------------

// MultiBucketLimiter composes multiple Limiters with AND semantics:
// Acquire calls each sub-limiter in order and fails fast on the first error.
// Use this for endpoints with multiple simultaneous constraints, such as
// POST /helix/whispers (per-second + per-minute + daily unique).
type MultiBucketLimiter struct {
	name    string
	buckets []Limiter
}

// NewMultiBucketLimiter creates a composite limiter over the provided buckets.
func NewMultiBucketLimiter(name string, buckets ...Limiter) *MultiBucketLimiter {
	return &MultiBucketLimiter{name: name, buckets: buckets}
}

// Acquire implements Limiter. It calls Acquire on each bucket in order,
// returning the first error encountered.
func (m *MultiBucketLimiter) Acquire(ctx context.Context) error {
	for _, b := range m.buckets {
		if err := b.Acquire(ctx); err != nil {
			return err
		}
	}
	return nil
}

// UpdateFromHeaders implements Limiter as a no-op — sub-buckets are local-only.
func (m *MultiBucketLimiter) UpdateFromHeaders(_ http.Header) {}

// Status implements Limiter. Returns the status of the most constrained sub-bucket.
func (m *MultiBucketLimiter) Status() LimiterStatus {
	if len(m.buckets) == 0 {
		return LimiterStatus{Name: m.name}
	}
	// Return the sub-bucket with the fewest tokens remaining.
	min := m.buckets[0].Status()
	for _, b := range m.buckets[1:] {
		if s := b.Status(); s.TokensRemaining < min.TokensRemaining {
			min = s
		}
	}
	min.Name = m.name
	return min
}

// -----------------------------------------------------------------------------
// EndpointRegistry
// -----------------------------------------------------------------------------

// EndpointRegistry maps HTTP method + path prefix pairs to dedicated Limiter
// instances. The most specific (longest prefix) match wins.
type EndpointRegistry struct {
	mu      sync.RWMutex
	entries []endpointEntry
}

type endpointEntry struct {
	method  string // empty string matches any method
	prefix  string
	limiter Limiter
}

// NewEndpointRegistry creates an empty registry.
func NewEndpointRegistry() *EndpointRegistry {
	return &EndpointRegistry{}
}

// Register adds a Limiter for the given HTTP method and path prefix.
// An empty method matches any HTTP method. Entries are kept sorted by
// descending prefix length so that LimiterFor returns the most specific match.
func (r *EndpointRegistry) Register(method, prefix string, l Limiter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.entries = append(r.entries, endpointEntry{method: method, prefix: prefix, limiter: l})
	sort.Slice(r.entries, func(i, j int) bool {
		return len(r.entries[i].prefix) > len(r.entries[j].prefix)
	})
}

// LimiterFor returns the most specific Limiter registered for the given method
// and path, or nil if no entry matches.
func (r *EndpointRegistry) LimiterFor(method, path string) Limiter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		methodMatch := e.method == "" || e.method == method
		if methodMatch && strings.HasPrefix(path, e.prefix) {
			return e.limiter
		}
	}
	return nil
}
