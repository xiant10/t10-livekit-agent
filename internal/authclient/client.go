// Package authclient turns a call's agent grant into short-lived tokens from
// the auth service. The agent holds its own service secret and grant IDs, and
// never a user's token. See auth-server/docs/service-api.md.
package authclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"t10-livekit-agent/internal/telemetry"
)

const (
	// renewBefore replaces a token this long before it expires, so a request
	// that has just left can't arrive with a dead one.
	renewBefore    = 60 * time.Second
	requestTimeout = 10 * time.Second
)

var (
	// ErrNoGrant means the participant joined without one, so nothing may be
	// done for them. Admission gives every user one.
	ErrNoGrant = errors.New("authclient: participant has no agent grant")
	// ErrGrantStopped means the grant was revoked, or the user lost access.
	ErrGrantStopped = errors.New("authclient: agent grant stopped")
	// ErrGrantExpired means the user has to join again for a new grant.
	ErrGrantExpired = errors.New("authclient: agent grant expired")
)

// Options configures New.
type Options struct {
	// URL is the auth service's base URL.
	URL string
	// ClientID is this service's account name.
	ClientID string
	// ClientSecret is its secret.
	ClientSecret string
}

// Client mints and caches delegated tokens, one entry per grant.
type Client struct {
	opts   Options
	client *http.Client

	mu      sync.Mutex
	tokens  map[string]*entry
	service entry
}

// entry's mu is held across a mint, so a burst on one grant makes one request
// without holding up any other grant.
type entry struct {
	mu      sync.Mutex
	token   string
	expires time.Time
}

// New builds a Client.
func New(opts Options) (*Client, error) {
	if opts.URL == "" {
		return nil, errors.New("authclient: AUTH_URL is required")
	}
	if opts.ClientID == "" {
		return nil, errors.New("authclient: AUTH_CLIENT_ID is required")
	}
	if opts.ClientSecret == "" {
		return nil, errors.New("authclient: AUTH_CLIENT_SECRET is required")
	}
	opts.URL = strings.TrimRight(opts.URL, "/")
	return &Client{opts: opts, client: &http.Client{Timeout: requestTimeout}, tokens: map[string]*entry{}}, nil
}

// Token returns a valid delegated token for grantID. Each mint rechecks the
// grant at the auth service, which is what stops a removed user's call.
func (c *Client) Token(ctx context.Context, grantID string) (string, error) {
	if grantID == "" {
		return "", ErrNoGrant
	}
	e := c.entry(grantID)
	e.mu.Lock()
	defer e.mu.Unlock()
	if time.Until(e.expires) > renewBefore {
		return e.token, nil
	}
	token, expires, err := c.mint(ctx, grantID)
	if err != nil {
		e.token, e.expires = "", time.Time{}
		return "", err
	}
	e.token, e.expires = token, expires
	return token, nil
}

// ServiceToken returns this service's own token, which speaks for no user. It's
// for t10-agent-control's routes, never for anything done during a call.
func (c *Client) ServiceToken(ctx context.Context) (string, error) {
	c.service.mu.Lock()
	defer c.service.mu.Unlock()
	if time.Until(c.service.expires) > renewBefore {
		return c.service.token, nil
	}
	token, expires, err := c.exchange(ctx, map[string]string{
		"grant_type": "client_credentials", "client_id": c.opts.ClientID, "client_secret": c.opts.ClientSecret,
	})
	if err != nil {
		return "", err
	}
	c.service.token, c.service.expires = token, expires
	return token, nil
}

func (c *Client) entry(grantID string) *entry {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	for id, e := range c.tokens {
		// A locked entry is minting, so it is in use. Expired ones aren't.
		if id != grantID && e.mu.TryLock() {
			if now.After(e.expires) {
				delete(c.tokens, id)
			}
			e.mu.Unlock()
		}
	}
	e, ok := c.tokens[grantID]
	if !ok {
		e = &entry{}
		c.tokens[grantID] = e
	}
	return e
}

func (c *Client) mint(ctx context.Context, grantID string) (string, time.Time, error) {
	return c.exchange(ctx, map[string]string{
		"grant_type": "agent_grant", "client_id": c.opts.ClientID, "client_secret": c.opts.ClientSecret, "grant_id": grantID,
	})
}

func (c *Client) exchange(ctx context.Context, fields map[string]string) (string, time.Time, error) {
	body, err := json.Marshal(fields)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("authclient: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.opts.URL+"/v1/token", bytes.NewReader(body))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("authclient: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if traceparent := telemetry.Traceparent(ctx); traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("authclient: exchange: %w", err)
	}
	defer resp.Body.Close()
	var result struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		Error       string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", time.Time{}, fmt.Errorf("authclient: decode (status %d): %w", resp.StatusCode, err)
	}
	switch {
	case resp.StatusCode == http.StatusOK && result.AccessToken != "":
		return result.AccessToken, time.Now().Add(time.Duration(result.ExpiresIn) * time.Second), nil
	case result.Error == "invalid_grant":
		return "", time.Time{}, ErrGrantStopped
	case result.Error == "grant_expired":
		return "", time.Time{}, ErrGrantExpired
	default:
		return "", time.Time{}, fmt.Errorf("authclient: exchange refused: status %d, %s", resp.StatusCode, result.Error)
	}
}
