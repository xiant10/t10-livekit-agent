package agentpi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	// routesRefresh is how often a routes file is read again, and
	// t10-agent-control asked.
	routesRefresh  = 10 * time.Second
	controlTimeout = 5 * time.Second
	maxSnapshot    = 4 << 20
	// instanceHeader names this process on the console's Fleet page.
	instanceHeader = "T10-Router-Instance"
)

// routeSource answers which replica each session ID prefix means.
type routeSource interface {
	current() map[string]string
}

// ServiceTokens gets this service's own token. *authclient.Client is the real one.
type ServiceTokens interface {
	ServiceToken(ctx context.Context) (string, error)
}

// routeControl follows t10-agent-control in the background, so a live call never
// waits on it and survives it being down. The last good copy stays in memory.
type routeControl struct {
	url      string
	tokens   ServiceTokens
	instance string
	client   *http.Client

	mu       sync.Mutex
	replicas map[string]string
	etag     string
}

func newRouteControl(base string, tokens ServiceTokens) *routeControl {
	instance, err := os.Hostname()
	if err != nil {
		instance = "unknown"
	}
	return &routeControl{
		url: strings.TrimRight(base, "/"), tokens: tokens, instance: instance,
		client: &http.Client{Timeout: controlTimeout}, replicas: map[string]string{},
	}
}

func (c *routeControl) follow(ctx context.Context) {
	ticker := time.NewTicker(routesRefresh)
	defer ticker.Stop()
	for {
		if err := c.fetch(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("routes unavailable, keeping the last good copy", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// fetch asks once. A bad snapshot changes nothing.
func (c *routeControl) fetch(ctx context.Context) error {
	token, err := c.tokens.ServiceToken(ctx)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url+"/v1/routes", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(instanceHeader, c.instance)
	c.mu.Lock()
	if c.etag != "" {
		req.Header.Set("If-None-Match", c.etag)
	}
	c.mu.Unlock()
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("agentpi: control answered %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxSnapshot))
	if err != nil {
		return err
	}
	replicas, err := replicasOfRoutes(raw)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replicas, c.etag = replicas, resp.Header.Get("ETag")
	return nil
}

func (c *routeControl) current() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.replicas
}

// routeFile follows a routing document, the file ai-gateway-agent also reads, so a
// new pool is one edit. Only the replicas matter here: a client creates a
// conversation before calling, so voice only finds sessions and places nothing.
type routeFile struct {
	path string
	now  func() time.Time

	mu       sync.Mutex
	replicas map[string]string
	raw      []byte
	read     time.Time
}

func openRouteFile(path string, now func() time.Time) (*routeFile, error) {
	f := &routeFile{path: path, now: now}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("agentpi: routes: %w", err)
	}
	if f.replicas, err = replicasOfRoutes(raw); err != nil {
		return nil, err
	}
	f.raw, f.read = raw, now()
	return f, nil
}

// current answers from memory, and rereads at most once per routesRefresh. A bad
// or missing file changes nothing, so a typo can't stop live calls.
func (f *routeFile) current() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.now().Sub(f.read) < routesRefresh {
		return f.replicas
	}
	f.read = f.now()
	raw, err := os.ReadFile(f.path)
	if err != nil {
		slog.Warn("routes file unreadable, keeping the last good routes", "error", err)
		return f.replicas
	}
	if bytes.Equal(raw, f.raw) {
		return f.replicas
	}
	replicas, err := replicasOfRoutes(raw)
	if err != nil {
		slog.Warn("routes file invalid, keeping the last good routes", "error", err)
		return f.replicas
	}
	f.replicas, f.raw = replicas, raw
	return f.replicas
}

// replicasOfRoutes maps each session ID prefix, <pool>.<replica>, to its endpoint.
func replicasOfRoutes(raw []byte) (map[string]string, error) {
	var doc struct {
		Pools map[string]struct {
			Replicas map[string]string `json:"replicas"`
		} `json:"pools"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("agentpi: routes: %w", err)
	}
	replicas := map[string]string{}
	for pool, config := range doc.Pools {
		for name, base := range config.Replicas {
			target, err := url.JoinPath(base, "agent")
			if err != nil || !strings.Contains(base, "://") {
				return nil, fmt.Errorf("agentpi: routes: %s.%s has no valid url", pool, name)
			}
			replicas[pool+"."+name] = target
		}
	}
	if len(replicas) == 0 {
		return nil, fmt.Errorf("agentpi: routes: no pool has a replica")
	}
	return replicas, nil
}
