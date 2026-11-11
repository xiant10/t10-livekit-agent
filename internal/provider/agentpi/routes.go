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
	// staleAfter is the platform's one staleness budget for copies of control.
	staleAfter = 15 * time.Minute
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
// waits on it and survives it being down. The last good copy is kept on disk too,
// at cache, so a worker that restarts while control is down still finds sessions,
// and it's kept with no time limit. Past staleAfter without an answer, each failed
// read logs an error, which is what alerts. See "Reading control" in
// t10-agent-control's docs/design.md.
type routeControl struct {
	url      string
	cache    string
	tokens   ServiceTokens
	instance string
	client   *http.Client

	mu       sync.Mutex
	replicas map[string]string
	etag     string
	// reached is when control last answered, or when the copy on disk was saved.
	reached time.Time
}

func newRouteControl(base, cache string, tokens ServiceTokens) *routeControl {
	instance, err := os.Hostname()
	if err != nil {
		instance = "unknown"
	}
	c := &routeControl{
		url: strings.TrimRight(base, "/"), cache: cache, tokens: tokens, instance: instance,
		client: &http.Client{Timeout: controlTimeout}, replicas: map[string]string{},
	}
	c.loadCopy()
	return c
}

// loadCopy starts from the copy on disk, aged from when it was saved.
func (c *routeControl) loadCopy() {
	if c.cache == "" {
		return
	}
	raw, err := os.ReadFile(c.cache)
	if err != nil {
		if !os.IsNotExist(err) {
			slog.Warn("saved routes can't be read", "file", c.cache, "error", err)
		}
		return
	}
	replicas, err := replicasOfRoutes(raw)
	if err != nil {
		slog.Warn("saved routes aren't valid", "file", c.cache, "error", err)
		return
	}
	saved := time.Now()
	if info, err := os.Stat(c.cache); err == nil {
		saved = info.ModTime()
	}
	c.replicas, c.reached = replicas, saved
	slog.Info("routes loaded from disk", "replicas", len(replicas))
}

// saveCopy replaces the copy on disk whole: written beside it, then renamed.
func (c *routeControl) saveCopy(raw []byte) {
	if c.cache == "" {
		return
	}
	tmp := c.cache + ".tmp"
	err := os.WriteFile(tmp, raw, 0o600)
	if err == nil {
		err = os.Rename(tmp, c.cache)
	}
	if err != nil {
		slog.Warn("routes not saved", "file", c.cache, "error", err)
	}
}

// failed logs a failed read: a warning while the copy is fresh, an error once
// it's stale or there's none.
func (c *routeControl) failed(err error) {
	c.mu.Lock()
	reached := c.reached
	c.mu.Unlock()
	if !reached.IsZero() && time.Since(reached) < staleAfter {
		slog.Warn("routes unavailable, keeping the last good copy", "age", time.Since(reached).Round(time.Second), "error", err)
		return
	}
	age := time.Duration(0)
	if !reached.IsZero() {
		age = time.Since(reached).Round(time.Second)
	}
	slog.Error("control feed stale", "feed", "routes", "age", age, "error", err)
}

func (c *routeControl) follow(ctx context.Context) {
	ticker := time.NewTicker(routesRefresh)
	defer ticker.Stop()
	for {
		if err := c.fetch(ctx); err != nil && ctx.Err() == nil {
			c.failed(err)
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
		c.mu.Lock()
		c.reached = time.Now()
		c.mu.Unlock()
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
	c.replicas, c.etag, c.reached = replicas, resp.Header.Get("ETag"), time.Now()
	c.mu.Unlock()
	c.saveCopy(raw)
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
