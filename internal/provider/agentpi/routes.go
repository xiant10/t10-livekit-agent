package agentpi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// routesRefresh is how often a routes file is read again.
const routesRefresh = 10 * time.Second

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
