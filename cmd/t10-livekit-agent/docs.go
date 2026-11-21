package main

import (
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/xiant10/t10-modkit/docsmcp"

	repodocs "t10-livekit-agent/docs"
)

// version is the commit the image was built from, set by the Dockerfile's VERSION.
var version = "dev"

// serveHTTP serves this repo's docs to coding agents at /docs/mcp, as the
// workspace's contracts/docs describes, and /healthz and /readyz for the
// cluster. The worker takes no calls over HTTP, so this listener holds only
// those, at LIVEKIT_AGENT_LISTEN_ADDR, off when it's empty. Run as
// "t10-livekit-agent docs <addr>", it serves only the docs, for the docs
// contract's suite. The returned func marks the worker as draining, which
// /readyz then reports.
func serveHTTP() (draining func()) {
	docs, err := docsmcp.New(docsmcp.Options{
		Component: "t10-livekit-agent",
		Summary:   "The voice worker: it joins LiveKit rooms, turns speech into agent turns and the agent's words back into speech. Clients talk to it through the room protocol.",
		Version:   version,
		Docs:      repodocs.FS,
	})
	if err != nil {
		slog.Error("read docs", "error", err)
		os.Exit(1)
	}
	if served, err := docsmcp.DocsOnly(os.Args, docs); served {
		slog.Error("docs only stopped", "error", err)
		os.Exit(1)
	}
	var drained atomic.Bool
	addr := os.Getenv("LIVEKIT_AGENT_LISTEN_ADDR")
	if addr == "" {
		return func() { drained.Store(true) }
	}
	srv := &http.Server{Addr: addr, Handler: docs.Wrap(healthMux(&drained)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("http server stopped", "addr", addr, "error", err)
		}
	}()
	return func() { drained.Store(true) }
}

// healthMux answers /healthz while the process runs, and /readyz until the
// worker starts draining and stops claiming rooms.
func healthMux(drained *atomic.Bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if drained.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
