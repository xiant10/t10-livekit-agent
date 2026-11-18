package main

import (
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/xiant10/t10-modkit/docsmcp"

	repodocs "t10-livekit-agent/docs"
)

// version is the commit the image was built from, set by the Dockerfile's VERSION.
var version = "dev"

// serveDocs serves this repo's docs to coding agents at /docs/mcp, as the
// workspace's contracts/docs describes. The worker has no other HTTP server, so
// they get their own listener at DOCS_ADDR, off when it's empty. Run as
// "agent docs <addr>", it serves only them, for the docs contract's suite.
func serveDocs() {
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
	addr := os.Getenv("DOCS_ADDR")
	if addr == "" {
		return
	}
	srv := &http.Server{Addr: addr, Handler: docs.Wrap(http.NotFoundHandler()), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil {
			slog.Error("docs server stopped", "addr", addr, "error", err)
		}
	}()
}
