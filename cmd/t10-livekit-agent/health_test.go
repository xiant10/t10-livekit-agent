package main

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHealthRoutes(t *testing.T) {
	var drained atomic.Bool
	h := healthMux(&drained)
	status := func(path string) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w.Code
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz %d", got)
	}
	if got := status("/readyz"); got != http.StatusOK {
		t.Fatalf("/readyz %d before draining", got)
	}
	drained.Store(true)
	if got := status("/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("/readyz %d while draining", got)
	}
	if got := status("/healthz"); got != http.StatusOK {
		t.Fatalf("/healthz %d while draining", got)
	}
}
