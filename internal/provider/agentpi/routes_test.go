package agentpi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

const routesDoc = `{"pools":{"sales":{"replicas":{"r0":"http://a:9810"}},"sales-codex":{"replicas":{"r0":"http://b:9820/"}}}}`

func TestRouteFileFollowsChangesAndKeepsTheLastGood(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	write := func(content string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(routesDoc)
	now := time.Unix(1000, 0)
	f, err := openRouteFile(path, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if got := f.current()["sales-codex.r0"]; got != "http://b:9820/agent" {
		t.Fatalf("replica = %q", got)
	}

	write(`{"pools":{"new":{"replicas":{"r0":"http://c:1"}}}}`)
	if _, ok := f.current()["new.r0"]; ok {
		t.Fatal("read the file again before the refresh was due")
	}
	now = now.Add(routesRefresh)
	if got := f.current()["new.r0"]; got != "http://c:1/agent" {
		t.Fatalf("after a change, replica = %q", got)
	}

	for _, bad := range []string{"{", `{"pools":{}}`, `{"pools":{"p":{"replicas":{"r":"nowhere"}}}}`} {
		write(bad)
		now = now.Add(routesRefresh)
		if got := f.current()["new.r0"]; got == "" {
			t.Fatalf("%q replaced good routes", bad)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	now = now.Add(routesRefresh)
	if got := f.current()["new.r0"]; got == "" {
		t.Fatal("a missing file replaced good routes")
	}
}

func TestOpenRouteFileRefusesABadStart(t *testing.T) {
	if _, err := openRouteFile(filepath.Join(t.TempDir(), "absent.json"), time.Now); err == nil {
		t.Fatal("started with no routes file")
	}
}

func TestSessionsRouteThroughTheRoutesFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agents.json")
	if err := os.WriteFile(path, []byte(routesDoc), 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := New(Options{URL: "http://default/agent", RoutesFile: path, Tokens: fixedToken("token"), WhenBusy: WhenBusyAuto})
	if err != nil {
		t.Fatal(err)
	}
	if got := l.urlFor("sales.r0.7"); got != "http://a:9810/agent" {
		t.Fatalf("urlFor = %q", got)
	}
	if got := l.urlFor("nobody.r0.7"); got != "http://default/agent" {
		t.Fatalf("an unknown prefix went to %q", got)
	}
}

type fixedServiceToken string

func (f fixedServiceToken) ServiceToken(context.Context) (string, error) { return string(f), nil }

func TestRouteControlKeepsTheLastGoodCopy(t *testing.T) {
	var status atomic.Int32
	var doc atomic.Value
	doc.Store(routesDoc)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/routes" || r.Header.Get("Authorization") != "Bearer service" || r.Header.Get(instanceHeader) == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if code := status.Load(); code != 0 {
			w.WriteHeader(int(code))
			return
		}
		w.Header().Set("ETag", `"one"`)
		if r.Header.Get("If-None-Match") == `"one"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write([]byte(doc.Load().(string)))
	}))
	defer server.Close()

	c := newRouteControl(server.URL, fixedServiceToken("service"))
	ctx := context.Background()
	if err := c.fetch(ctx); err != nil {
		t.Fatal(err)
	}
	if got := c.current()["sales.r0"]; got != "http://a:9810/agent" {
		t.Fatalf("replica: %q", got)
	}
	if err := c.fetch(ctx); err != nil {
		t.Fatalf("unchanged: %v", err)
	}

	status.Store(http.StatusServiceUnavailable)
	if err := c.fetch(ctx); err == nil {
		t.Fatal("no error from an outage")
	}
	if got := c.current()["sales.r0"]; got == "" {
		t.Fatal("an outage lost the routes, which would drop live calls")
	}

	// A bad document needs a new ETag to arrive at all, so start from none.
	status.Store(0)
	doc.Store(`{"pools":{}}`)
	c.etag = ""
	if err := c.fetch(ctx); err == nil {
		t.Fatal("a bad snapshot was taken")
	}
	if got := c.current()["sales.r0"]; got == "" {
		t.Fatal("a bad snapshot lost the routes")
	}
}
