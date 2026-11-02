package agentpi

import (
	"os"
	"path/filepath"
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
