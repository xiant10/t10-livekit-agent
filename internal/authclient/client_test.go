package authclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type server struct {
	*httptest.Server
	mints     atomic.Int32
	expiresIn atomic.Int32
	refuse    atomic.Value
}

func newServer(t *testing.T) (*server, *Client) {
	t.Helper()
	s := &server{}
	s.expiresIn.Store(300)
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/v1/token" || req["grant_type"] != "agent_grant" || req["client_id"] != "svc" || req["client_secret"] != "secret" {
			t.Errorf("bad request: %s %v", r.URL.Path, req)
		}
		if code, _ := s.refuse.Load().(string); code != "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": code})
			return
		}
		n := s.mints.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"access_token": fmt.Sprintf("token-%s-%d", req["grant_id"], n), "expires_in": s.expiresIn.Load(),
		})
	}))
	t.Cleanup(s.Close)
	c, err := New(Options{URL: s.URL + "/", ClientID: "svc", ClientSecret: "secret"})
	if err != nil {
		t.Fatal(err)
	}
	return s, c
}

func TestCachesPerGrant(t *testing.T) {
	s, c := newServer(t)
	ctx := context.Background()
	first, err := c.Token(ctx, "a")
	if err != nil {
		t.Fatal(err)
	}
	again, _ := c.Token(ctx, "a")
	other, _ := c.Token(ctx, "b")
	if first != again || first == other || s.mints.Load() != 2 {
		t.Fatalf("a=%q again=%q b=%q mints=%d", first, again, other, s.mints.Load())
	}
}

func TestRenewsNearExpiry(t *testing.T) {
	s, c := newServer(t)
	s.expiresIn.Store(30)
	ctx := context.Background()
	first, _ := c.Token(ctx, "a")
	second, _ := c.Token(ctx, "a")
	if first == second {
		t.Fatal("reused a token with under a minute left")
	}
}

func TestBurstMintsOnce(t *testing.T) {
	s, c := newServer(t)
	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Token(context.Background(), "a"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if s.mints.Load() != 1 {
		t.Fatalf("%d mints", s.mints.Load())
	}
}

func TestNoGrantNeverCallsTheServer(t *testing.T) {
	s, c := newServer(t)
	if _, err := c.Token(context.Background(), ""); !errors.Is(err, ErrNoGrant) {
		t.Fatalf("got %v", err)
	}
	if s.mints.Load() != 0 {
		t.Fatal("asked for a token with no grant")
	}
}

func TestStoppedGrantDropsTheCachedToken(t *testing.T) {
	s, c := newServer(t)
	s.expiresIn.Store(30)
	ctx := context.Background()
	if _, err := c.Token(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	s.refuse.Store("invalid_grant")
	if _, err := c.Token(ctx, "a"); !errors.Is(err, ErrGrantStopped) {
		t.Fatalf("got %v", err)
	}
	if token, ok := c.tokens["a"]; ok && token.token != "" {
		t.Fatal("kept a token for a stopped grant")
	}
	s.refuse.Store("grant_expired")
	if _, err := c.Token(ctx, "a"); !errors.Is(err, ErrGrantExpired) {
		t.Fatalf("got %v", err)
	}
}

func TestExpiredEntriesAreDropped(t *testing.T) {
	_, c := newServer(t)
	ctx := context.Background()
	if _, err := c.Token(ctx, "old"); err != nil {
		t.Fatal(err)
	}
	c.tokens["old"].expires = time.Now().Add(-time.Second)
	if _, err := c.Token(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.tokens["old"]; ok {
		t.Fatal("kept a finished call's token")
	}
}

func TestNewRequiresCredentials(t *testing.T) {
	for _, opts := range []Options{{}, {URL: "u"}, {URL: "u", ClientID: "c"}} {
		if _, err := New(opts); err == nil {
			t.Errorf("accepted %+v", opts)
		}
	}
}
