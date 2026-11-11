package qwen

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/coder/websocket"

	"t10-livekit-agent/internal/authclient"
	"t10-livekit-agent/internal/provider"
)

type fakeTokens map[string]string

func (f fakeTokens) Token(_ context.Context, grant string) (string, error) {
	if grant == "" {
		return "", authclient.ErrNoGrant
	}
	if token, ok := f[grant]; ok {
		return token, nil
	}
	return "", errors.New("auth down")
}

// Through the gateway each socket carries the call's run token and what it's
// for, so the gateway chooses the call's tenant's key.
func TestGatewaySocketsCarryTheCallsRunToken(t *testing.T) {
	var got http.Header
	var query string
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, query = r.Header.Clone(), r.URL.RawQuery
		conn, err := websocket.Accept(w, r, nil)
		if err == nil {
			conn.Close(websocket.StatusNormalClosure, "")
		}
	}))
	defer gateway.Close()
	r := route{wsURL: "ws" + strings.TrimPrefix(gateway.URL, "http") + "/v1/audio/realtime", apiKey: "voice-key", use: "tts", tokens: fakeTokens{"grt_1": "run-token"}}
	client, err := r.dial(provider.WithGrant(context.Background(), "grt_1"), "qwen3-tts")
	if err != nil {
		t.Fatal(err)
	}
	client.close()
	if got.Get("X-Run-Token") != "run-token" || got.Get("Authorization") != "Bearer voice-key" || !strings.Contains(query, "use=tts") || !strings.Contains(query, "model=qwen3-tts") {
		t.Fatalf("headers %v query %q", got, query)
	}
	// Without a token for a call, it doesn't fall back to sending none.
	if _, err := r.dial(provider.WithGrant(context.Background(), "grt_unknown"), "m"); err == nil {
		t.Fatal("a call's socket opened without its run token")
	}
}
