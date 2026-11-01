package openaicompat

import (
	"context"
	"net/http"
	"testing"

	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

func TestSetIdentity(t *testing.T) {
	ctx := provider.WithSessionName(telemetry.WithSessionID(context.Background(), "PA_1"), "ten_acme__usr_a__conv-x/usr_a")
	req, _ := http.NewRequest(http.MethodPost, "http://gateway", nil)
	setIdentity(ctx, req)
	for name, want := range map[string]string{"X-Tenant-ID": "ten_acme", "X-User-Id": "usr_a", "X-Session-ID": "PA_1"} {
		if got := req.Header.Get(name); got != want {
			t.Errorf("%s = %q, want %q", name, got, want)
		}
	}
}

func TestSetIdentityLeavesATestRoomWithNoTenant(t *testing.T) {
	ctx := provider.WithSessionName(context.Background(), "voice-agent-demo/felix")
	req, _ := http.NewRequest(http.MethodPost, "http://gateway", nil)
	setIdentity(ctx, req)
	if got := req.Header.Get("X-Tenant-ID"); got != "" {
		t.Errorf("X-Tenant-ID = %q", got)
	}
}
