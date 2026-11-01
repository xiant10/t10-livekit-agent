package livekit

import (
	"context"
	"testing"
	"time"
)

func TestAwaitGrantWaitsForAJoinInFlight(t *testing.T) {
	calls := 0
	grant := awaitGrant(context.Background(), func() (string, bool) {
		calls++
		return "grt_1", calls == 3
	})
	if grant != "grt_1" || calls != 3 {
		t.Fatalf("grant %q after %d lookups", grant, calls)
	}
}

func TestAwaitGrantGivesUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if grant := awaitGrant(ctx, func() (string, bool) { return "", false }); grant != "" {
		t.Fatalf("grant %q from nobody", grant)
	}
}

func TestAwaitGrantDoesNotWaitForAParticipantWithoutOne(t *testing.T) {
	start := time.Now()
	if grant := awaitGrant(context.Background(), func() (string, bool) { return "", true }); grant != "" {
		t.Fatalf("grant %q", grant)
	}
	if time.Since(start) > time.Second {
		t.Fatal("waited for a grant that is never coming")
	}
}
