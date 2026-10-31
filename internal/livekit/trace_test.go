package livekit

import (
	"context"
	"strings"
	"testing"

	"t10-livekit-agent/internal/roomprotocol"
	"t10-livekit-agent/internal/telemetry"
)

func TestTurnAttributes(t *testing.T) {
	outside := turnAttributes(context.Background(), map[string]string{"a": "b"})
	if len(outside) != 1 {
		t.Errorf("attributes outside a turn = %v", outside)
	}

	turn := telemetry.StartTurn(context.Background(), telemetry.TurnOptions{Mode: "text"})
	defer turn.End(telemetry.OutcomeCompleted, nil)
	attrs := turnAttributes(turn.Context(), map[string]string{"a": "b"})
	id := telemetry.TurnID(turn.Context())
	if attrs["a"] != "b" || attrs[roomprotocol.AttrTurnID] != id || !strings.Contains(attrs[roomprotocol.AttrTraceparent], id) {
		t.Errorf("attributes = %v, want turn %s", attrs, id)
	}
}
