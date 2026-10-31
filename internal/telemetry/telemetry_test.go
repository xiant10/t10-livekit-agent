package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var (
	exporter      = tracetest.NewInMemoryExporter()
	installTracer sync.Once
)

// These run first (file order, then source order): they cover the no-SDK
// path, and the global provider below can only be installed once.

func TestTurnIDWithoutAnSDK(t *testing.T) {
	if got := TurnID(context.Background()); got != "" {
		t.Fatalf("TurnID on a bare context = %q, want \"\"", got)
	}
	if got := Traceparent(context.Background()); got != "" {
		t.Fatalf("Traceparent on a bare context = %q, want \"\"", got)
	}

	turn := StartTurn(context.Background(), TurnOptions{Mode: "text"})
	defer turn.End(OutcomeCompleted, nil)
	id := TurnID(turn.Context())
	if len(id) != 32 {
		t.Fatalf("TurnID = %q, want a 32-char hex trace ID", id)
	}

	// A context derived from it (as every real call site does via
	// context.WithCancel/WithTimeout) must still carry the same ID.
	child, cancel := context.WithCancel(turn.Context())
	defer cancel()
	if got := TurnID(child); got != id {
		t.Errorf("TurnID on derived context = %q, want %q", got, id)
	}

	stageCtx, stageEnd := StartStage(turn.Context(), "test.stage")
	if got := TurnID(stageCtx); got != id {
		t.Errorf("TurnID inside a stage = %q, want %q", got, id)
	}
	stageEnd(errTest)

	second := StartTurn(context.Background(), TurnOptions{Mode: "text"})
	defer second.End(OutcomeCompleted, nil)
	if got := TurnID(second.Context()); got == id {
		t.Error("two turns produced the same ID")
	}

	if got := Traceparent(turn.Context()); !strings.Contains(got, id) {
		t.Errorf("Traceparent = %q, want it to contain %s", got, id)
	}
}

func TestStartTurnContinuesATraceparentWithoutAnSDK(t *testing.T) {
	const id = "4bf92f3577b34da6a3ce929d0e0e4736"
	turn := StartTurn(context.Background(), TurnOptions{Traceparent: "00-" + id + "-00f067aa0ba902b7-01"})
	defer turn.End(OutcomeCompleted, nil)
	if got := TurnID(turn.Context()); got != id {
		t.Errorf("TurnID = %q, want the sender's %q", got, id)
	}

	bad := StartTurn(context.Background(), TurnOptions{Traceparent: "not-a-traceparent"})
	defer bad.End(OutcomeCompleted, nil)
	if got := TurnID(bad.Context()); len(got) != 32 {
		t.Errorf("TurnID after a malformed traceparent = %q, want a fresh trace ID", got)
	}
}

var errTest = errors.New("boom")

func TestLogHandlerAttachesIDs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "nothing")
	if strings.Contains(buf.String(), "_id=") {
		t.Errorf("log line outside a session unexpectedly has IDs: %s", buf.String())
	}
	buf.Reset()

	ctx := WithSessionID(context.Background(), "PA_1")
	logger.InfoContext(ctx, "in session")
	if !strings.Contains(buf.String(), "session_id=PA_1") || strings.Contains(buf.String(), "turn_id") {
		t.Errorf("log line = %q, want session_id only", buf.String())
	}
	buf.Reset()

	turn := StartTurn(ctx, TurnOptions{})
	defer turn.End(OutcomeCompleted, nil)
	logger.InfoContext(turn.Context(), "in turn")
	if want := "turn_id=" + TurnID(turn.Context()); !strings.Contains(buf.String(), want) || !strings.Contains(buf.String(), "session_id=PA_1") {
		t.Errorf("log line = %q, want session_id and %s", buf.String(), want)
	}
}

func TestLogHandlerReachesBothBranchesOfTheOTelFanout(t *testing.T) {
	var local, bridged bytes.Buffer
	fanout := &fanoutHandler{local: slog.NewTextHandler(&local, nil), otel: slog.NewTextHandler(&bridged, nil)}
	logger := slog.New(NewLogHandler(fanout))

	logger.InfoContext(WithSessionID(context.Background(), "PA_1"), "line")
	for name, buf := range map[string]*bytes.Buffer{"local": &local, "otel": &bridged} {
		if !strings.Contains(buf.String(), "session_id=PA_1") {
			t.Errorf("%s branch = %q, want session_id", name, buf.String())
		}
	}
}

func TestTurnSpans(t *testing.T) {
	installTracer.Do(func() {
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter)))
	})
	exporter.Reset()

	span := func(name string) tracetest.SpanStub {
		t.Helper()
		for _, s := range exporter.GetSpans() {
			if s.Name == name {
				return s
			}
		}
		t.Fatalf("no %q span in %v", name, exporter.GetSpans().Snapshots())
		return tracetest.SpanStub{}
	}
	attr := func(s tracetest.SpanStub, key string) string {
		for _, kv := range s.Attributes {
			if string(kv.Key) == key {
				return kv.Value.Emit()
			}
		}
		return ""
	}

	ctx := WithSessionID(context.Background(), "PA_1")
	first := StartTurn(ctx, TurnOptions{Mode: "live"})
	_, stageEnd := StartStage(first.Context(), "stage")
	stageEnd(nil)
	MarkOutcome(first.Context(), OutcomeInterrupted)
	MarkOutcome(first.Context(), OutcomeSuperseded)
	first.End(OutcomeCompleted, nil)
	first.End(OutcomeFailed, errTest)

	root := span("conversation.turn")
	if root.Parent.IsValid() {
		t.Error("a turn with no traceparent should be a true root, not have a parent")
	}
	if root.SpanContext.TraceID().String() != TurnID(first.Context()) {
		t.Error("TurnID is not the trace ID actually exported")
	}
	if got := attr(root, "turn.outcome"); got != OutcomeInterrupted {
		t.Errorf("turn.outcome = %q, want the first mark", got)
	}
	if got := attr(root, "session.id"); got != "PA_1" {
		t.Errorf("root session.id = %q", got)
	}
	if got := attr(span("stage"), "session.id"); got != "PA_1" {
		t.Errorf("stage session.id = %q", got)
	}
	if !span("stage").Parent.Equal(root.SpanContext) {
		t.Error("stage is not a child of the turn")
	}
	exporter.Reset()

	second := StartTurn(ctx, TurnOptions{Mode: "live", Follows: first})
	second.End(OutcomeCompleted, errTest)
	root = span("conversation.turn")
	if got := attr(root, "turn.outcome"); got != OutcomeFailed {
		t.Errorf("turn.outcome with an error = %q", got)
	}
	if len(root.Links) != 1 || root.Links[0].SpanContext.TraceID().String() != TurnID(first.Context()) {
		t.Errorf("links = %v, want the turn it followed", root.Links)
	}
	if TurnID(second.Context()) == TurnID(first.Context()) {
		t.Error("a following turn should be its own trace")
	}
	exporter.Reset()

	const id = "4bf92f3577b34da6a3ce929d0e0e4736"
	third := StartTurn(ctx, TurnOptions{Traceparent: "00-" + id + "-00f067aa0ba902b7-01"})
	third.End(OutcomeCompleted, nil)
	if root = span("conversation.turn"); root.SpanContext.TraceID().String() != id || !root.Parent.IsRemote() {
		t.Errorf("turn did not continue the sender's trace: %+v", root.SpanContext)
	}
}
