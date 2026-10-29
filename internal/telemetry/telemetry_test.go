package telemetry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestStartSessionAndCorrelationID(t *testing.T) {
	if got := CorrelationID(context.Background()); got != "" {
		t.Fatalf("CorrelationID on a bare context = %q, want \"\"", got)
	}

	ctx, end := StartSession(context.Background())
	defer end()
	id := CorrelationID(ctx)
	if id == "" {
		t.Fatal("CorrelationID after StartSession is empty")
	}
	if len(id) != 32 {
		t.Errorf("CorrelationID = %q, want a 32-char hex trace ID", id)
	}

	// A context derived from ctx (as every real call site does via
	// context.WithCancel/WithTimeout) must still carry the same ID.
	child, cancel := context.WithCancel(ctx)
	defer cancel()
	if got := CorrelationID(child); got != id {
		t.Errorf("CorrelationID on derived context = %q, want %q", got, id)
	}

	ctx2, end2 := StartSession(context.Background())
	defer end2()
	if got := CorrelationID(ctx2); got == id {
		t.Error("two StartSession calls produced the same correlation ID")
	}
}

func TestStartStagePreservesCorrelationID(t *testing.T) {
	ctx, end := StartSession(context.Background())
	defer end()
	id := CorrelationID(ctx)

	stageCtx, stageEnd := StartStage(ctx, "test.stage")
	if got := CorrelationID(stageCtx); got != id {
		t.Errorf("CorrelationID inside a stage = %q, want %q", got, id)
	}
	stageEnd(nil)
	stageEnd2Ctx, stageEnd2 := StartStage(ctx, "test.stage.error")
	if got := CorrelationID(stageEnd2Ctx); got != id {
		t.Errorf("CorrelationID inside a failing stage = %q, want %q", got, id)
	}
	stageEnd2(errTest)
}

var errTest = errors.New("boom")

func TestLogHandlerAttachesCorrelationID(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewLogHandler(slog.NewTextHandler(&buf, nil)))

	logger.InfoContext(context.Background(), "no session")
	if strings.Contains(buf.String(), "correlation_id") {
		t.Errorf("log line without a session unexpectedly has correlation_id: %s", buf.String())
	}
	buf.Reset()

	ctx, end := StartSession(context.Background())
	defer end()
	id := CorrelationID(ctx)
	logger.InfoContext(ctx, "with session")
	if !strings.Contains(buf.String(), "correlation_id="+id) {
		t.Errorf("log line = %q, want it to contain correlation_id=%s", buf.String(), id)
	}
}
