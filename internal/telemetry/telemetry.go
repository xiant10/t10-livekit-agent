// Package telemetry stamps every client-initiated unit of work — an
// incoming text/voice message, or a live call — with a correlation ID and
// carries it on a context.Context, so any log line emitted anywhere while
// handling that request or session can be tied back together. The same
// context also carries real OpenTelemetry spans once SetupOTel has been
// called, so the same IDs that tie log lines together also tie together a
// trace/metrics view of where time actually went.
//
// The correlation ID is an OpenTelemetry trace ID (go.opentelemetry.io/otel/trace):
// StartSession fabricates one and stamps it as a *remote* span context, the
// same way an incoming W3C traceparent header would. StartStage and
// StartSession's own root span then start real child spans off of it via
// the ordinary global otel.Tracer/otel.Meter — which are no-ops until
// SetupOTel installs a real SDK TracerProvider/MeterProvider, so every call
// site works identically (and CorrelationID keeps returning the same ID)
// whether or not an OTLP endpoint is configured.
package telemetry

import (
	"context"
	"crypto/rand"
	"log/slog"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName scopes every span/metric this package produces.
const instrumentationName = "t10-livekit-agent"

// tracer and meter are the global otel.Tracer/otel.Meter handles: the
// go.opentelemetry.io/otel global package returns proxies that transparently
// start delegating to the real implementation the moment SetupOTel calls
// otel.SetTracerProvider/SetMeterProvider, so obtaining them once here (long
// before SetupOTel may or may not ever run) is the intended usage.
var (
	tracer = otel.Tracer(instrumentationName)
	meter  = otel.Meter(instrumentationName)

	// Named without a "_ms"/"duration_ms" suffix because WithUnit("ms")
	// already gets translated into one (e.g. "..._milliseconds_bucket") by
	// the Prometheus exporter — baking our own in too would double it up.
	stageDuration = mustFloat64Histogram(meter, "t10.stage.duration",
		"Duration of one STT/LLM/TTS provider call, or one live-call turn.", "ms")
	stageCalls = mustInt64Counter(meter, "t10.stage.calls",
		"Count of completed stage calls, tagged by stage name and whether it errored.")
)

func mustFloat64Histogram(m metric.Meter, name, desc, unit string) metric.Float64Histogram {
	h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		panic(err) // only fails on a malformed instrument name/description — a programming bug
	}
	return h
}

func mustInt64Counter(m metric.Meter, name, desc string) metric.Int64Counter {
	c, err := m.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		panic(err)
	}
	return c
}

// StartSession stamps ctx with a fresh correlation ID and opens the root
// "session" span for it. Call it once per client-initiated request or
// session — when a text or voice message comes in, or when a live call
// starts — before any work begins for it. Every context derived from the
// returned one (context.WithCancel, context.WithTimeout, ...) carries the
// same ID, and so does every slog "*Context" call and every StartStage span
// made with it or a descendant. Call the returned func (typically via
// defer) once the request/session is fully done, to close the root span.
//
// The "session" span's parent is a fabricated remote span ID (see
// newSpanID below) rather than an actual received span, so a trace backend
// may flag it with a harmless "invalid parent span" / "skipping clock skew
// adjustment" warning — that's an accurate description of what's going on
// (there's genuinely no real parent), not a bug.
func StartSession(ctx context.Context) (context.Context, func()) {
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    newTraceID(),
		SpanID:     newSpanID(),
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx = trace.ContextWithRemoteSpanContext(ctx, sc)
	ctx, span := tracer.Start(ctx, "session")
	return ctx, func() { span.End() }
}

// StartStage starts a child span for one stage of work within a session —
// an STT/LLM/TTS provider call, or one live-call turn — and starts timing
// it. Call the returned end func exactly once, with the error the stage
// finished with (nil on success): it records the error on the span (if
// any), closes it, and records the stage's duration and outcome as OTel
// metrics tagged by name, so a single call site drives both traces and
// metrics.
func StartStage(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(err error)) {
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...))
	start := time.Now()
	return ctx, func(err error) {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()

		metricAttrs := metric.WithAttributes(
			attribute.String("stage", name),
			attribute.Bool("error", err != nil),
		)
		stageDuration.Record(ctx, float64(time.Since(start).Microseconds())/1000, metricAttrs)
		stageCalls.Add(ctx, 1, metricAttrs)
	}
}

// CorrelationID returns the correlation ID StartSession stamped on ctx (or
// one of its ancestors), or "" if StartSession was never called for it.
func CorrelationID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

func newTraceID() (id trace.TraceID) {
	_, _ = rand.Read(id[:])
	return id
}

func newSpanID() (id trace.SpanID) {
	_, _ = rand.Read(id[:])
	return id
}

// NewLogHandler wraps next so that any record logged through a context
// StartSession stamped gets a "correlation_id" attribute attached
// automatically. This is what gets the ID into "logs along the way"
// without every log call site needing to know about it — they just need to
// use the *Context slog variants (InfoContext, ErrorContext, ...) with a
// context descended from StartSession.
func NewLogHandler(next slog.Handler) slog.Handler {
	return &logHandler{next: next}
}

type logHandler struct{ next slog.Handler }

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := CorrelationID(ctx); id != "" {
		r.AddAttrs(slog.String("correlation_id", id))
	}
	return h.next.Handle(ctx, r)
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{next: h.next.WithAttrs(attrs)}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	return &logHandler{next: h.next.WithGroup(name)}
}

// NewOTelLogHandler returns a slog.Handler that bridges log records into the
// OTel log pipeline (via otelslog) *and* forwards them to next. This gives
// us both: logs exported via OTLP to whatever backend the collector routes
// them to, and the same logs written locally (to stderr/a file) by next.
// If no LoggerProvider has been set (SetupOTel was not called), the bridge
// half is a no-op — the same graceful-degradation contract as traces/metrics.
func NewOTelLogHandler(next slog.Handler) slog.Handler {
	bridge := otelslog.NewHandler(instrumentationName)
	return &fanoutHandler{local: next, otel: bridge}
}

type fanoutHandler struct {
	local slog.Handler
	otel  slog.Handler
}

func (f *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return f.local.Enabled(ctx, level) || f.otel.Enabled(ctx, level)
}

func (f *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	_ = f.otel.Handle(ctx, r)
	return f.local.Handle(ctx, r)
}

func (f *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &fanoutHandler{local: f.local.WithAttrs(attrs), otel: f.otel.WithAttrs(attrs)}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	return &fanoutHandler{local: f.local.WithGroup(name), otel: f.otel.WithGroup(name)}
}
