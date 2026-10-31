// Package telemetry gives the agent's work two IDs and carries them on a
// context.Context, so log lines and spans from anywhere in the process can be
// tied back together:
//
//   - A session is one participant's connection to the room. WithSessionID
//     stamps its ID (the LiveKit participant SID, which both ends already
//     know) and every span and log line below it carries it. A session has no
//     span of its own: a span that lives as long as a call would only be
//     exported when the call ends.
//   - A turn is one input and the reply to it, across every process that
//     touches it. It is one trace, and its ID is that trace's ID, so a turn_id
//     in a log line can be pasted straight into Tempo. StartTurn opens it,
//     continuing the caller's trace when it arrives with a W3C traceparent.
//
// StartStage then starts real child spans via the ordinary global
// otel.Tracer/otel.Meter, which are no-ops until SetupOTel installs a real
// SDK TracerProvider/MeterProvider. StartTurn mints a trace ID itself in that
// case, so every call site works identically, and TurnID and Traceparent keep
// returning real values, whether or not an OTLP endpoint is configured.
package telemetry

import (
	"context"
	"crypto/rand"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName scopes every span/metric this package produces.
const instrumentationName = "t10-livekit-agent"

// How a turn ended, on its root span as turn.outcome and tagging
// t10.turn.outcomes. Bounded on purpose: it is a metric label.
const (
	OutcomeCompleted   = "completed"
	OutcomeInterrupted = "interrupted" // the caller barged in
	OutcomeSuperseded  = "superseded"  // a newer message took over
	OutcomeFailed      = "failed"
	OutcomeNoSpeech    = "no_speech" // VAD fired but no words came of it
	OutcomeCallEnded   = "call_ended"
)

// traceContext is explicit rather than the otel global so propagation works
// whether or not SetupOTel ran.
var traceContext = propagation.TraceContext{}

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
	turnOutcomes = mustInt64Counter(meter, "t10.turn.outcomes",
		"Count of finished turns, tagged by mode and how they ended.")
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

type (
	sessionKey struct{}
	turnKey    struct{}
)

// WithSessionID stamps ctx with the session it belongs to.
func WithSessionID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, sessionKey{}, id)
}

// SessionID returns the ID WithSessionID stamped on ctx, or "".
func SessionID(ctx context.Context) string {
	id, _ := ctx.Value(sessionKey{}).(string)
	return id
}

func sessionAttr(ctx context.Context) []attribute.KeyValue {
	if id := SessionID(ctx); id != "" {
		return []attribute.KeyValue{attribute.String("session.id", id)}
	}
	return nil
}

// TurnOptions configures StartTurn.
type TurnOptions struct {
	// Mode is how the input arrived: text, voice or live.
	Mode string
	// Traceparent continues a trace the sender started. Empty or malformed
	// starts a new one, so a client that sends none still gets traced.
	Traceparent string
	// Follows is the turn this one cut off, if any.
	Follows *Turn
	Attrs   []attribute.KeyValue
}

// Turn is one input and the reply to it: the root span of one trace here.
type Turn struct {
	ctx  context.Context
	span trace.Span
	mode string

	mu      sync.Mutex
	outcome string
	ended   bool
}

// StartTurn opens a turn under ctx, which supplies its cancellation and
// session. Call End exactly when the turn is over, however it went.
func StartTurn(ctx context.Context, opts TurnOptions) *Turn {
	spanOpts := []trace.SpanStartOption{trace.WithAttributes(attribute.String("turn.mode", opts.Mode))}
	spanOpts = append(spanOpts, trace.WithAttributes(sessionAttr(ctx)...), trace.WithAttributes(opts.Attrs...))
	if opts.Follows != nil {
		spanOpts = append(spanOpts, trace.WithLinks(trace.Link{SpanContext: opts.Follows.span.SpanContext()}))
	}
	ctx, span := startRoot(ctx, "conversation.turn", opts.Traceparent, spanOpts...)
	t := &Turn{span: span, mode: opts.Mode}
	t.ctx = context.WithValue(ctx, turnKey{}, t)
	return t
}

// startRoot opens the first span this process has for a trace: a child of
// traceparent if there is a valid one, otherwise a true root. Without an SDK
// the root has no IDs, so one is minted to keep TurnID and Traceparent working.
func startRoot(ctx context.Context, name, traceparent string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if remote := ContextWithTraceparent(ctx, traceparent); trace.SpanContextFromContext(remote).IsValid() {
		return tracer.Start(remote, name, opts...)
	}
	ctx, span := tracer.Start(ctx, name, append(opts, trace.WithNewRoot())...)
	if !span.SpanContext().IsValid() {
		ctx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    newTraceID(),
			SpanID:     newSpanID(),
			TraceFlags: trace.FlagsSampled,
		}))
	}
	return ctx, span
}

// Context carries the turn's trace, and descends from the ctx given to StartTurn.
func (t *Turn) Context() context.Context { return t.ctx }

// SetAttributes records attrs on the turn's root span.
func (t *Turn) SetAttributes(attrs ...attribute.KeyValue) { t.span.SetAttributes(attrs...) }

// End closes the turn. An outcome MarkOutcome recorded wins over the one given
// here; failing that, a non-nil err makes it OutcomeFailed. Only the first
// call counts.
func (t *Turn) End(outcome string, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.ended {
		return
	}
	t.ended = true
	if err != nil {
		t.span.RecordError(err)
		t.span.SetStatus(codes.Error, err.Error())
		outcome = OutcomeFailed
	}
	if t.outcome != "" {
		outcome = t.outcome
	}
	t.span.SetAttributes(attribute.String("turn.outcome", outcome))
	t.span.End()
	turnOutcomes.Add(t.ctx, 1, metric.WithAttributes(attribute.String("mode", t.mode), attribute.String("outcome", outcome)))
}

// MarkOutcome records how the turn on ctx ended, for code that knows why but
// does not own the turn. The first mark stands.
func MarkOutcome(ctx context.Context, outcome string) {
	t, _ := ctx.Value(turnKey{}).(*Turn)
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.outcome == "" {
		t.outcome = outcome
	}
}

// StartStage starts a child span for one stage of work within a turn — an
// STT/LLM/TTS provider call, or a live turn's reply — and starts timing it. Call the returned end func exactly once, with the error the stage
// finished with (nil on success): it records the error on the span (if
// any), closes it, and records the stage's duration and outcome as OTel
// metrics tagged by name, so a single call site drives both traces and
// metrics.
func StartStage(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func(err error)) {
	ctx, span := tracer.Start(ctx, name, trace.WithAttributes(attrs...), trace.WithAttributes(sessionAttr(ctx)...))
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

// TurnID returns the ID of the turn on ctx, which is its trace ID, or "" outside
// a turn.
func TurnID(ctx context.Context) string {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.HasTraceID() {
		return ""
	}
	return sc.TraceID().String()
}

// Traceparent renders the span on ctx as a W3C traceparent, or "" outside a turn.
func Traceparent(ctx context.Context) string {
	carrier := propagation.MapCarrier{}
	traceContext.Inject(ctx, carrier)
	return carrier.Get("traceparent")
}

// ContextWithTraceparent makes traceparent the remote parent of spans started
// under the result. ctx is returned unchanged if traceparent is empty or malformed.
func ContextWithTraceparent(ctx context.Context, traceparent string) context.Context {
	return traceContext.Extract(ctx, propagation.MapCarrier{"traceparent": traceparent})
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
// carrying a session or a turn gets session_id and turn_id attributes
// automatically. This is what gets the IDs into "logs along the way" without
// every log call site needing to know about them — they just need to use the
// *Context slog variants (InfoContext, ErrorContext, ...). Put it outside
// NewOTelLogHandler, so the OTel branch sees the attributes too.
func NewLogHandler(next slog.Handler) slog.Handler {
	return &logHandler{next: next}
}

type logHandler struct{ next slog.Handler }

func (h *logHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *logHandler) Handle(ctx context.Context, r slog.Record) error {
	if id := SessionID(ctx); id != "" {
		r.AddAttrs(slog.String("session_id", id))
	}
	if id := TurnID(ctx); id != "" {
		r.AddAttrs(slog.String("turn_id", id))
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
