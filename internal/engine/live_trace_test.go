package engine

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"t10-livekit-agent/internal/telemetry"
)

var (
	spanExporter  = tracetest.NewInMemoryExporter()
	installTracer sync.Once
)

// tracedCall is a live call whose spans are recorded, kept apart from other
// tests' by a session ID of its own.
type tracedCall struct {
	t      *testing.T
	ctrl   *LiveCallController
	source *fakeSource
	sink   *fakeSink
	done   chan error
}

func startTracedCall(t *testing.T, deps *Deps) *tracedCall {
	t.Helper()
	// The global tracer can only be pointed at a provider once per process.
	installTracer.Do(func() {
		otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSyncer(spanExporter)))
	})
	spanExporter.Reset()
	deps.Conversations = NewManager("")
	ctx := telemetry.WithSessionID(context.Background(), t.Name())
	c := &tracedCall{
		t:      t,
		ctrl:   NewLiveCallController(ctx, deps, "bob", testLiveConfig()),
		source: newFakeSource(16000),
		sink:   newFakeSink(16000),
		done:   make(chan error, 1),
	}
	return c
}

func (c *tracedCall) run() { go func() { c.done <- c.ctrl.Run(c.source, c.sink) }() }

func (c *tracedCall) utterance() {
	c.source.frames <- loudFrame(320)
	c.source.frames <- silentFrame(320)
}

// hangUp ends the call and returns its turns, oldest first.
func (c *tracedCall) hangUp() []recordedTurn {
	c.t.Helper()
	close(c.source.frames)
	select {
	case <-c.done:
	case <-time.After(2 * time.Second):
		c.t.Fatal("Run did not return after the source closed")
	}

	var turns []recordedTurn
	byTrace := map[string]int{}
	spans := spanExporter.GetSpans()
	for _, s := range spans {
		if s.Name == "conversation.turn" && attrOf(s, "session.id") == c.t.Name() {
			turns = append(turns, recordedTurn{root: s, id: s.SpanContext.TraceID().String()})
		}
	}
	// Spans are exported as they end, and a turn can outlast the one after it:
	// noise settles before the reply it cut off has finished unwinding.
	sort.Slice(turns, func(i, j int) bool { return turns[i].root.StartTime.Before(turns[j].root.StartTime) })
	for i, turn := range turns {
		byTrace[turn.id] = i
	}
	for _, s := range spans {
		if i, ok := byTrace[s.SpanContext.TraceID().String()]; ok {
			turns[i].names = append(turns[i].names, s.Name)
		}
	}
	return turns
}

type recordedTurn struct {
	id    string
	root  tracetest.SpanStub
	names []string
}

func (r recordedTurn) outcome() string { return attrOf(r.root, "turn.outcome") }

func (r recordedTurn) count(name string) int {
	n := 0
	for _, have := range r.names {
		if have == name {
			n++
		}
	}
	return n
}

func attrOf(s tracetest.SpanStub, key string) string {
	for _, kv := range s.Attributes {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

func TestLiveTrace_CompletedTurn(t *testing.T) {
	stt := &fakeSTTProvider{transcripts: []string{"hello"}}
	c := startTracedCall(t, &Deps{STT: stt, LLM: &fakeLLM{tokens: []string{"ok."}}, TTS: &fakeTTS{sampleRate: 16000}})

	var mu sync.Mutex
	var hookTurns []string
	seen := func(ctx context.Context) {
		mu.Lock()
		defer mu.Unlock()
		hookTurns = append(hookTurns, telemetry.TurnID(ctx))
	}
	c.ctrl.OnTranscript = func(ctx context.Context, _ uint64, _ string) { seen(ctx) }
	c.ctrl.OnState = func(ctx context.Context, _ CallState, _ uint64) { seen(ctx) }
	c.ctrl.NewReplySink = func(ctx context.Context) ReplySink { seen(ctx); return &recordingSink{} }

	c.run()
	c.utterance()
	waitFor(t, 2*time.Second, func() bool { return c.sink.pushCount() >= 1 })
	waitFor(t, 2*time.Second, func() bool {
		c.ctrl.mu.Lock()
		defer c.ctrl.mu.Unlock()
		return c.ctrl.current == nil
	})

	turns := c.hangUp()
	if len(turns) != 1 {
		t.Fatalf("turns = %+v, want 1", turns)
	}
	turn := turns[0]
	if turn.outcome() != telemetry.OutcomeCompleted {
		t.Errorf("outcome = %q", turn.outcome())
	}
	for _, name := range []string{"utterance", "stt.commit", "turn", "llm.chat_stream", "tts.synthesize", "playback"} {
		if turn.count(name) != 1 {
			t.Errorf("%q spans = %d, want 1, in %v", name, turn.count(name), turn.names)
		}
	}
	if attrOf(turn.root, "turn.first_audio_ms") == "" {
		t.Error("turn.first_audio_ms is missing")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hookTurns) == 0 {
		t.Fatal("no hook ran")
	}
	for _, id := range hookTurns {
		if id != turn.id {
			t.Errorf("a hook saw turn %q, want %q (all: %v)", id, turn.id, hookTurns)
		}
	}
}

func TestLiveTrace_BargeInStartsALinkedTurn(t *testing.T) {
	llm := newBlockingLLM()
	c := startTracedCall(t, &Deps{
		STT: &fakeSTTProvider{transcripts: []string{"hello", "interrupt"}},
		LLM: llm, TTS: &fakeTTS{sampleRate: 16000},
	})
	c.run()
	c.utterance()
	wait := time.After(2 * time.Second)
	select {
	case <-llm.started:
	case <-wait:
		t.Fatal("first reply never started")
	}
	c.utterance()
	waitFor(t, 2*time.Second, func() bool { return c.sink.pushCount() >= 1 })
	waitFor(t, 2*time.Second, func() bool {
		c.ctrl.mu.Lock()
		defer c.ctrl.mu.Unlock()
		return c.ctrl.current == nil
	})

	turns := c.hangUp()
	if len(turns) != 2 {
		t.Fatalf("turns = %+v, want 2", turns)
	}
	first, second := turns[0], turns[1]
	if first.outcome() != telemetry.OutcomeInterrupted {
		t.Errorf("first outcome = %q", first.outcome())
	}
	if second.outcome() != telemetry.OutcomeCompleted {
		t.Errorf("second outcome = %q", second.outcome())
	}
	if len(second.root.Links) != 1 || second.root.Links[0].SpanContext.TraceID().String() != first.id {
		t.Errorf("second links = %+v, want the first turn", second.root.Links)
	}
}

// A pause mid-thought is two utterances and one turn.
func TestLiveTrace_CoalescedUtterancesAreOneTurn(t *testing.T) {
	gate := make(chan struct{})
	stt := &fakeSTTProvider{transcripts: []string{"so what I", "wanted was"}, commitGate: gate}
	c := startTracedCall(t, &Deps{STT: stt, LLM: &fakeLLM{tokens: []string{"ok."}}, TTS: &fakeTTS{sampleRate: 16000}})
	c.run()
	c.utterance()
	c.utterance()
	waitFor(t, 2*time.Second, func() bool { return stt.streamCount() == 2 })
	close(gate)
	waitFor(t, 2*time.Second, func() bool { return c.sink.pushCount() >= 1 })
	waitFor(t, 2*time.Second, func() bool {
		c.ctrl.mu.Lock()
		defer c.ctrl.mu.Unlock()
		return c.ctrl.current == nil
	})

	turns := c.hangUp()
	if len(turns) != 1 {
		t.Fatalf("turns = %+v, want 1", turns)
	}
	if got := turns[0].count("utterance"); got != 2 {
		t.Errorf("utterance spans = %d, want 2", got)
	}
}

func TestLiveTrace_NoiseEndsTheTurn(t *testing.T) {
	stt := &fakeSTTProvider{transcripts: []string{""}}
	c := startTracedCall(t, &Deps{STT: stt, LLM: &fakeLLM{tokens: []string{"ok."}}, TTS: &fakeTTS{sampleRate: 16000}})
	c.run()
	c.utterance()
	waitFor(t, 2*time.Second, func() bool {
		c.ctrl.mu.Lock()
		defer c.ctrl.mu.Unlock()
		return stt.streamCount() == 1 && c.ctrl.pending == nil && c.ctrl.inFlight.Load() == 0
	})

	turns := c.hangUp()
	if len(turns) != 1 {
		t.Fatalf("turns = %+v, want 1", turns)
	}
	if turns[0].outcome() != telemetry.OutcomeNoSpeech {
		t.Errorf("outcome = %q", turns[0].outcome())
	}
	if turns[0].count("turn") != 0 {
		t.Errorf("noise produced a reply: %v", turns[0].names)
	}
}

// A cough that cuts the agent off shows as interrupted, then no_speech.
func TestLiveTrace_FalseBargeIn(t *testing.T) {
	llm := newBlockingLLM()
	c := startTracedCall(t, &Deps{
		STT: &fakeSTTProvider{transcripts: []string{"hello", ""}},
		LLM: llm, TTS: &fakeTTS{sampleRate: 16000},
	})
	c.run()
	c.utterance()
	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first reply never started")
	}
	c.utterance()
	waitFor(t, 2*time.Second, func() bool {
		c.ctrl.mu.Lock()
		defer c.ctrl.mu.Unlock()
		return c.ctrl.turnVersion.Load() == 2 && c.ctrl.pending == nil && c.ctrl.current == nil && c.ctrl.inFlight.Load() == 0
	})

	turns := c.hangUp()
	if len(turns) != 2 {
		t.Fatalf("turns = %+v, want 2", turns)
	}
	if turns[0].outcome() != telemetry.OutcomeInterrupted || turns[1].outcome() != telemetry.OutcomeNoSpeech {
		t.Errorf("outcomes = %q, %q", turns[0].outcome(), turns[1].outcome())
	}
	if len(turns[1].root.Links) != 1 {
		t.Errorf("the false barge-in does not link what it cut off: %+v", turns[1].root.Links)
	}
}

func TestLiveTrace_HangingUpMidSpeech(t *testing.T) {
	stt := &fakeSTTProvider{transcripts: []string{"hello"}}
	c := startTracedCall(t, &Deps{STT: stt, LLM: &fakeLLM{tokens: []string{"ok."}}, TTS: &fakeTTS{sampleRate: 16000}})
	c.run()
	c.source.frames <- loudFrame(320)
	waitFor(t, 2*time.Second, func() bool { return stt.streamCount() == 1 })

	turns := c.hangUp()
	if len(turns) != 1 {
		t.Fatalf("turns = %+v, want 1", turns)
	}
	if turns[0].outcome() != telemetry.OutcomeCallEnded {
		t.Errorf("outcome = %q", turns[0].outcome())
	}
}

// Words transcribed but never answered must still close the turn.
func TestLiveTrace_HangingUpBeforeTheReply(t *testing.T) {
	llm := newBlockingLLM()
	c := startTracedCall(t, &Deps{STT: &fakeSTTProvider{transcripts: []string{"hello"}}, LLM: llm, TTS: &fakeTTS{sampleRate: 16000}})
	c.run()
	c.utterance()
	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("reply never started")
	}

	turns := c.hangUp()
	if len(turns) != 1 {
		t.Fatalf("turns = %+v, want 1", turns)
	}
	if turns[0].outcome() != telemetry.OutcomeCallEnded {
		t.Errorf("outcome = %q", turns[0].outcome())
	}
}
