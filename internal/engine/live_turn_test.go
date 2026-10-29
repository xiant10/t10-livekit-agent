package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"t10-livekit-agent/internal/audio"
)

// testLiveConfig is the tuning every live-call test runs with: VAD
// thresholds short enough that one loud frame plus one silent frame make a
// whole utterance, and the same barge-in debounce as a first utterance so a
// test doesn't have to feed extra frames to interrupt. Everything else keeps
// its production default — the sink fakes report an empty backlog, so the
// playback-paced waits never actually block.
func testLiveConfig() LiveConfig {
	return LiveConfig{
		VAD: VADConfig{EnergyThreshold: 500, MinSpeechMS: 20, MinSilenceMS: 20, BargeInMinSpeechMS: 20},
	}
}

// TestLiveCallController_BargeIn exercises the whole live-call turn loop
// with fakes — no network, no LiveKit, no cgo — proving the concrete payoff
// of splitting AudioSource/AudioSink out of the transport: a second
// utterance arriving while the agent is still generating its first reply
// must cancel that reply, drain the sink, and bump the turn version, and
// the interrupted turn must never reach the sink.
func TestLiveCallController_BargeIn(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	llm := newBlockingLLM()
	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"hello", "interrupt"}},
		LLM:           llm,
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	source := newFakeSource(rate)
	sink := newFakeSink(rate)

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	// First utterance: "hello". Its reply gets stuck mid-flight in
	// blockingLLM until barged in on below.
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	select {
	case <-llm.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the first turn's LLM call to start")
	}

	// Barge in with a second utterance while the agent is "speaking".
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, time.Second, func() bool { return sink.pushCount() >= 1 })

	// The version counter advances once per utterance the caller starts, not
	// once per interruption: a bump is what invalidates any reply in flight,
	// and bumping unconditionally is what makes it work even when the
	// interruption lands during the STT commit, before there is a reply to
	// cancel (see TestLiveCallController_WaitsOutAMidThoughtPause).
	if got := ctrl.turnVersion.Load(); got != 2 {
		t.Errorf("turnVersion = %d, want 2 (one per utterance started)", got)
	}
	if got := sink.drainCount(); got < 1 {
		t.Errorf("sink.Drain() was never called on barge-in")
	}
	if got := sink.pushCount(); got != 1 {
		t.Errorf("sink.PushPCM called %d times, want exactly 1 (only the post-interrupt reply; the interrupted turn must push nothing)", got)
	}

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the source closed")
	}

	hist := deps.Conversations.Get("bob").Snapshot()
	wantRoles := []string{"user", "user", "assistant"}
	wantContents := []string{"hello", "interrupt", "ok."}
	if len(hist) != len(wantRoles) {
		t.Fatalf("history = %+v, want roles %v / contents %v", hist, wantRoles, wantContents)
	}
	for i := range wantRoles {
		if hist[i].Role != wantRoles[i] || hist[i].Content != wantContents[i] {
			t.Errorf("hist[%d] = {%s, %q}, want {%s, %q}", i, hist[i].Role, hist[i].Content, wantRoles[i], wantContents[i])
		}
	}
}

// TestLiveCallController_StreamsTranscriptAndReply proves the optional
// OnTranscript/NewReplySink hooks fire alongside (not instead of) the
// audio pushed to sink — the plumbing internal/livekit wires up so a
// live-call client sees the same STT transcript and streamed LLM reply
// text/voice mode already send.
func TestLiveCallController_StreamsTranscriptAndReply(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"hello"}},
		LLM:           &fakeLLM{tokens: []string{"ok", "."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	var (
		mu          sync.Mutex
		transcripts []string
	)
	ctrl.OnTranscript = func(_ uint64, transcript string) {
		mu.Lock()
		defer mu.Unlock()
		transcripts = append(transcripts, transcript)
	}
	sink := &recordingSink{}
	ctrl.NewReplySink = func() ReplySink { return sink }

	source := newFakeSource(rate)
	audioSink := newFakeSink(rate)

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, audioSink) }()

	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, time.Second, func() bool { return audioSink.pushCount() >= 1 })
	waitFor(t, time.Second, func() bool { return sink.Closed() })

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the source closed")
	}

	mu.Lock()
	gotTranscripts := transcripts
	mu.Unlock()
	if want := []string{"hello"}; len(gotTranscripts) != 1 || gotTranscripts[0] != want[0] {
		t.Errorf("OnTranscript calls = %v, want %v", gotTranscripts, want)
	}
	if got := strings.Join(sink.Tokens(), ""); got != "ok." {
		t.Errorf("reply sink saw tokens %v (joined %q), want \"ok.\"", sink.Tokens(), got)
	}
	if !sink.Closed() {
		t.Error("reply sink was never closed")
	}
}

// TestLiveCallController_BargeInWhilePlaybackStillOngoing proves a barge-in
// still works after a turn's LLM/TTS generation has entirely finished (and
// handed all its audio off to the sink) but that audio hasn't actually
// finished playing yet — the gap PushPCM returning immediately creates for
// any reply pipelined well ahead of real-time playback. Before the turn
// waited on AudioSink.QueuedDuration, "speaking" flipped false the moment
// generation ended rather than when the sink actually caught up, so a
// barge-in attempted during this (very real, often multi-second) window
// found nothing to interrupt and the reply played out untouched.
func TestLiveCallController_BargeInWhilePlaybackStillOngoing(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"hello", "interrupt"}},
		LLM:           &fakeLLM{tokens: []string{"ok", "."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	sink.hold() // audio generation finishes long before "playback" does

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	// The whole (fake, instant) LLM+TTS pipeline has long since handed its
	// audio to sink by now, even though sink reports it's still playing.
	waitFor(t, time.Second, func() bool { return sink.pushCount() >= 1 })
	if !ctrl.speaking.Load() {
		t.Fatal("speaking flipped false before the sink finished playing what was pushed to it")
	}

	// Barge in while sink is still (per hold()) mid-playback.
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, time.Second, func() bool { return ctrl.turnVersion.Load() == 2 })
	waitFor(t, time.Second, func() bool { return sink.drainCount() >= 1 })

	sink.releasePlayback()
	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the source closed")
	}
}

// TestLiveCallController_InterimTranscript proves NewInterimSink receives
// the STT's incremental deltas — word by word, as recognized — ahead of and
// independently from OnTranscript's authoritative final text.
func TestLiveCallController_InterimTranscript(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT: &fakeSTTProvider{
			transcripts:   []string{"hello world"},
			interimDeltas: [][]string{{"hel", "lo wor", "ld"}},
		},
		LLM:           &fakeLLM{tokens: []string{"ok", "."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	interim := &recordingSink{}
	ctrl.NewInterimSink = func(_ uint64) TranscriptSink { return interim }
	reply := &recordingSink{}
	ctrl.NewReplySink = func() ReplySink { return reply }

	source := newFakeSource(rate)
	sink := newFakeSink(rate)

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, time.Second, func() bool { return reply.Closed() })

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the source closed")
	}

	if !interim.Closed() {
		t.Error("interim sink was never closed")
	}
	if got := interim.Tokens()[len(interim.Tokens())-1]; got != "hello world" {
		t.Errorf("interim sink saw deltas %v (joined %q), want \"hello world\"", interim.Tokens(), got)
	}
}

// TestLiveCallController_SilentUtteranceDoesNotStall proves an utterance
// whose STT commit comes back empty (e.g. a false VAD trigger on background
// noise) still produces a closed interim sink and an OnTranscript("") call —
// so a client showing a live caption gets told to clear it — while costing
// no turn at all, and without wedging the utterance behind it.
func TestLiveCallController_SilentUtteranceDoesNotStall(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"", "hello"}},
		LLM:           &fakeLLM{tokens: []string{"ok", "."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	var mu sync.Mutex
	var transcripts []string
	ctrl.OnTranscript = func(_ uint64, transcript string) {
		mu.Lock()
		defer mu.Unlock()
		transcripts = append(transcripts, transcript)
	}
	var sinksMu sync.Mutex
	var interimSinks, replySinks []*recordingSink
	ctrl.NewInterimSink = func(_ uint64) TranscriptSink {
		s := &recordingSink{}
		sinksMu.Lock()
		interimSinks = append(interimSinks, s)
		sinksMu.Unlock()
		return s
	}
	ctrl.NewReplySink = func() ReplySink {
		s := &recordingSink{}
		sinksMu.Lock()
		replySinks = append(replySinks, s)
		sinksMu.Unlock()
		return s
	}

	source := newFakeSource(rate)
	sink := newFakeSink(rate)

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	// First "utterance" transcribes empty; second is real speech. The
	// second one's reply completing proves the first one's stages all ran
	// instead of wedging the pipeline.
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, time.Second, func() bool {
		sinksMu.Lock()
		defer sinksMu.Unlock()
		return len(replySinks) == 1 && replySinks[0].Closed()
	})

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after the source closed")
	}

	sinksMu.Lock()
	defer sinksMu.Unlock()
	if len(interimSinks) != 2 || !interimSinks[0].Closed() || !interimSinks[1].Closed() {
		t.Errorf("interim sinks = %+v, want exactly 2 (one per utterance), both closed", interimSinks)
	}
	if len(replySinks) != 1 {
		t.Errorf("reply sinks = %+v, want exactly 1 — a turn is per *reply*, and the empty utterance earns none", replySinks)
	}

	mu.Lock()
	defer mu.Unlock()
	if want := []string{"", "hello"}; len(transcripts) != len(want) || transcripts[0] != want[0] || transcripts[1] != want[1] {
		t.Errorf("OnTranscript calls = %v, want %v", transcripts, want)
	}
}

// TestLiveCallController_WaitsOutAMidThoughtPause is the regression test for
// the worst failure mode a real call showed: someone pausing to think gets
// cut in half by the energy VAD, and the agent answers the first half while
// they are still saying the second.
//
// Two things have to hold for that not to happen, and this exercises both.
// No turn may start while an utterance is still open or still being
// transcribed — even though, by then, a complete transcript is already
// sitting in the queue ready to answer. And when the caller does finally
// stop, both halves have to reach the model as a single turn, not as two
// replies back to back.
func TestLiveCallController_WaitsOutAMidThoughtPause(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	gate := make(chan struct{})
	stt := &fakeSTTProvider{transcripts: []string{"my plan today is", "to go to Beijing"}, commitGate: gate}
	llm := &recordingLLM{tokens: []string{"ok", "."}}
	deps := &Deps{STT: stt, LLM: llm, TTS: &fakeTTS{sampleRate: rate}, Conversations: NewManager("")}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	var replies atomic.Int32
	ctrl.NewReplySink = func() ReplySink { replies.Add(1); return &recordingSink{} }

	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	// First half, said and finished. Its transcription is held by the gate.
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)
	waitFor(t, time.Second, func() bool { return stt.streamCount() == 1 })

	// Second half starts before the first half has even been transcribed.
	source.frames <- loudFrame(frameN)
	waitFor(t, time.Second, func() bool { return stt.streamCount() == 2 })

	// Releasing the gate makes the first half's transcript available. The
	// agent must sit on it: the caller is still mid-sentence.
	close(gate)
	time.Sleep(150 * time.Millisecond)
	if n := llm.callCount(); n != 0 {
		t.Fatalf("LLM called %d times while the caller was still talking, want 0", n)
	}

	// Now they stop.
	source.frames <- silentFrame(frameN)
	waitFor(t, 2*time.Second, func() bool { return llm.callCount() == 1 })
	time.Sleep(150 * time.Millisecond)

	if n := llm.callCount(); n != 1 {
		t.Errorf("LLM called %d times, want exactly 1 — both halves belong to one turn", n)
	}
	if n := replies.Load(); n != 1 {
		t.Errorf("%d reply sinks opened, want exactly 1", n)
	}
	var gotUser []string
	for _, m := range llm.lastCall() {
		if m.Role == "user" {
			gotUser = append(gotUser, m.Content)
		}
	}
	want := []string{"my plan today is", "to go to Beijing"}
	if len(gotUser) != 2 || gotUser[0] != want[0] || gotUser[1] != want[1] {
		t.Errorf("model saw user messages %q, want both halves %q", gotUser, want)
	}

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the source closed")
	}
}

// TestLiveCallController_CaptionsFollowThePlayback proves the reply's text is
// released at the pace of the audio rather than the pace of the model.
//
// The LLM here finishes instantly and produces two chunks; the sink is told
// to report the first one as still playing. Captioning from the token stream
// — which is where it used to happen — would put both chunks on the caller's
// screen immediately, seconds before they heard the first, and a barge-in
// would then leave them staring at a sentence the agent never said.
func TestLiveCallController_CaptionsFollowThePlayback(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"hello"}},
		LLM:           &fakeLLM{tokens: []string{"First part. ", "Second part."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	reply := &recordingSink{}
	ctrl.NewReplySink = func() ReplySink { return reply }

	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	sink.hold() // whatever is pushed is reported as still playing

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, 2*time.Second, func() bool { return sink.pushCount() >= 1 })
	time.Sleep(150 * time.Millisecond) // generation has long since finished

	if got := reply.Tokens(); len(got) != 1 || got[0] != "First part." {
		t.Errorf("captions = %q, want only the chunk that is actually playing ([\"First part.\"])", got)
	}
	if got := sink.pushCount(); got != 1 {
		t.Errorf("sink.PushPCM called %d times, want 1 — the second chunk must wait for the first to play out", got)
	}

	sink.releasePlayback()
	waitFor(t, 2*time.Second, func() bool { return strings.Join(reply.Tokens(), "") == "First part. Second part." })

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the source closed")
	}
}

// TestLiveCallController_RemembersOnlyWhatWasSpoken proves an interrupted
// reply goes into the conversation as the part the caller actually heard,
// marked as cut off — not as the whole thing the model generated, and not
// (as it used to be) as nothing at all. A model told it said nothing will
// happily start the same sentence again on the next turn.
func TestLiveCallController_RemembersOnlyWhatWasSpoken(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"hello", "stop"}},
		LLM:           &fakeLLM{tokens: []string{"First part. ", "Second part."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())

	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	sink.hold()

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)
	waitFor(t, 2*time.Second, func() bool { return sink.pushCount() >= 1 })

	// Cut it off after the first chunk, with the second still queued.
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	waitFor(t, 2*time.Second, func() bool {
		for _, m := range deps.Conversations.Get("bob").Snapshot() {
			if m.Role == "assistant" {
				return true
			}
		}
		return false
	})

	hist := deps.Conversations.Get("bob").Snapshot()
	if len(hist) < 2 || hist[0].Role != "user" || hist[0].Content != "hello" {
		t.Fatalf("history = %+v, want it to open with the user's \"hello\"", hist)
	}
	if want := "First part." + interruptedMarker; hist[1].Role != "assistant" || hist[1].Content != want {
		t.Errorf("hist[1] = {%s, %q}, want the interrupted reply recorded as {assistant, %q}", hist[1].Role, hist[1].Content, want)
	}

	sink.releasePlayback()
	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the source closed")
	}
}

// TestLiveCallController_PreRollFeedsTheStartOfSpeech proves the audio that
// convinced VAD speech had started is itself transcribed. An energy detector
// only fires after MinSpeechMS of loud audio has gone by, so without the
// pre-roll buffer that leading fifth of a second — the first syllable of
// every single utterance — was dropped before the recognizer ever saw it.
func TestLiveCallController_PreRollFeedsTheStartOfSpeech(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50 // 20ms frames

	stt := &fakeSTTProvider{transcripts: []string{"hello"}}
	deps := &Deps{STT: stt, LLM: &fakeLLM{tokens: []string{"ok."}}, TTS: &fakeTTS{sampleRate: rate}, Conversations: NewManager("")}
	cfg := testLiveConfig()
	cfg.VAD.MinSpeechMS = 40 // two loud frames to trigger
	cfg.PreRollMS = 1        // undersizing must not clip the detection window
	ctrl := NewLiveCallController(context.Background(), deps, "bob", cfg)

	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	// Three loud frames: VAD declares speech on the second, so the first is
	// only transcribed if it was held in the pre-roll.
	source.frames <- loudFrame(frameN)
	source.frames <- loudFrame(frameN)
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)

	wantBytes := 3 * frameN * 2 // int16 samples
	waitFor(t, 2*time.Second, func() bool { return stt.pushed() >= wantBytes })
	if got := stt.pushed(); got != wantBytes {
		t.Errorf("STT received %d bytes of audio, want %d (all three loud frames, including the one before speech was declared)", got, wantBytes)
	}

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the source closed")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		t.Fatalf("condition not met within %s", timeout)
	}
}

// Failed and empty synthesis must not claim the model said those words.
func TestLiveCallController_SynthesisFailureIsNeverCaptionedOrRemembered(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "error"}[fail], func(t *testing.T) {
			deps := &Deps{LLM: &fakeLLM{tokens: []string{"Missing. ", "Heard."}},
				TTS: &selectiveTTS{fail: fail}, Conversations: NewManager("")}
			ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
			defer ctrl.stopCall()
			reply := &recordingSink{}
			ctrl.NewReplySink = func() ReplySink { return reply }
			ctrl.handleTurn([]string{"hello"}, newFakeSink(16000), 0)
			if got := strings.TrimSpace(strings.Join(reply.Tokens(), "")); got != "Heard." {
				t.Fatalf("caption = %q, want only successfully synthesized words", got)
			}
			hist := deps.Conversations.Get("bob").Snapshot()
			if len(hist) != 2 || hist[1].Content != "Heard." {
				t.Fatalf("history credits unspoken words: %+v", hist)
			}
		})
	}
}

type selectiveTTS struct{ fail bool }

func (f *selectiveTTS) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	if strings.Contains(text, "Missing") {
		if f.fail {
			return nil, 16000, errors.New("synthesis failed")
		}
		return nil, 16000, nil
	}
	return (&fakeTTS{sampleRate: 16000}).Synthesize(ctx, text, voice)
}

func TestLiveCallController_RejectsTurnRacingWithSpeech(t *testing.T) {
	llm := &recordingLLM{}
	deps := &Deps{LLM: llm, Conversations: NewManager("")}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
	defer ctrl.stopCall()
	sink := newFakeSink(16000)
	ctrl.bargeIn(sink)
	if ctrl.handleTurn([]string{"fragment"}, sink, 0) {
		t.Fatal("accepted a version invalidated after the quiet check")
	}
	ctrl.inFlight.Add(1)
	if ctrl.handleTurn([]string{"fragment"}, sink, 1) {
		t.Fatal("accepted a turn while STT setup/capture was in progress")
	}
	if llm.callCount() != 0 || len(deps.Conversations.Get("bob").Snapshot()) != 0 {
		t.Fatal("failed admission consumed the transcript batch")
	}
}

func TestLiveCallController_CancelDoesNotWaitForSynthesisResult(t *testing.T) {
	ctrl := NewLiveCallController(context.Background(), &Deps{}, "bob", testLiveConfig())
	defer ctrl.stopCall()
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan pendingChunk, 1)
	queue <- pendingChunk{text: "Never heard.", result: make(chan speechResult)}
	close(queue)
	done := make(chan struct{})
	reply := &recordingSink{}
	var spoken strings.Builder
	go ctrl.playSpeech(ctx, 0, queue, newFakeSink(16000), reply, &spoken, done)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("interruption waited for a stalled provider")
	}
	if len(reply.Tokens()) != 0 || spoken.Len() != 0 {
		t.Fatal("cancelled synthesis produced captions or history")
	}
}

func TestLiveCallController_LateAudioCannotRefillDrainedSink(t *testing.T) {
	ctrl := NewLiveCallController(context.Background(), &Deps{}, "bob", testLiveConfig())
	defer ctrl.stopCall()
	sink := newFakeSink(16000)
	result := make(chan speechResult, 1)
	queue := make(chan pendingChunk, 1)
	queue <- pendingChunk{text: "Stale.", result: result}
	close(queue)
	done := make(chan struct{})
	reply := &recordingSink{}
	var spoken strings.Builder
	go ctrl.playSpeech(context.Background(), 0, queue, sink, reply, &spoken, done)
	ctrl.bargeIn(sink)
	result <- speechResult{samples: []int16{1, 2}}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("player did not finish")
	}
	if sink.pushCount() != 0 || len(reply.Tokens()) != 0 || spoken.Len() != 0 {
		t.Fatal("stale synthesis reached playback after drain")
	}
}

func TestLiveCallController_SlowFirstChunkTimesOutWithoutReordering(t *testing.T) {
	var mu sync.Mutex
	var requests []string
	tts := ttsFunc(func(ctx context.Context, text, voice string) ([]byte, int, error) {
		mu.Lock()
		requests = append(requests, strings.TrimSpace(text))
		mu.Unlock()
		if strings.Contains(text, "Slow") {
			<-ctx.Done()
			return nil, 16000, ctx.Err()
		}
		return (&fakeTTS{sampleRate: 16000}).Synthesize(ctx, text, voice)
	})
	deps := &Deps{LLM: &fakeLLM{tokens: []string{"Slow. Second. Third."}},
		TTS: tts, Conversations: NewManager("")}
	cfg := testLiveConfig()
	cfg.TTSConcurrency = 1
	cfg.TTSTimeout = 30 * time.Millisecond
	cfg.MinChunkRunes = 1
	ctrl := NewLiveCallController(context.Background(), deps, "bob", cfg)
	defer ctrl.stopCall()
	reply := &recordingSink{}
	ctrl.NewReplySink = func() ReplySink { return reply }
	done := make(chan struct{})
	go func() { ctrl.handleTurn([]string{"hello"}, newFakeSink(16000), 0); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("one slow chunk stalled the reply")
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(requests, " ") != "Slow. Second. Third." {
		t.Fatalf("synthesis requests overtook the first chunk: %q", requests)
	}
	if got := strings.TrimSpace(strings.Join(reply.Tokens(), "")); got != "Second. Third." {
		t.Fatalf("captions = %q, want only successfully spoken chunks in order", got)
	}
}

type ttsFunc func(context.Context, string, string) ([]byte, int, error)

func (f ttsFunc) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	return f(ctx, text, voice)
}

type mockVADProcessor struct {
	mu        sync.Mutex
	events    []audio.Event
	speaking  bool
	minSpeech int
}

func (m *mockVADProcessor) Process(frame []int16, sampleRate int) audio.Event {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.events) > 0 {
		ev := m.events[0]
		m.events = m.events[1:]
		if ev == audio.EventSpeechStart {
			m.speaking = true
		} else if ev == audio.EventSpeechEnd {
			m.speaking = false
		}
		return ev
	}
	return audio.EventNone
}

func (m *mockVADProcessor) Speaking() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.speaking
}

func (m *mockVADProcessor) SetMinSpeechMS(ms int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.minSpeech = ms
}

func TestLiveCallController_VADOverride(t *testing.T) {
	const rate = 16000
	vad := &mockVADProcessor{
		events: []audio.Event{
			audio.EventSpeechStart,
			audio.EventSpeechEnd,
		},
	}
	cfg := testLiveConfig()
	cfg.VADOverride = vad

	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"custom vad speech"}},
		LLM:           &fakeLLM{tokens: []string{"acknowledged."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "alice", cfg)

	source := newFakeSource(rate)
	sink := newFakeSink(rate)

	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	// Send two frames; mockVADProcessor returns SpeechStart then SpeechEnd
	source.frames <- silentFrame(320)
	source.frames <- silentFrame(320)

	waitFor(t, time.Second, func() bool { return sink.pushCount() >= 1 })

	close(source.frames)
	select {
	case <-runErr:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after source closed")
	}

	hist := deps.Conversations.Get("alice").Snapshot()
	if len(hist) < 2 {
		t.Fatalf("history length = %d, want at least 2", len(hist))
	}
	if hist[0].Content != "custom vad speech" {
		t.Errorf("user message = %q, want 'custom vad speech'", hist[0].Content)
	}
	if hist[1].Content != "acknowledged." {
		t.Errorf("assistant message = %q, want 'acknowledged.'", hist[1].Content)
	}
}

