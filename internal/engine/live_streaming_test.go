package engine

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"t10-livekit-agent/internal/audio"
)

// streamingTTS releases audio only when the test says so, one piece per send
// on pieces, and counts Warm calls.
type streamingTTS struct {
	pieces chan []int16
	failed error
	warmed chan string
}

func newStreamingTTS() *streamingTTS {
	return &streamingTTS{pieces: make(chan []int16), warmed: make(chan string, 4)}
}

func (f *streamingTTS) Synthesize(context.Context, string, string) ([]byte, int, error) {
	panic("a streaming provider should be streamed from")
}

func (f *streamingTTS) Warm(voice string) { f.warmed <- voice }

func (f *streamingTTS) SynthesizeStream(ctx context.Context, _, _ string, onAudio func([]byte, int)) error {
	for {
		select {
		case piece, ok := <-f.pieces:
			if !ok {
				return f.failed
			}
			onAudio(audio.Int16ToBytes(piece), 16000)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func startStreamingChunk(t *testing.T, tts *streamingTTS) (*LiveCallController, *fakeSink, *recordingSink, *strings.Builder, chan struct{}) {
	t.Helper()
	deps := &Deps{TTS: tts, Conversations: NewManager("")}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
	t.Cleanup(ctrl.stopCall)
	sink := newFakeSink(16000)
	reply := &recordingSink{}
	spoken := &strings.Builder{}
	queue := make(chan pendingChunk, 1)
	chunk := pendingChunk{text: "A chunk.", result: make(chan speechResult, chunkAudioBuffer)}
	ctrl.ttsSem <- struct{}{}
	go ctrl.synthesizeChunk(ctrl.callCtx, chunk.text, 16000, chunk.result)
	queue <- chunk
	close(queue)
	done := make(chan struct{})
	go ctrl.playSpeech(ctrl.callCtx, 0, queue, sink, reply, spoken, done)
	return ctrl, sink, reply, spoken, done
}

func TestLiveCallController_AudioPlaysBeforeSynthesisFinishes(t *testing.T) {
	tts := newStreamingTTS()
	_, sink, reply, spoken, done := startStreamingChunk(t, tts)

	if len(reply.Tokens()) != 0 {
		t.Fatal("caption released before any audio")
	}
	tts.pieces <- []int16{1, 2}
	waitFor(t, 2*time.Second, func() bool { return sink.pushCount() == 1 })
	if got := reply.Tokens(); len(got) != 1 || got[0] != "A chunk." {
		t.Fatalf("caption = %q", got)
	}
	tts.pieces <- []int16{3}
	close(tts.pieces)
	<-done
	if sink.pushCount() != 2 || len(reply.Tokens()) != 1 || spoken.String() != "A chunk." {
		t.Fatalf("pushes=%d captions=%q spoken=%q", sink.pushCount(), reply.Tokens(), spoken.String())
	}
}

func TestLiveCallController_BargeInStopsAChunkMidStream(t *testing.T) {
	tts := newStreamingTTS()
	ctrl, sink, _, spoken, done := startStreamingChunk(t, tts)

	tts.pieces <- []int16{1, 2}
	waitFor(t, 2*time.Second, func() bool { return sink.pushCount() == 1 })
	ctrl.bargeIn(sink)
	select {
	case tts.pieces <- []int16{3}:
	case <-time.After(100 * time.Millisecond):
	}
	close(tts.pieces)
	<-done
	if sink.pushCount() != 0 { // the drain emptied it; anything here came after
		t.Fatalf("audio after the interruption reached the sink: %d pushes", sink.pushCount())
	}
	if spoken.String() != "A chunk." {
		t.Fatalf("what was started must stay in history, got %q", spoken.String())
	}
}

func TestLiveCallController_FailureBeforeAudioProducesNoCaption(t *testing.T) {
	tts := newStreamingTTS()
	tts.failed = context.DeadlineExceeded
	_, sink, reply, spoken, done := startStreamingChunk(t, tts)
	close(tts.pieces)
	<-done
	if sink.pushCount() != 0 || len(reply.Tokens()) != 0 || spoken.Len() != 0 {
		t.Fatal("failed synthesis produced audio, captions or history")
	}
}

func TestLiveCallController_TurnWarmsTheProvider(t *testing.T) {
	tts := newStreamingTTS()
	close(tts.pieces)
	deps := &Deps{TTS: tts, LLM: &fakeLLM{tokens: []string{"ok."}}, Conversations: NewManager(""), Voice: "Cherry"}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
	defer ctrl.stopCall()
	ctrl.handleTurn([]string{"hello"}, newFakeSink(16000), 0)
	select {
	case voice := <-tts.warmed:
		if voice != "Cherry" {
			t.Fatalf("voice = %q", voice)
		}
	default:
		t.Fatal("turn did not warm the provider")
	}
}

// syncBuffer is a log destination safe to read while the call still writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLogs(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}

func TestLiveCallController_LogsSpeechLostWhenTheCallEnds(t *testing.T) {
	logs := captureLogs(t)
	stt := &fakeSTTProvider{interimDeltas: [][]string{{"还没说完"}}}
	deps := &Deps{STT: stt, TTS: &fakeTTS{sampleRate: 16000}, LLM: &fakeLLM{}, Conversations: NewManager("")}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
	source := newFakeSource(16000)
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(source, newFakeSink(16000)) }()

	source.frames <- loudFrame(320)
	source.frames <- loudFrame(320)
	close(source.frames)
	<-done

	got := logs.String()
	for _, want := range []string{`msg="live call: speech discarded"`, `reason="call ended before end of speech"`, "partial=还没说完"} {
		if !strings.Contains(got, want) {
			t.Fatalf("log lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "captured_ms=0 ") {
		t.Fatalf("captured audio not counted:\n%s", got)
	}
}

func TestLiveCallController_LogsSpeechThatTranscribedToNothing(t *testing.T) {
	logs := captureLogs(t)
	deps := &Deps{STT: &fakeSTTProvider{transcripts: []string{""}}, TTS: &fakeTTS{sampleRate: 16000}, LLM: &fakeLLM{}, Conversations: NewManager("")}
	ctrl := NewLiveCallController(context.Background(), deps, "bob", testLiveConfig())
	source := newFakeSource(16000)
	done := make(chan error, 1)
	go func() { done <- ctrl.Run(source, newFakeSink(16000)) }()

	source.frames <- loudFrame(320)
	source.frames <- silentFrame(320)
	source.frames <- silentFrame(320)
	waitFor(t, 2*time.Second, func() bool { return strings.Contains(logs.String(), "speech discarded") })
	close(source.frames)
	<-done
	if got := logs.String(); !strings.Contains(got, `reason="empty transcript"`) {
		t.Fatalf("log:\n%s", got)
	}
}
