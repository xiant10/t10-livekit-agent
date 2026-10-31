package qwen

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeRealtime speaks enough of the DashScope protocol for synthesis: session
// setup, then the audio deltas for one committed text.
type fakeRealtime struct {
	dials     atomic.Int32
	syntheses atomic.Int32
	deltas    [][]byte
	// failFirst makes the first connection close instead of synthesizing,
	// like a spare the server dropped while it sat idle.
	failFirst bool
	gate      chan struct{} // if set, the first delta waits for it
}

func (f *fakeRealtime) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	n := f.dials.Add(1)
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	send := func(v map[string]any) {
		data, _ := json.Marshal(v)
		_ = conn.Write(r.Context(), websocket.MessageText, data)
	}
	send(map[string]any{"type": "session.created"})
	for {
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var ev map[string]any
		_ = json.Unmarshal(data, &ev)
		switch ev["type"] {
		case "session.update":
			send(map[string]any{"type": "session.updated"})
		case "input_text_buffer.commit":
			if f.failFirst && n == 1 {
				return
			}
			f.syntheses.Add(1)
			for i, delta := range f.deltas {
				if i == 0 && f.gate != nil {
					<-f.gate
				}
				send(map[string]any{"type": "response.audio.delta", "delta": base64.StdEncoding.EncodeToString(delta)})
			}
			send(map[string]any{"type": "response.done"})
		}
	}
}

func newFakeTTS(t *testing.T, f *fakeRealtime) *TTS {
	t.Helper()
	server := httptest.NewServer(f)
	t.Cleanup(server.Close)
	return &TTS{wsURL: "ws" + strings.TrimPrefix(server.URL, "http"), apiKey: "k", model: "m", defaultVoice: "Cherry"}
}

func TestSynthesizeStreamDeliversAudioBeforeTheResponseEnds(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 0}, {2, 0}}, gate: make(chan struct{})}
	tts := newFakeTTS(t, f)
	first := make(chan []byte, 2)
	done := make(chan error, 1)
	go func() {
		done <- tts.SynthesizeStream(context.Background(), "你好", "", func(pcm []byte, rate int) {
			if rate != ttsSampleRate {
				t.Errorf("rate = %d", rate)
			}
			first <- append([]byte(nil), pcm...)
		})
	}()
	close(f.gate)
	select {
	case got := <-first:
		if got[0] != 1 {
			t.Fatalf("first delta = %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no audio delivered")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestSynthesizeKeepsSamplesWholeAcrossDeltas(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 2, 3}, {4}, {5, 6}}}
	tts := newFakeTTS(t, f)
	var sizes []int
	err := tts.SynthesizeStream(context.Background(), "你好", "", func(pcm []byte, _ int) { sizes = append(sizes, len(pcm)) })
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, size := range sizes {
		if size%2 != 0 {
			t.Fatalf("split sample: sizes = %v", sizes)
		}
		total += size
	}
	if total != 6 {
		t.Fatalf("sizes = %v", sizes)
	}
	pcm, rate, err := tts.Synthesize(context.Background(), "你好", "")
	if err != nil || len(pcm) != 6 || pcm[5] != 6 || rate != ttsSampleRate {
		t.Fatalf("pcm=%v rate=%d err=%v", pcm, rate, err)
	}
}

func TestWarmSessionIsUsedOnceAndNotDialledTwice(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 0}}}
	tts := newFakeTTS(t, f)
	tts.Warm("")
	tts.Warm("") // a second turn starting before the first spare is used
	if _, _, err := tts.Synthesize(context.Background(), "你好", ""); err != nil {
		t.Fatal(err)
	}
	if got := f.dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want the spare alone", got)
	}
	if _, _, err := tts.Synthesize(context.Background(), "你好", ""); err != nil {
		t.Fatal(err)
	}
	if got := f.dials.Load(); got != 2 {
		t.Fatalf("dials = %d: a spare must serve one synthesis only", got)
	}
}

func TestDeadSpareFallsBackToAFreshSession(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 0}}, failFirst: true}
	tts := newFakeTTS(t, f)
	tts.Warm("")
	pcm, _, err := tts.Synthesize(context.Background(), "你好", "")
	if err != nil || len(pcm) != 2 {
		t.Fatalf("pcm=%v err=%v", pcm, err)
	}
	if f.dials.Load() != 2 || f.syntheses.Load() != 1 {
		t.Fatalf("dials=%d syntheses=%d", f.dials.Load(), f.syntheses.Load())
	}
}

func TestSpareForAnotherVoiceIsNotUsed(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 0}}}
	tts := newFakeTTS(t, f)
	tts.Warm("Serena")
	if _, _, err := tts.Synthesize(context.Background(), "你好", "Cherry"); err != nil {
		t.Fatal(err)
	}
	if got := f.dials.Load(); got != 2 {
		t.Fatalf("dials = %d", got)
	}
}

func TestFailedSpareFallsBackToAFreshSession(t *testing.T) {
	f := &fakeRealtime{deltas: [][]byte{{1, 0}}}
	tts := newFakeTTS(t, f)
	s := &ttsSpare{voice: "Cherry", ready: make(chan struct{}), err: errors.New("dial failed"), expire: time.NewTimer(time.Hour)}
	close(s.ready)
	tts.spare = s
	if _, _, err := tts.Synthesize(context.Background(), "你好", ""); err != nil {
		t.Fatal(err)
	}
	if got := f.dials.Load(); got != 1 {
		t.Fatalf("dials = %d", got)
	}
}

func TestConnectionClosedBeforeAudioIsAnError(t *testing.T) {
	f := &fakeRealtime{failFirst: true}
	tts := newFakeTTS(t, f)
	if err := tts.SynthesizeStream(context.Background(), "你好", "", func([]byte, int) {}); err == nil {
		t.Fatal("expected an error")
	}
}
