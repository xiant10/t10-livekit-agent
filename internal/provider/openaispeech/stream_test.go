package openaispeech

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeRealtime answers like the transcription socket: nothing happens until
// the client commits, then the segment's words stream back.
func fakeRealtime(t *testing.T, audioBytes *int) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/realtime" || r.URL.Query().Get("intent") != "transcription" || r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("bad dial %s %q", r.URL.String(), r.Header.Get("Authorization"))
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(1 << 20)
		ctx := r.Context()
		send := func(v map[string]any) {
			b, _ := json.Marshal(v)
			c.Write(ctx, websocket.MessageText, b)
		}
		segments := []struct{ words, final string }{
			{"hello there", "Hello there."},
			{"how are you", "How are you?"},
		}
		commits := 0
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var ev map[string]any
			json.Unmarshal(data, &ev)
			switch ev["type"] {
			case "session.update":
				s := ev["session"].(map[string]any)
				in := s["audio"].(map[string]any)["input"].(map[string]any)
				if s["type"] != "transcription" || in["turn_detection"] != nil || in["transcription"].(map[string]any)["language"] != "en" {
					t.Errorf("bad session %v", s)
				}
			case "input_audio_buffer.append":
				b64, _ := ev["audio"].(string)
				*audioBytes += len(b64) / 4 * 3
			case "input_audio_buffer.commit":
				seg := segments[commits]
				id := string(rune('a' + commits))
				commits++
				send(map[string]any{"type": "input_audio_buffer.committed", "item_id": id})
				for i, word := range strings.Fields(seg.words) {
					if i > 0 {
						word = " " + word
					}
					send(map[string]any{"type": "conversation.item.input_audio_transcription.delta", "item_id": id, "delta": word})
				}
				send(map[string]any{"type": "conversation.item.input_audio_transcription.completed", "item_id": id, "transcript": seg.final})
			}
		}
	}))
}

func TestStreamInterimAndCommit(t *testing.T) {
	var audioBytes int
	srv := fakeRealtime(t, &audioBytes)
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	stt := &STT{client: c, model: "m"}

	var mu sync.Mutex
	var snaps []string
	s, _ := stt.NewStream(context.Background(), "en", func(snap string) {
		mu.Lock()
		snaps = append(snaps, snap)
		mu.Unlock()
	})
	s.PushPCM(make([]byte, minChunk)) // enough audio, ending quiet: the stream commits by itself

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(snaps)
		mu.Unlock()
		if n >= 3 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	early := append([]string(nil), snaps...)
	mu.Unlock()
	if len(early) < 3 || early[0] != "hello" || early[len(early)-1] != "Hello there." {
		t.Fatalf("interim before commit: %q", early)
	}

	s.PushPCM(make([]byte, 3200)) // too short to commit alone: Commit sends it
	got, err := s.Commit(context.Background())
	if err != nil || got != "Hello there. How are you?" {
		t.Fatalf("got %q, %v", got, err)
	}
	if want := (minChunk + 3200) / 2 * 3; audioBytes < want-100 || audioBytes > want+100 {
		t.Errorf("server saw %d bytes, want ~%d resampled to 24kHz", audioBytes, want)
	}
}

func TestStreamAbort(t *testing.T) {
	var audioBytes int
	srv := fakeRealtime(t, &audioBytes)
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	s, _ := (&STT{client: c, model: "m"}).NewStream(context.Background(), "en", nil)
	s.PushPCM(make([]byte, 100))
	s.Abort()
	if _, err := s.Commit(context.Background()); err == nil {
		t.Error("Commit after Abort should fail")
	}
}

func TestCJKJoin(t *testing.T) {
	var s rtStream
	s.items = map[string]*item{"a": {text: "你好。"}, "b": {text: "请问"}}
	s.order = []string{"a", "b"}
	if got := s.snapshot(); got != "你好。请问" {
		t.Errorf("got %q", got)
	}
}
