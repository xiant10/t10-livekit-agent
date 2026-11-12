package openaispeech

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"t10-livekit-agent/internal/config"
)

func cfgFor(url string) config.Config {
	return config.Config{OpenAISpeechBaseURL: url, OpenAISpeechAPIKey: "k", OpenAISTTModel: "stt-m", OpenAITTSModel: "tts-m", OpenAITTSVoice: "alloy"}
}

func TestKeyRequired(t *testing.T) {
	if _, err := newClient(config.Config{}); err == nil {
		t.Fatal("want error without a key")
	}
}

func TestTranscribeBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/audio/transcriptions" || r.Header.Get("Authorization") != "Bearer k" {
			t.Errorf("bad request %s %q", r.URL.Path, r.Header.Get("Authorization"))
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Fatal(err)
		}
		if r.FormValue("model") != "stt-m" || r.FormValue("language") != "zh" {
			t.Errorf("form: %v", r.MultipartForm.Value)
		}
		f, _, err := r.FormFile("file")
		if err != nil {
			t.Fatal(err)
		}
		head := make([]byte, 4)
		f.Read(head)
		if string(head) != "RIFF" {
			t.Errorf("file is not a WAV: %q", head)
		}
		w.Write([]byte(`{"text":" 你好 "}`))
	}))
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	got, err := (&STT{client: c, model: "stt-m"}).TranscribeBatch(context.Background(), make([]byte, 3200), "zh")
	if err != nil || got != "你好" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestStreamCommitAndAbort(t *testing.T) {
	var bytesSeen int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/audio/transcriptions" { // no realtime socket here: the stream must fall back
			http.NotFound(w, r)
			return
		}
		r.ParseMultipartForm(1 << 20)
		f, _, _ := r.FormFile("file")
		b := make([]byte, 1<<16)
		bytesSeen, _ = f.Read(b)
		w.Write([]byte(`{"text":"hi"}`))
	}))
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	stt := &STT{client: c, model: "m"}

	s, _ := stt.NewStream(context.Background(), "", nil)
	s.PushPCM(make([]byte, 1000))
	s.PushPCM(make([]byte, 1000))
	if got, err := s.Commit(context.Background()); err != nil || got != "hi" {
		t.Fatalf("got %q, %v", got, err)
	}
	if bytesSeen != 44+2000 {
		t.Errorf("uploaded %d bytes, want WAV of both pushes", bytesSeen)
	}

	a, _ := stt.NewStream(context.Background(), "", nil)
	a.PushPCM(make([]byte, 100))
	a.Abort()
	if _, err := a.Commit(context.Background()); err == nil {
		t.Error("Commit after Abort should fail")
	}
}

func TestSynthesizeStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		json.NewDecoder(r.Body).Decode(&req)
		if r.URL.Path != "/audio/speech" || req["response_format"] != "pcm" || req["voice"] != "nova" || req["model"] != "tts-m" || req["input"] != "hello" {
			t.Errorf("bad request %s %v", r.URL.Path, req)
		}
		fl := w.(http.Flusher)
		w.Write([]byte{1, 2, 3}) // odd split must not break sample alignment
		fl.Flush()
		w.Write([]byte{4, 5, 6})
	}))
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	tts := &TTS{client: c, model: "tts-m", defaultVoice: "alloy"}
	pcm, rate, err := tts.Synthesize(context.Background(), "hello", "nova")
	if err != nil || rate != 24000 || len(pcm) != 6 {
		t.Fatalf("got %d bytes at %d, %v", len(pcm), rate, err)
	}
	if string(pcm) != string([]byte{1, 2, 3, 4, 5, 6}) {
		t.Errorf("pcm reordered: %v", pcm)
	}
}

func TestErrorMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(401)
		w.Write([]byte(`{"error":{"message":"bad key"}}`))
	}))
	defer srv.Close()
	c, _ := newClient(cfgFor(srv.URL))
	_, _, err := (&TTS{client: c}).Synthesize(context.Background(), "x", "")
	if err == nil || !strings.Contains(err.Error(), "bad key") {
		t.Fatalf("got %v", err)
	}
}
