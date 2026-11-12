package openaispeech

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/config"
)

// TestLiveRoundTrip speaks a sentence with TTS and transcribes it with STT.
// It needs OPENAI_API_KEY and costs a fraction of a cent.
func TestLiveRoundTrip(t *testing.T) {
	if os.Getenv("OPENAI_API_KEY") == "" {
		t.Skip("OPENAI_API_KEY not set")
	}
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	c, err := newClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	tts := &TTS{client: c, model: cfg.OpenAITTSModel, defaultVoice: cfg.OpenAITTSVoice}
	var chunks int
	var pcm []byte
	start := time.Now()
	var first time.Duration
	err = tts.SynthesizeStream(ctx, "The quick brown fox jumps over the lazy dog.", "", func(b []byte, rate int) {
		if chunks == 0 {
			first = time.Since(start)
		}
		chunks++
		pcm = append(pcm, b...)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("tts: %d bytes in %d chunks, first audio after %v, total %v", len(pcm), chunks, first, time.Since(start))

	pcm16k := audio.Int16ToBytes(audio.Resample(audio.BytesToInt16(pcm), 24000, 16000))
	stt := &STT{client: c, model: cfg.OpenAISTTModel}

	start = time.Now()
	got, err := stt.TranscribeBatch(ctx, pcm16k, "en")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("stt batch (%v): %q", time.Since(start), got)
	if !strings.Contains(strings.ToLower(got), "quick brown fox") {
		t.Errorf("transcript %q", got)
	}

	// Feed the audio at real-time pace, as the live call does, and note when
	// captions show up: they must arrive before Commit, or the socket was not used.
	start = time.Now()
	var interims []string
	var mu sync.Mutex
	s, _ := stt.NewStream(ctx, "en", func(snap string) {
		mu.Lock()
		interims = append(interims, snap)
		mu.Unlock()
		t.Logf("  interim +%v: %q", time.Since(start).Round(time.Millisecond), snap)
	})
	for i := 0; i < len(pcm16k); i += 640 {
		s.PushPCM(pcm16k[i:min(i+640, len(pcm16k))])
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	before := len(interims)
	mu.Unlock()
	got2, err := s.Commit(ctx)
	if err != nil || !strings.Contains(strings.ToLower(got2), "lazy dog") {
		t.Errorf("stream: %q, %v", got2, err)
	}
	t.Logf("stt stream: %q (%d interims before commit, commit took %v after speech ended)", got2, before, time.Since(start))
	if before == 0 {
		t.Error("no interim text arrived while audio was still being pushed")
	}
}
