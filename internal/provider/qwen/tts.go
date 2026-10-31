package qwen

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
)

func init() {
	provider.RegisterTTS("qwen", func(cfg config.Config) (provider.TTSProvider, error) {
		if cfg.DashScopeAPIKey == "" {
			return nil, fmt.Errorf("qwen: DASHSCOPE_API_KEY is required")
		}
		return &TTS{
			wsURL:        cfg.DashScopeWSURL,
			apiKey:       cfg.DashScopeAPIKey,
			model:        cfg.QwenTTSModel,
			defaultVoice: cfg.QwenTTSVoice,
		}, nil
	})
}

const (
	ttsSampleRate = 24000

	// ttsSpareSetupTimeout bounds opening a spare session. It has to leave a
	// cold retry room inside the caller's own TTS timeout.
	ttsSpareSetupTimeout = 3 * time.Second
	// ttsSpareMaxAge is how long a spare session may sit unused. It covers a
	// slow model and a tool call, without keeping connections open between
	// turns on a quiet call.
	ttsSpareMaxAge = 15 * time.Second
)

// TTS is a provider.TTSProvider backed by DashScope's Qwen realtime speech
// synthesis API. Like STT, each synthesis uses its own WebSocket connection,
// but because opening one costs ~850ms, Warm can open it ahead of time.
type TTS struct {
	wsURL        string
	apiKey       string
	model        string
	defaultVoice string

	mu    sync.Mutex
	spare *ttsSpare
}

// ttsSpare is a session opened by Warm. Its client and err are set before
// ready closes.
type ttsSpare struct {
	voice  string
	ready  chan struct{}
	client *realtimeClient
	err    error
	expire *time.Timer
}

// discard closes the spare once it has finished opening.
func (s *ttsSpare) discard() {
	go func() {
		<-s.ready
		if s.client != nil {
			s.client.close()
		}
	}()
}

var markdownCleanupRe = regexp.MustCompile("[*_~`#>+=|\\[\\]{}()!]")

func sanitizeForSpeech(text string) string {
	cleaned := markdownCleanupRe.ReplaceAllString(text, "")
	return strings.Join(strings.Fields(cleaned), " ")
}

// open dials a connection and configures it for voice.
func (p *TTS) open(ctx context.Context, voice string) (*realtimeClient, error) {
	client, err := dialRealtime(ctx, p.wsURL, p.model, p.apiKey)
	if err != nil {
		return nil, err
	}
	err = client.send(ctx, map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"voice":           voice,
			"mode":            "server_commit",
			"response_format": "pcm",
			"sample_rate":     ttsSampleRate,
		},
	})
	if err != nil {
		client.close()
		return nil, fmt.Errorf("qwen tts: session.update: %w", err)
	}
	// Wait for the server's answer so that it can reject the session (e.g. bad
	// voice) before we commit to sending text. session.created arrives first
	// and says nothing about this update.
	if _, err := waitForType(ctx, client, "session.updated", ttsSpareSetupTimeout); err != nil {
		client.close()
		return nil, fmt.Errorf("qwen tts: session setup (voice %q): %w", voice, err)
	}
	return client, nil
}

// Warm opens a session in the background, so that the next synthesis finds it
// ready. It runs alongside the LLM, which usually takes longer than this does.
// The spare is not tied to any turn: one left behind by a barge-in serves the
// next turn, and an unused one closes itself after ttsSpareMaxAge.
func (p *TTS) Warm(voice string) {
	if voice == "" {
		voice = p.defaultVoice
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.spare != nil {
		if p.spare.voice == voice {
			return
		}
		p.spare.expire.Stop()
		p.spare.discard()
	}
	s := &ttsSpare{voice: voice, ready: make(chan struct{})}
	p.spare = s
	s.expire = time.AfterFunc(ttsSpareMaxAge, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.spare == s {
			p.spare = nil
			s.discard()
		}
	})
	go func() {
		defer close(s.ready)
		ctx, cancel := context.WithTimeout(context.Background(), ttsSpareSetupTimeout)
		defer cancel()
		s.client, s.err = p.open(ctx, voice)
	}()
}

// takeSpare hands over the spare session for voice, if there is one.
func (p *TTS) takeSpare(voice string) *ttsSpare {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := p.spare
	if s == nil || s.voice != voice {
		return nil
	}
	s.expire.Stop()
	p.spare = nil
	return s
}

// acquire returns a session for voice and whether it was a spare. A spare still
// opening is waited for, since it can only be closer to ready than a new one.
func (p *TTS) acquire(ctx context.Context, voice string) (*realtimeClient, bool, error) {
	if s := p.takeSpare(voice); s != nil {
		select {
		case <-s.ready:
			if s.err == nil {
				return s.client, true, nil
			}
		case <-ctx.Done():
			s.discard()
			return nil, false, ctx.Err()
		}
	}
	client, err := p.open(ctx, voice)
	return client, false, err
}

func (p *TTS) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	var pcm []byte
	err := p.SynthesizeStream(ctx, text, voice, func(delta []byte, _ int) {
		pcm = append(pcm, delta...)
	})
	return pcm, ttsSampleRate, err
}

// SynthesizeStream implements provider.StreamingTTS.
func (p *TTS) SynthesizeStream(ctx context.Context, text, voice string, onAudio func(pcm []byte, sampleRate int)) error {
	clean := sanitizeForSpeech(text)
	if clean == "" {
		return nil
	}
	if voice == "" {
		voice = p.defaultVoice
	}

	client, warm, err := p.acquire(ctx, voice)
	if err != nil {
		return err
	}
	trace.SpanFromContext(ctx).SetAttributes(attribute.Bool("tts.warm", warm))

	delivered, err := speak(ctx, client, clean, onAudio)
	client.close()
	// A spare may have been closed by the server while it waited. Nothing has
	// been heard yet in that case, so a fresh session can start cleanly.
	if err != nil && warm && !delivered && ctx.Err() == nil {
		if client, err = p.open(ctx, voice); err != nil {
			return err
		}
		defer client.close()
		_, err = speak(ctx, client, clean, onAudio)
	}
	return err
}

// speak synthesizes text on an open session, and reports whether any audio was
// delivered.
func speak(ctx context.Context, client *realtimeClient, text string, onAudio func(pcm []byte, sampleRate int)) (bool, error) {
	if err := client.send(ctx, map[string]any{"type": "input_text_buffer.append", "text": text}); err != nil {
		return false, fmt.Errorf("qwen tts: append text: %w", err)
	}
	if err := client.send(ctx, map[string]any{"type": "input_text_buffer.commit"}); err != nil {
		return false, fmt.Errorf("qwen tts: commit: %w", err)
	}

	const idleTimeout = 20 * time.Second
	deadline := time.NewTimer(idleTimeout)
	defer deadline.Stop()

	// How long the provider takes to produce its first audio, which is what
	// the caller waits for, and how long it takes to produce all of it. The
	// gap between tts.total_ms and the span's duration is connection setup.
	started := time.Now()
	var firstDelta time.Duration
	defer func() {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64("tts.first_delta_ms", firstDelta.Milliseconds()),
			attribute.Int64("tts.total_ms", time.Since(started).Milliseconds()),
		)
	}()

	// A delta can in principle end in the middle of a sample.
	var carry []byte
	delivered := false
	for {
		select {
		case ev, ok := <-client.events:
			if !ok {
				if !delivered {
					return false, errors.New("qwen tts: connection closed before any audio")
				}
				return true, nil
			}
			switch ev.typ() {
			case "response.audio.delta":
				if delta := ev.str("delta"); delta != "" {
					if firstDelta == 0 {
						firstDelta = time.Since(started)
					}
					if decoded, derr := base64.StdEncoding.DecodeString(delta); derr == nil {
						pcm := append(carry, decoded...)
						whole := len(pcm) &^ 1
						carry = append([]byte(nil), pcm[whole:]...)
						if whole > 0 {
							delivered = true
							onAudio(pcm[:whole], ttsSampleRate)
						}
					}
					if !deadline.Stop() {
						<-deadline.C
					}
					deadline.Reset(idleTimeout)
				}
			case "response.done":
				return delivered, nil
			case "error":
				return delivered, fmt.Errorf("qwen tts: provider error: %v", ev)
			}
		case <-deadline.C:
			return delivered, fmt.Errorf("qwen tts: synthesis idle-timeout after %s", idleTimeout)
		case <-ctx.Done():
			return delivered, ctx.Err()
		}
	}
}
