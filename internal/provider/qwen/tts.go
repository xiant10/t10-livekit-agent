package qwen

import (
	"context"
	"encoding/base64"
	"fmt"
	"regexp"
	"strings"
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

const ttsSampleRate = 24000

// TTS is a provider.TTSProvider backed by DashScope's Qwen realtime speech
// synthesis API. Like STT, each call opens and tears down its own
// WebSocket connection for simplicity.
type TTS struct {
	wsURL        string
	apiKey       string
	model        string
	defaultVoice string
}

var markdownCleanupRe = regexp.MustCompile("[*_~`#>+=|\\[\\]{}()!]")

func sanitizeForSpeech(text string) string {
	cleaned := markdownCleanupRe.ReplaceAllString(text, "")
	return strings.Join(strings.Fields(cleaned), " ")
}

func (p *TTS) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	clean := sanitizeForSpeech(text)
	if clean == "" {
		return nil, ttsSampleRate, nil
	}
	if voice == "" {
		voice = p.defaultVoice
	}

	client, err := dialRealtime(ctx, p.wsURL, p.model, p.apiKey)
	if err != nil {
		return nil, ttsSampleRate, err
	}
	defer client.close()

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
		return nil, ttsSampleRate, fmt.Errorf("qwen tts: session.update: %w", err)
	}

	// Give the server a brief window to reject the session (e.g. bad voice)
	// before we commit to sending text.
	select {
	case ev, ok := <-client.events:
		if !ok {
			return nil, ttsSampleRate, fmt.Errorf("qwen tts: connection closed during session setup (voice %q rejected?)", voice)
		}
		if ev.typ() == "error" {
			return nil, ttsSampleRate, fmt.Errorf("qwen tts: session.update rejected: %v", ev)
		}
	case <-time.After(800 * time.Millisecond):
	}

	if err := client.send(ctx, map[string]any{"type": "input_text_buffer.append", "text": clean}); err != nil {
		return nil, ttsSampleRate, fmt.Errorf("qwen tts: append text: %w", err)
	}
	if err := client.send(ctx, map[string]any{"type": "input_text_buffer.commit"}); err != nil {
		return nil, ttsSampleRate, fmt.Errorf("qwen tts: commit: %w", err)
	}

	const idleTimeout = 20 * time.Second
	var pcm []byte
	deadline := time.NewTimer(idleTimeout)
	defer deadline.Stop()

	// How long the provider takes to produce its *first* audio, as distinct
	// from all of it. The gap between the two is latency this batch-shaped
	// interface throws away — the caller can't start playing until
	// Synthesize returns — so it's worth being able to see in a trace.
	started := time.Now()
	var firstDelta time.Duration
	defer func() {
		span := trace.SpanFromContext(ctx)
		span.SetAttributes(
			attribute.Int64("tts.first_delta_ms", firstDelta.Milliseconds()),
			attribute.Int64("tts.total_ms", time.Since(started).Milliseconds()),
		)
	}()

	for {
		select {
		case ev, ok := <-client.events:
			if !ok {
				return pcm, ttsSampleRate, nil
			}
			switch ev.typ() {
			case "response.audio.delta":
				if delta := ev.str("delta"); delta != "" {
					if firstDelta == 0 {
						firstDelta = time.Since(started)
					}
					decoded, derr := base64.StdEncoding.DecodeString(delta)
					if derr == nil {
						pcm = append(pcm, decoded...)
					}
					if !deadline.Stop() {
						<-deadline.C
					}
					deadline.Reset(idleTimeout)
				}
			case "response.done":
				return pcm, ttsSampleRate, nil
			case "error":
				return pcm, ttsSampleRate, fmt.Errorf("qwen tts: provider error: %v", ev)
			}
		case <-deadline.C:
			return pcm, ttsSampleRate, fmt.Errorf("qwen tts: synthesis idle-timeout after %s", idleTimeout)
		case <-ctx.Done():
			return pcm, ttsSampleRate, ctx.Err()
		}
	}
}
