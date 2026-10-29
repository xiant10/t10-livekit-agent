package qwen

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
)

func init() {
	provider.RegisterSTT("qwen", func(cfg config.Config) (provider.STTProvider, error) {
		if cfg.DashScopeAPIKey == "" {
			return nil, fmt.Errorf("qwen: DASHSCOPE_API_KEY is required")
		}
		return &STT{
			wsURL:  cfg.DashScopeWSURL,
			apiKey: cfg.DashScopeAPIKey,
			model:  cfg.QwenSTTModel,
		}, nil
	})
}

// STT is a provider.STTProvider backed by DashScope's Qwen Omni-Realtime
// transcription API. Each utterance (batch call or streaming session) opens
// its own WebSocket connection and closes it when done — simpler than the
// Python original's persistent shared-connection pool, at the cost of one
// extra handshake per turn.
type STT struct {
	wsURL  string
	apiKey string
	model  string
}

const sttResultTimeout = 10 * time.Second

func (p *STT) sessionUpdate(lang string) map[string]any {
	transcription := map[string]any{}
	if lang != "" {
		transcription["language"] = lang
	}
	return map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"modalities":                []string{"text"},
			"input_audio_format":        "pcm",
			"sample_rate":               16000,
			"turn_detection":            nil, // we drive segmentation ourselves (VAD or a single batch)
			"input_audio_transcription": transcription,
		},
	}
}

func (p *STT) TranscribeBatch(ctx context.Context, pcm16Mono16k []byte, lang string) (string, error) {
	if len(pcm16Mono16k) == 0 {
		return "", nil
	}

	client, err := dialRealtime(ctx, p.wsURL, p.model, p.apiKey)
	if err != nil {
		return "", err
	}
	defer client.close()

	if err := client.send(ctx, p.sessionUpdate(lang)); err != nil {
		return "", fmt.Errorf("qwen stt: session.update: %w", err)
	}
	if _, err := waitForType(ctx, client, "session.updated", 30*time.Second); err != nil {
		return "", err
	}

	const chunkSize = 96_000 // bytes of raw PCM per append event
	for offset := 0; offset < len(pcm16Mono16k); offset += chunkSize {
		end := min(offset+chunkSize, len(pcm16Mono16k))
		chunk := pcm16Mono16k[offset:end]
		err := client.send(ctx, map[string]any{
			"type":  "input_audio_buffer.append",
			"audio": base64.StdEncoding.EncodeToString(chunk),
		})
		if err != nil {
			return "", fmt.Errorf("qwen stt: append audio: %w", err)
		}
	}
	if err := client.send(ctx, map[string]any{"type": "input_audio_buffer.commit"}); err != nil {
		return "", fmt.Errorf("qwen stt: commit: %w", err)
	}

	audioSeconds := float64(len(pcm16Mono16k)) / 32000.0 // 16kHz * 2 bytes/sample
	timeout := max(sttResultTimeout, time.Duration(audioSeconds*1.5*float64(time.Second))+3*time.Second)
	return consumeTranscript(ctx, client, timeout)
}

// consumeTranscript drains events until an authoritative transcript arrives.
func consumeTranscript(ctx context.Context, client *realtimeClient, timeout time.Duration) (string, error) {
	var preview string
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	for {
		select {
		case ev, ok := <-client.events:
			if !ok {
				return preview, nil
			}
			switch ev.typ() {
			case "conversation.item.input_audio_transcription.text", "conversation.item.input_audio_transcription.delta":
				updatePreview(&preview, ev)
			case "conversation.item.input_audio_transcription.completed":
				transcript := ev.str("transcript")
				return pick(transcript, preview), nil
			case "response.done":
				return preview, nil
			case "error":
				return preview, fmt.Errorf("qwen stt: provider error: %v", ev)
			}
		case <-deadline.C:
			return preview, nil
		case <-ctx.Done():
			return preview, ctx.Err()
		}
	}
}

func pick(candidates ...string) string {
	for _, c := range candidates {
		if c != "" {
			return c
		}
	}
	return ""
}

// stream is a live, incremental transcription session (one per VAD-bounded
// speech segment in live-call mode).
type stream struct {
	pcmCh         chan []byte
	stopCh        chan struct{}
	stopOnce      sync.Once
	senderDone    chan struct{}
	resultCh      chan sttResult
	resultTimeout time.Duration
	done          atomic.Bool
	onInterim     func(delta string)

	// readyCh closes once the dial goroutine has either produced a usable
	// client or given up (dialErr set). NewStream returns before this
	// closes — see its comment for why that matters.
	readyCh chan struct{}
	client  *realtimeClient
	dialErr error
}

type sttResult struct {
	transcript string
	err        error
}

// NewStream returns immediately, without waiting for the WebSocket
// handshake: the dial happens on its own goroutine while this stream's
// PushPCM already accepts and buffers audio. This matters because the
// caller (engine.LiveCallController.Run) invokes NewStream synchronously
// from its real-time audio-read loop on every speech-start, including
// barge-in — blocking that loop on a network round-trip would stall
// draining the live RTP track at exactly the moment responsiveness matters
// most. A dial failure surfaces later, from Commit, instead of here.
func (p *STT) NewStream(ctx context.Context, lang string, onInterim func(delta string)) (provider.STTStream, error) {
	s := &stream{
		pcmCh:         make(chan []byte, 4096),
		stopCh:        make(chan struct{}),
		senderDone:    make(chan struct{}),
		resultCh:      make(chan sttResult, 1),
		resultTimeout: sttResultTimeout,
		readyCh:       make(chan struct{}),
		onInterim:     onInterim,
	}
	go s.dial(ctx, p, lang)
	return s, nil
}

func (s *stream) dial(ctx context.Context, p *STT, lang string) {
	defer close(s.readyCh)

	client, err := dialRealtime(ctx, p.wsURL, p.model, p.apiKey)
	if err != nil {
		s.dialErr = err
		close(s.senderDone)
		return
	}
	if err := client.send(ctx, p.sessionUpdate(lang)); err != nil {
		s.dialErr = fmt.Errorf("qwen stt: session.update: %w", err)
		client.close()
		close(s.senderDone)
		return
	}
	if _, err := waitForType(ctx, client, "session.updated", 30*time.Second); err != nil {
		s.dialErr = err
		client.close()
		close(s.senderDone)
		return
	}

	s.client = client
	go s.runSender()
	go s.runEventConsumer()
}

func (s *stream) PushPCM(pcm []byte) {
	if s.done.Load() || len(pcm) == 0 {
		return
	}
	select {
	case s.pcmCh <- pcm:
	case <-s.stopCh:
	default:
		// Backpressure: drop rather than block the live-audio reader.
	}
}

func (s *stream) runSender() {
	defer close(s.senderDone)
	send := func(pcm []byte) {
		_ = s.client.send(context.Background(), map[string]any{
			"type":  "input_audio_buffer.append",
			"audio": base64.StdEncoding.EncodeToString(pcm),
		})
	}
	for {
		select {
		case pcm := <-s.pcmCh:
			send(pcm)
		case <-s.stopCh:
			for {
				select {
				case pcm := <-s.pcmCh:
					send(pcm)
				default:
					return
				}
			}
		}
	}
}

func (s *stream) runEventConsumer() {
	var preview string
	for ev := range s.client.events {
		switch ev.typ() {
		case "conversation.item.input_audio_transcription.text", "conversation.item.input_audio_transcription.delta":
			if updatePreview(&preview, ev) && s.onInterim != nil {
				s.onInterim(preview)
			}
		case "conversation.item.input_audio_transcription.completed":
			s.resultCh <- sttResult{transcript: pick(ev.str("transcript"), preview)}
			return
		case "response.done":
			s.resultCh <- sttResult{transcript: preview}
			return
		case "error":
			s.resultCh <- sttResult{err: fmt.Errorf("qwen stt: provider error: %v", ev)}
			return
		}
	}
	// Connection closed without a terminal event.
	s.resultCh <- sttResult{transcript: preview}
}

func (s *stream) Commit(ctx context.Context) (string, error) {
	if !s.done.CompareAndSwap(false, true) {
		return "", fmt.Errorf("qwen stt: stream already finished")
	}
	s.stopOnce.Do(func() { close(s.stopCh) })

	select {
	case <-s.readyCh:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if s.dialErr != nil {
		return "", fmt.Errorf("qwen stt: dial: %w", s.dialErr)
	}
	defer s.client.close()

	<-s.senderDone
	if err := s.client.send(ctx, map[string]any{"type": "input_audio_buffer.commit"}); err != nil {
		return "", fmt.Errorf("qwen stt: commit: %w", err)
	}

	select {
	case res := <-s.resultCh:
		return res.transcript, res.err
	case <-time.After(s.resultTimeout):
		return "", fmt.Errorf("qwen stt: result timeout")
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (s *stream) Abort() {
	if !s.done.CompareAndSwap(false, true) {
		return
	}
	s.stopOnce.Do(func() { close(s.stopCh) })
	// Don't block the caller (engine.LiveCallController.Run's hot loop, on
	// barge-in) on a dial that may still be in flight.
	go func() {
		<-s.readyCh
		if s.client != nil {
			s.client.close()
		}
	}()
}

// Qwen's text/stash fields are replacement snapshots: confirmed prefix plus
// tentative suffix. Appending either would duplicate words and prevent corrections.
// Some compatible endpoints instead send a literal incremental delta string.
func updatePreview(preview *string, ev realtimeEvent) bool {
	previous := *preview
	_, hasText := ev["text"]
	_, hasStash := ev["stash"]
	if hasText || hasStash {
		*preview = ev.str("text") + ev.str("stash")
	} else {
		*preview += ev.str("delta")
	}
	return *preview != previous
}
