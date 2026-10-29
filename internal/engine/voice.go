package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"t10-livekit-agent/internal/telemetry"
)

// VoiceReplySink delivers the transcript before opening the streamed reply.
// RunVoiceTurn closes the reply after synthesis and delivery finish.
type VoiceReplySink interface {
	ReplySink
	WriteTranscript(text string)
}

// VoiceAudioSink opts into ordered incremental PCM delivery. Without it,
// RunVoiceTurn returns the accumulated PCM for file-oriented callers.
type VoiceAudioSink interface{ WriteAudio([]byte, int) error }

// RunVoiceTurn serializes transcription, reply generation and audio delivery
// with text/voice turns for this identity. Speech starts at the first chunk.
func RunVoiceTurn(ctx context.Context, deps *Deps, identity string, pcm16k []byte, sink VoiceReplySink) ([]byte, int, error) {
	defer sink.Close()
	convo := deps.Conversations.Get(identity)
	release, err := convo.ReserveTurn()(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer release()
	sttCtx, sttEnd := telemetry.StartStage(ctx, "stt.transcribe_batch")
	transcript, err := deps.STT.TranscribeBatch(sttCtx, pcm16k, "")
	sttEnd(err)
	if err != nil {
		return nil, 0, fmt.Errorf("stt: %w", err)
	}
	transcript = strings.TrimSpace(transcript)
	if transcript == "" {
		transcript = "(unintelligible)"
	}
	slog.InfoContext(ctx, "voice mode: heard", "identity", identity, "text", transcript)
	sink.WriteTranscript(transcript)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	chunks := make(chan string, 2)
	type result struct {
		pcm  []byte
		rate int
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		var out result
		defer func() { finished <- out }()
		for text := range chunks {
			if ctx.Err() != nil {
				out.err = ctx.Err()
				return
			}
			ttsCtx, end := telemetry.StartStage(ctx, "tts.synthesize", attribute.Int("text.chars", len(text)))
			pcm, rate, err := deps.TTS.Synthesize(ttsCtx, text, deps.Voice)
			if err == nil && (len(pcm) == 0 || len(pcm)%2 != 0 || rate <= 0) {
				err = fmt.Errorf("invalid synthesized audio")
			}
			end(err)
			if err == nil {
				if audioSink, ok := sink.(VoiceAudioSink); ok {
					err = audioSink.WriteAudio(pcm, rate)
				} else {
					if out.rate != 0 && out.rate != rate {
						err = fmt.Errorf("sample rate changed between chunks")
					} else {
						out.pcm = append(out.pcm, pcm...)
						out.rate = rate
					}
				}
			}
			if err != nil {
				out.err = fmt.Errorf("tts: %w", err)
				cancel()
				return
			}
		}
	}()
	chunker := newSpeechChunker(80, 160)
	submit := func(text string) {
		if strings.TrimSpace(text) != "" {
			select {
			case chunks <- text:
			case <-ctx.Done():
			}
		}
	}
	replySink := voiceTokenSink{write: func(delta string) {
		sink.WriteToken(delta)
		for _, chunk := range chunker.push(delta) {
			submit(chunk)
		}
	}}
	_, chatErr := runChatTurn(ctx, deps, convo, transcript, replySink)
	if chatErr == nil {
		submit(chunker.flush())
	} else {
		cancel()
	}
	close(chunks)
	out := <-finished
	if out.err != nil && (chatErr == nil || !errors.Is(out.err, context.Canceled)) {
		return nil, 0, out.err
	}
	if chatErr != nil {
		return nil, 0, fmt.Errorf("chat turn: %w", chatErr)
	}
	return out.pcm, out.rate, out.err
}

type voiceTokenSink struct{ write func(string) }

func (s voiceTokenSink) WriteToken(text string) { s.write(text) }
func (s voiceTokenSink) Close()                 {}
