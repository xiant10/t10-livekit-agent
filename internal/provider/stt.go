// Package provider defines capability-agnostic interfaces for the
// speech-to-text, LLM, and text-to-speech stages of the voice agent
// pipeline, plus a small registry so concrete implementations (Qwen,
// OpenAI-compatible, etc.) can be swapped via configuration alone.
package provider

import "context"

// STTStream is an incremental transcription session for one utterance
// (e.g. one VAD-bounded speech segment in live-call mode).
type STTStream interface {
	// PushPCM enqueues 16kHz mono PCM16 audio for transcription.
	PushPCM(pcm []byte)
	// Commit signals end-of-speech and returns the final transcript.
	Commit(ctx context.Context) (string, error)
	// Abort cancels the session without waiting for a result (barge-in).
	Abort()
}

// STTProvider turns speech into text, either in one shot (voice-message
// mode) or incrementally (live-call mode).
type STTProvider interface {
	// TranscribeBatch transcribes a complete utterance already available
	// as 16kHz mono PCM16.
	TranscribeBatch(ctx context.Context, pcm16Mono16k []byte, lang string) (string, error)
	// NewStream opens an incremental transcription session. onInterim, if
	// non-nil, is called zero or more times with full replacement transcript snapshots as
	// the provider recognizes them, before Commit returns — lets a caller
	// show live captions while the user is still talking. It's called from
	// whatever goroutine the provider uses to consume its own transport, so
	// must not block; the text Commit ultimately returns is authoritative
	// and may correct words these snapshots guessed wrong.
	NewStream(ctx context.Context, lang string, onInterim func(snapshot string)) (STTStream, error)
}
