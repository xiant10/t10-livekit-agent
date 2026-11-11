package provider

import "context"

// TTSProvider synthesizes speech audio for a piece of text, returning raw
// PCM16 mono samples at sampleRate.
type TTSProvider interface {
	Synthesize(ctx context.Context, text, voice string) (pcm []byte, sampleRate int, err error)
}

// StreamingTTS is an optional TTSProvider capability for callers that can use
// audio before the whole text has been synthesized.
type StreamingTTS interface {
	TTSProvider
	// SynthesizeStream calls onAudio with PCM16 mono samples as they arrive,
	// in order and from one goroutine, and returns when synthesis ends. Audio
	// already delivered stays delivered if it then returns an error.
	SynthesizeStream(ctx context.Context, text, voice string, onAudio func(pcm []byte, sampleRate int)) error
	// Warm starts preparing a connection for a synthesis expected shortly and
	// returns at once. It is only a hint: nothing fails if it is never used.
	// ctx is the call's: a session warmed for one call, with its tenant's
	// key, never serves another.
	Warm(ctx context.Context, voice string)
}
