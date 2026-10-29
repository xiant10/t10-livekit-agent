package provider

import "context"

// TTSProvider synthesizes speech audio for a piece of text, returning raw
// PCM16 mono samples at sampleRate.
type TTSProvider interface {
	Synthesize(ctx context.Context, text, voice string) (pcm []byte, sampleRate int, err error)
}
