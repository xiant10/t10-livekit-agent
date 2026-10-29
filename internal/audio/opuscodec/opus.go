// Package opuscodec wraps libopus (via cgo) for encoding/decoding the fixed
// 20ms mono frames a LiveKit audio track carries. It's kept out of
// internal/audio proper (and therefore out of internal/engine's import
// graph) because — even though the codec itself has no LiveKit dependency —
// its framing (48kHz RTP clock, 960-sample frames) is an artifact of
// WebRTC/RTP, not of the STT→LLM→TTS pipeline; only the internal/livekit
// transport adapter needs it.
package opuscodec

import hopus "github.com/hraban/opus"

// LiveKit audio tracks are WebRTC/Opus, whose RTP clock is fixed at 48kHz
// regardless of the audio's actual content rate. 20ms frames are the
// conventional WebRTC packetization interval.
const (
	SampleRate = 48000
	Channels   = 1 // actual encoded/decoded content: mono
	FrameMS    = 20
	// RTPChannels is what RFC 7587 requires RTP codec negotiation to
	// declare (always 2, regardless of actual content channel count) — use
	// this, not Channels, when building a webrtc.RTPCodecCapability.
	RTPChannels  = 2
	FrameSamples = SampleRate * FrameMS / 1000 // 960
)

// Encoder encodes fixed-size 20ms mono frames at SampleRate.
type Encoder struct {
	enc *hopus.Encoder
}

func NewEncoder() (*Encoder, error) {
	enc, err := hopus.NewEncoder(SampleRate, Channels, hopus.AppVoIP)
	if err != nil {
		return nil, err
	}
	return &Encoder{enc: enc}, nil
}

// EncodeFrame encodes exactly FrameSamples samples of mono PCM16. Shorter
// frames are zero-padded; longer frames are truncated.
func (e *Encoder) EncodeFrame(pcm []int16) ([]byte, error) {
	frame := pcm
	if len(frame) != FrameSamples {
		frame = make([]int16, FrameSamples)
		copy(frame, pcm)
	}
	buf := make([]byte, 4000) // generous upper bound for one Opus frame
	n, err := e.enc.Encode(frame, buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

// Decoder decodes Opus packets back to mono PCM16 at SampleRate.
type Decoder struct {
	dec *hopus.Decoder
}

func NewDecoder() (*Decoder, error) {
	dec, err := hopus.NewDecoder(SampleRate, Channels)
	if err != nil {
		return nil, err
	}
	return &Decoder{dec: dec}, nil
}

// DecodeFrame decodes one Opus packet. The output buffer is sized generously
// since a single packet can (rarely) encode up to 120ms of audio.
func (d *Decoder) DecodeFrame(packet []byte) ([]int16, error) {
	pcm := make([]int16, FrameSamples*6)
	n, err := d.dec.Decode(packet, pcm)
	if err != nil {
		return nil, err
	}
	return pcm[:n], nil
}
