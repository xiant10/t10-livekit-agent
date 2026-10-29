package engine

import (
	"time"

	"t10-livekit-agent/internal/audio"
)

// ReplySink receives a streamed LLM reply, one delta at a time, and is
// closed once the reply is complete. internal/livekit's implementation
// wraps a LiveKit text-stream writer; a different transport would wrap
// whatever it uses to push incremental text to a client.
type ReplySink interface {
	WriteToken(delta string)
	Close()
}

// TranscriptSink receives complete, replaceable interim hypotheses. Unlike a
// ReplySink, its updates may correct or remove previously displayed words.
type TranscriptSink interface {
	WriteSnapshot(text string)
	Close()
}

// AudioSource is live inbound audio: mono PCM16 samples at a fixed
// SampleRate, delivered frame by frame as they arrive. ReadFrame blocks
// until the next frame is available and returns an error (e.g. io.EOF) once
// the source has ended — mirroring the shape of a blocking track/socket
// read loop, whatever the underlying transport actually is.
type AudioSource interface {
	SampleRate() int
	ReadFrame() ([]int16, error)
}

// AudioSink is live outbound audio: mono PCM16 samples at a fixed
// SampleRate. PushPCM may buffer and return before the audio has actually
// played. PushPCM and Drain must return promptly: live-call floor changes
// serialize with enqueueing, so neither method may wait for playback or
// network I/O. Callers must resample to SampleRate() themselves (see
// internal/audio.Resample) since the sink only deals in its own rate.
type AudioSink interface {
	SampleRate() int
	PushPCM(samples []int16)
	// Drain discards any queued-but-unplayed audio — used for barge-in.
	Drain()
	// QueuedDuration reports how much pushed-but-not-yet-played audio the
	// sink is still holding, at its own real-time playout pace. PushPCM
	// returning only means audio was *queued*; for a reply pipelined ahead
	// of playback that can be many seconds before it's been heard. The
	// live-call loop uses this for two things: to keep a turn
	// barge-in-eligible for as long as it is still audibly speaking, and to
	// cap how far ahead of the speaker its own caption text may run (see
	// LiveCallController.playSpeech).
	QueuedDuration() time.Duration
}

// VADProcessor detects speech boundaries in a PCM16 audio stream.
// Each call to Process feeds one frame and returns any boundary event.
// The default implementation is audio.VAD (energy-based); a neural VAD
// (Silero, WebRTC) can be injected by implementing this interface.
type VADProcessor interface {
	Process(frame []int16, sampleRate int) audio.Event
	Speaking() bool
	// SetMinSpeechMS adjusts the speech-start debounce. The live-call loop
	// swaps between responsive (fresh turn) and strict (barge-in) thresholds
	// frame-by-frame without creating a new detector.
	SetMinSpeechMS(ms int)
}
