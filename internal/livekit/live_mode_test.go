package livekit

import (
	"t10-livekit-agent/internal/audio/opuscodec"
	"testing"
	"time"
)

func TestTrackAudioSinkPlaysPartialTailAndBecomesIdle(t *testing.T) {
	s := &trackAudioSink{}
	samples := make([]int16, opuscodec.FrameSamples+7)
	for i := range samples {
		samples[i] = int16(i + 1)
	}
	s.PushPCM(samples)
	first := s.takeFrame()
	if len(first) != opuscodec.FrameSamples || first[0] != 1 {
		t.Fatal("first frame lost")
	}
	tail := s.takeFrame()
	if len(tail) != opuscodec.FrameSamples || tail[6] != samples[len(samples)-1] || tail[7] != 0 {
		t.Fatal("partial final frame was not preserved and padded")
	}
	if s.QueuedDuration() != 0 || s.takeFrame() != nil {
		t.Fatal("tail prevented idle")
	}
}

func TestTrackAudioSinkLargePushDoesNotBlockAndDrainClearsTail(t *testing.T) {
	s := &trackAudioSink{}
	done := make(chan struct{})
	go func() { s.PushPCM(make([]int16, 21*opuscodec.SampleRate+7)); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("push blocked behind a full encoded frame channel")
	}
	if s.QueuedDuration() < 21*time.Second {
		t.Fatal("queued duration lost audio")
	}
	s.Drain()
	if s.QueuedDuration() != 0 || s.takeFrame() != nil {
		t.Fatal("drain left stale audio")
	}
	s.PushPCM([]int16{42})
	if frame := s.takeFrame(); frame[0] != 42 {
		t.Fatal("next turn was lost after drain")
	}
}
