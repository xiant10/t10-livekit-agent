package audio

import (
	"math"
	"testing"
)

func TestWAVRoundTrip(t *testing.T) {
	samples := []int16{0, 100, -100, 32767, -32768, 42}
	encoded := EncodeWAV(samples, 16000, 1)

	decoded, sampleRate, channels, err := DecodeWAV(encoded)
	if err != nil {
		t.Fatalf("DecodeWAV: %v", err)
	}
	if sampleRate != 16000 || channels != 1 {
		t.Fatalf("got sampleRate=%d channels=%d, want 16000/1", sampleRate, channels)
	}
	if len(decoded) != len(samples) {
		t.Fatalf("got %d samples, want %d", len(decoded), len(samples))
	}
	for i := range samples {
		if decoded[i] != samples[i] {
			t.Fatalf("sample %d: got %d, want %d", i, decoded[i], samples[i])
		}
	}
}

func TestResampleIdentity(t *testing.T) {
	samples := []int16{1, 2, 3, 4, 5}
	out := Resample(samples, 16000, 16000)
	if len(out) != len(samples) {
		t.Fatalf("identity resample changed length: got %d want %d", len(out), len(samples))
	}
}

func TestResampleLengthScales(t *testing.T) {
	samples := make([]int16, 1600) // 100ms @ 16kHz
	out := Resample(samples, 16000, 48000)
	want := 4800 // 100ms @ 48kHz
	if diff := abs(len(out) - want); diff > 2 {
		t.Fatalf("upsampled length = %d, want ~%d", len(out), want)
	}
}

func TestVADSpeechBoundaries(t *testing.T) {
	v := &VAD{EnergyThreshold: 500, MinSpeechMS: 40, MinSilenceMS: 60}
	const sampleRate = 16000
	frameSamples := sampleRate / 50 // 20ms frames

	loud := make([]int16, frameSamples)
	for i := range loud {
		loud[i] = int16(10000 * math.Sin(float64(i)))
	}
	quiet := make([]int16, frameSamples)

	var events []Event
	// 3 loud frames (60ms) should cross the 40ms MinSpeechMS threshold.
	for i := 0; i < 3; i++ {
		if ev := v.Process(loud, sampleRate); ev != EventNone {
			events = append(events, ev)
		}
	}
	if len(events) != 1 || events[0] != EventSpeechStart {
		t.Fatalf("expected exactly one EventSpeechStart, got %v", events)
	}
	if !v.Speaking() {
		t.Fatalf("expected Speaking()==true after speech start")
	}

	events = nil
	// 4 quiet frames (80ms) should cross the 60ms MinSilenceMS threshold.
	for i := 0; i < 4; i++ {
		if ev := v.Process(quiet, sampleRate); ev != EventNone {
			events = append(events, ev)
		}
	}
	if len(events) != 1 || events[0] != EventSpeechEnd {
		t.Fatalf("expected exactly one EventSpeechEnd, got %v", events)
	}
	if v.Speaking() {
		t.Fatalf("expected Speaking()==false after speech end")
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
