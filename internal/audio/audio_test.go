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

// feed runs alternating loud/quiet stretches (in ms, loud first) through v in
// 20ms frames and reports whether speech started.
func feed(v *VAD, stretches ...int) bool {
	loud, quiet := make([]int16, 320), make([]int16, 320)
	for i := range loud {
		loud[i] = 3000
	}
	started := false
	for i, ms := range stretches {
		frame := loud
		if i%2 == 1 {
			frame = quiet
		}
		for ; ms > 0; ms -= 20 {
			started = v.Process(frame, 16000) == EventSpeechStart || started
		}
	}
	return started
}

func TestVADCountsSpeechAcrossSyllableGaps(t *testing.T) {
	// Measured from "换个话题吧": about a second of speech, never 400ms unbroken.
	if !feed(&VAD{EnergyThreshold: 500, MinSpeechMS: 400, MinSilenceMS: 700}, 180, 40, 140, 20, 160, 60, 120) {
		t.Fatal("a short phrase did not count as speech")
	}
}

func TestVADStillIgnoresACough(t *testing.T) {
	for name, stretches := range map[string][]int{
		"one cough":                  {300},
		"two coughs, a breath apart": {200, 300, 200},
		"tapping":                    {60, 200, 60, 200, 60, 200, 60, 200, 60},
	} {
		if feed(&VAD{EnergyThreshold: 500, MinSpeechMS: 400, MinSilenceMS: 700}, stretches...) {
			t.Fatalf("%s counted as speech", name)
		}
	}
}

func TestStreamResamplerMatchesWholeInputHoweverItIsSplit(t *testing.T) {
	input := make([]int16, 240)
	for i := range input {
		input[i] = int16(8000 * math.Sin(float64(i)/7))
	}
	for _, rates := range [][2]int{{24000, 48000}, {24000, 16000}, {16000, 48000}} {
		whole := NewStreamResampler(rates[0], rates[1]).Push(input)
		for _, size := range []int{1, 7, 100} {
			r := NewStreamResampler(rates[0], rates[1])
			var pieces []int16
			for start := 0; start < len(input); start += size {
				pieces = append(pieces, r.Push(input[start:min(start+size, len(input))])...)
			}
			// Accumulated position can round one trailing sample either way.
			if d := len(pieces) - len(whole); d < -1 || d > 1 {
				t.Fatalf("%v split %d: %d samples, want %d", rates, size, len(pieces), len(whole))
			}
			for i := range min(len(pieces), len(whole)) {
				if d := int(pieces[i]) - int(whole[i]); d < -1 || d > 1 {
					t.Fatalf("%v split %d: sample %d = %d, want %d", rates, size, i, pieces[i], whole[i])
				}
			}
		}
	}
}

func TestStreamResamplerSameRatePassesThrough(t *testing.T) {
	in := []int16{1, 2, 3}
	if got := NewStreamResampler(48000, 48000).Push(in); len(got) != 3 || got[2] != 3 {
		t.Fatalf("got %v", got)
	}
}
