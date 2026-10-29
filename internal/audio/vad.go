package audio

import "math"

// Event is a speech-boundary transition reported by VAD.Process.
type Event int

const (
	EventNone Event = iota
	EventSpeechStart
	EventSpeechEnd
)

// VAD is a small energy-threshold voice activity detector with debouncing:
// speech must persist for MinSpeechMS before EventSpeechStart fires, and
// silence must persist for MinSilenceMS before EventSpeechEnd fires. This is
// a deliberate simplification of the Python original's neural (Silero) VAD
// — good enough to drive turn-taking in a demo, swappable later behind the
// same Process method signature if a real model is wanted.
type VAD struct {
	EnergyThreshold float64
	MinSpeechMS     int
	MinSilenceMS    int

	speaking  bool
	speechMS  int
	silenceMS int
}

// rms returns the root-mean-square amplitude of a PCM16 frame.
func rms(frame []int16) float64 {
	if len(frame) == 0 {
		return 0
	}
	var sumSq float64
	for _, s := range frame {
		v := float64(s)
		sumSq += v * v
	}
	return math.Sqrt(sumSq / float64(len(frame)))
}

// Process feeds one frame (mono PCM16 at sampleRate) through the detector
// and returns any boundary event that just occurred.
func (v *VAD) Process(frame []int16, sampleRate int) Event {
	if len(frame) == 0 || sampleRate <= 0 {
		return EventNone
	}
	frameMS := len(frame) * 1000 / sampleRate
	loud := rms(frame) >= v.EnergyThreshold

	if loud {
		v.silenceMS = 0
		v.speechMS += frameMS
		if !v.speaking && v.speechMS >= v.MinSpeechMS {
			v.speaking = true
			return EventSpeechStart
		}
		return EventNone
	}

	v.speechMS = 0
	if v.speaking {
		v.silenceMS += frameMS
		if v.silenceMS >= v.MinSilenceMS {
			v.speaking = false
			v.silenceMS = 0
			return EventSpeechEnd
		}
	}
	return EventNone
}

// Speaking reports whether the detector currently considers the signal to
// be mid-utterance.
func (v *VAD) Speaking() bool { return v.speaking }

func (v *VAD) SetMinSpeechMS(ms int) { v.MinSpeechMS = ms }
