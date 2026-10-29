// Package audio provides the small amount of raw-audio plumbing the voice
// agent needs: PCM16 <-> byte conversion, linear resampling, stereo
// downmixing, minimal WAV I/O, and a lightweight energy-based VAD.
package audio

import "encoding/binary"

// BytesToInt16 decodes little-endian PCM16 bytes into samples.
func BytesToInt16(b []byte) []int16 {
	n := len(b) / 2
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

// Int16ToBytes encodes samples as little-endian PCM16 bytes.
func Int16ToBytes(samples []int16) []byte {
	out := make([]byte, len(samples)*2)
	for i, s := range samples {
		binary.LittleEndian.PutUint16(out[i*2:], uint16(s))
	}
	return out
}

// StereoToMono downmixes interleaved stereo samples by averaging channels.
func StereoToMono(samples []int16) []int16 {
	n := len(samples) / 2
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		l, r := int32(samples[i*2]), int32(samples[i*2+1])
		out[i] = int16((l + r) / 2)
	}
	return out
}

// DownmixToMono averages an arbitrary number of interleaved channels down
// to one.
func DownmixToMono(samples []int16, channels int) []int16 {
	if channels <= 1 {
		return samples
	}
	n := len(samples) / channels
	out := make([]int16, n)
	for i := 0; i < n; i++ {
		var sum int32
		for c := 0; c < channels; c++ {
			sum += int32(samples[i*channels+c])
		}
		out[i] = int16(sum / int32(channels))
	}
	return out
}

// Resample linearly interpolates mono PCM16 samples from fromRate to toRate.
// It's not a high-quality resampler, but it's dependency-free and more than
// good enough for speech at the sample rates this project uses
// (16k/24k/48k).
func Resample(samples []int16, fromRate, toRate int) []int16 {
	if fromRate == toRate || len(samples) == 0 {
		return samples
	}
	outLen := int(int64(len(samples)) * int64(toRate) / int64(fromRate))
	if outLen <= 0 {
		return nil
	}
	out := make([]int16, outLen)
	ratio := float64(fromRate) / float64(toRate)
	for i := range out {
		srcPos := float64(i) * ratio
		idx := int(srcPos)
		frac := srcPos - float64(idx)
		if idx >= len(samples)-1 {
			out[i] = samples[len(samples)-1]
			continue
		}
		a, b := float64(samples[idx]), float64(samples[idx+1])
		out[i] = int16(a + (b-a)*frac)
	}
	return out
}

// ResampleIf resamples only when rates differ; returns the input unchanged
// otherwise. This avoids the repeated if-from-!=to guard at every call site.
func ResampleIf(samples []int16, from, to int) []int16 {
	if from == to {
		return samples
	}
	return Resample(samples, from, to)
}
