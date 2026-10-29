package audio

import (
	"encoding/binary"
	"fmt"
)

// DecodeWAV parses a PCM16 WAV file (any channel count / sample rate) and
// returns the interleaved samples. Only uncompressed PCM (format tag 1) is
// supported, which is all this project ever produces or consumes.
func DecodeWAV(data []byte) (samples []int16, sampleRate int, channels int, err error) {
	if len(data) < 12 || string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil, 0, 0, fmt.Errorf("audio: not a RIFF/WAVE file")
	}

	var (
		bitsPerSample int
		haveFmt       bool
		pcm           []byte
		haveData      bool
	)

	pos := 12
	for pos+8 <= len(data) {
		chunkID := string(data[pos : pos+4])
		chunkSize := int(binary.LittleEndian.Uint32(data[pos+4 : pos+8]))
		body := pos + 8
		if body+chunkSize > len(data) {
			chunkSize = len(data) - body // tolerate truncated trailing chunk
		}

		switch chunkID {
		case "fmt ":
			if chunkSize < 16 {
				return nil, 0, 0, fmt.Errorf("audio: fmt chunk too small")
			}
			formatTag := binary.LittleEndian.Uint16(data[body : body+2])
			if formatTag != 1 {
				return nil, 0, 0, fmt.Errorf("audio: unsupported WAV format tag %d (only PCM is supported)", formatTag)
			}
			channels = int(binary.LittleEndian.Uint16(data[body+2 : body+4]))
			sampleRate = int(binary.LittleEndian.Uint32(data[body+4 : body+8]))
			bitsPerSample = int(binary.LittleEndian.Uint16(data[body+14 : body+16]))
			haveFmt = true
		case "data":
			pcm = data[body : body+chunkSize]
			haveData = true
		}

		pos = body + chunkSize
		if chunkSize%2 == 1 {
			pos++ // chunks are word-aligned
		}
	}

	if !haveFmt || !haveData {
		return nil, 0, 0, fmt.Errorf("audio: missing fmt or data chunk")
	}
	if bitsPerSample != 16 {
		return nil, 0, 0, fmt.Errorf("audio: unsupported bits-per-sample %d (only 16-bit PCM is supported)", bitsPerSample)
	}

	return BytesToInt16(pcm), sampleRate, channels, nil
}

// EncodeWAV writes interleaved PCM16 samples as a WAV file.
func EncodeWAV(samples []int16, sampleRate, channels int) []byte {
	pcm := Int16ToBytes(samples)
	blockAlign := channels * 2
	byteRate := sampleRate * blockAlign
	dataSize := len(pcm)

	buf := make([]byte, 0, 44+dataSize)
	buf = append(buf, "RIFF"...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(36+dataSize))
	buf = append(buf, "WAVE"...)

	buf = append(buf, "fmt "...)
	buf = binary.LittleEndian.AppendUint32(buf, 16)
	buf = binary.LittleEndian.AppendUint16(buf, 1) // PCM
	buf = binary.LittleEndian.AppendUint16(buf, uint16(channels))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(sampleRate))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(byteRate))
	buf = binary.LittleEndian.AppendUint16(buf, uint16(blockAlign))
	buf = binary.LittleEndian.AppendUint16(buf, 16) // bits per sample

	buf = append(buf, "data"...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(dataSize))
	buf = append(buf, pcm...)

	return buf
}
