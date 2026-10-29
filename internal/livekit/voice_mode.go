package livekit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/roomprotocol"
	"t10-livekit-agent/internal/telemetry"
)

const (
	sttInputSampleRate = 16000
)

// newVoiceStreamHandler returns the room byte-stream handler for roomprotocol.TopicVoice.
func newVoiceStreamHandler(room *lksdk.Room, deps *engine.Deps, queue *engine.Manager) lksdk.ByteStreamHandler {
	return func(reader *lksdk.ByteStreamReader, identity string) {
		enter := queue.Get(identity).ReserveTurn()
		ctx, end := telemetry.StartSession(context.Background())
		ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
		status := newMessageStatus(ctx, room, identity, requestID(reader.Info.Attributes, reader.Info.Id))
		status.send("queued", "", 0)
		go func() {
			defer end()
			defer cancel()
			defer status.w.Close()
			sink := &voiceReplySink{ctx: ctx, room: room, identity: identity, status: status}
			release, err := enter(ctx)
			if err == nil {
				defer release()
				status.send("transcribing", "", 0)
				var data []byte
				data, err = readMessage(ctx, reader)
				if err == nil {
					err = handleVoiceMessage(ctx, deps, identity, data, sink)
				}
			}
			status.finish(err, sink.chunks)
		}()
	}
}

func handleVoiceMessage(ctx context.Context, deps *engine.Deps, identity string, wavBytes []byte, sink *voiceReplySink) error {
	samples, sampleRate, channels, err := audio.DecodeWAV(wavBytes)
	if err != nil {
		return fmt.Errorf("decode wav: %w", err)
	}
	if sampleRate <= 0 || channels <= 0 || len(samples) == 0 {
		return fmt.Errorf("empty or invalid recording")
	}
	mono := audio.DownmixToMono(samples, channels)
	pcm16k := audio.ResampleIf(mono, sampleRate, sttInputSampleRate)
	_, _, err = engine.RunVoiceTurn(ctx, deps, identity, audio.Int16ToBytes(pcm16k), sink)
	return err
}

// Open the reply lazily so the transcript is delivered before any reply stream.
type voiceReplySink struct {
	ctx      context.Context
	status   *messageStatus
	chunks   int
	room     *lksdk.Room
	identity string
	reply    engine.ReplySink
}

func (s *voiceReplySink) WriteTranscript(text string) {
	w := s.room.LocalParticipant.StreamText(lksdk.StreamTextOptions{
		Topic: roomprotocol.TopicTranscript, DestinationIdentities: []string{s.identity},
		Attributes: map[string]string{roomprotocol.AttrRequestID: s.status.id},
	})
	_ = writeMessage(s.ctx, w.Write, text)
	w.Close()
	s.status.send("thinking", "", 0)
	s.reply = newChatReplySink(s.ctx, s.room, s.identity, s.status.id)
}
func (s *voiceReplySink) WriteToken(delta string) { s.reply.WriteToken(delta) }
func (s *voiceReplySink) Close() {
	if s.reply != nil {
		s.reply.Close()
	}
}

func (s *voiceReplySink) WriteAudio(pcm []byte, rate int) error {
	if s.chunks == 0 {
		s.status.send("audio_ready", "", 0)
	}
	w := s.room.LocalParticipant.StreamBytes(lksdk.StreamBytesOptions{
		Topic: roomprotocol.TopicVoice, MimeType: "audio/wav", DestinationIdentities: []string{s.identity},
		Attributes: map[string]string{roomprotocol.AttrRequestID: s.status.id, roomprotocol.AttrChunkIndex: strconv.Itoa(s.chunks)},
	})
	defer w.Close()
	if err := writeMessage(s.ctx, w.Write, audio.EncodeWAV(audio.BytesToInt16(pcm), rate, 1)); err != nil {
		return err
	}
	s.chunks++
	return nil
}
