package livekit

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lkproto "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/rtp/codecs"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/pion/webrtc/v4/pkg/media/samplebuilder"

	"t10-livekit-agent/internal/audio/opuscodec"
	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/roomprotocol"
)

// trackAudioSource adapts a subscribed remote audio track to
// engine.AudioSource, reassembling RTP packets through a jitter buffer
// (samplebuilder) and decoding Opus frames into PCM16.
type trackAudioSource struct {
	track   *webrtc.TrackRemote
	decoder *opuscodec.Decoder
	builder *samplebuilder.SampleBuilder
}

func newTrackAudioSource(track *webrtc.TrackRemote, decoder *opuscodec.Decoder) *trackAudioSource {
	return &trackAudioSource{
		track:   track,
		decoder: decoder,
		builder: samplebuilder.New(50, &codecs.OpusPacket{}, opuscodec.SampleRate),
	}
}

func (s *trackAudioSource) SampleRate() int { return opuscodec.SampleRate }

func (s *trackAudioSource) ReadFrame() ([]int16, error) {
	for {
		if sample := s.builder.Pop(); sample != nil {
			pcm, err := s.decoder.DecodeFrame(sample.Data)
			if err != nil {
				continue // skip an undecodable packet rather than failing the call
			}
			return pcm, nil
		}
		pkt, _, err := s.track.ReadRTP()
		if err != nil {
			return nil, err
		}
		s.builder.Push(pkt)
	}
}

// trackAudioSink adapts a published local track to engine.AudioSink,
// buffering pushed PCM into fixed 20ms Opus frames and playing them out at
// real-time pace on its own goroutine (emitting silence when idle, since a
// WebRTC track is expected to keep flowing packets).
//
// PCM is queued under a short mutex; only the writer encodes it. Drain
// therefore cannot race with a producer enqueuing previously encoded audio.
// The writer pads the final partial frame, so every queued sample can play.
type trackAudioSink struct {
	track   *lksdk.LocalTrack
	encoder *opuscodec.Encoder

	mu  sync.Mutex
	buf []int16
}

func newTrackAudioSink(ctx context.Context, track *lksdk.LocalTrack, encoder *opuscodec.Encoder) (*trackAudioSink, error) {
	silence, err := encoder.EncodeFrame(make([]int16, opuscodec.FrameSamples))
	if err != nil {
		return nil, fmt.Errorf("prime silence frame: %w", err)
	}
	s := &trackAudioSink{
		track:   track,
		encoder: encoder,
	}
	go s.runWriter(ctx, silence)
	return s, nil
}

func (s *trackAudioSink) SampleRate() int { return opuscodec.SampleRate }

func (s *trackAudioSink) PushPCM(samples []int16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = append(s.buf, samples...)
}

func (s *trackAudioSink) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.buf = nil
}

func (s *trackAudioSink) QueuedDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Duration(len(s.buf)) * time.Second / opuscodec.SampleRate
}

// takeFrame pads the tail with silence instead of retaining an unplayable
// remainder forever (which would prevent the turn from returning to idle).
func (s *trackAudioSink) takeFrame() []int16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buf) == 0 {
		return nil
	}
	frame := make([]int16, opuscodec.FrameSamples)
	n := copy(frame, s.buf)
	s.buf = s.buf[n:]
	if len(s.buf) == 0 {
		s.buf = nil
	}
	return frame
}

func (s *trackAudioSink) runWriter(ctx context.Context, silence []byte) {
	ticker := time.NewTicker(opuscodec.FrameMS * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			frame := silence
			if pcm := s.takeFrame(); pcm != nil {
				if encoded, err := s.encoder.EncodeFrame(pcm); err == nil {
					frame = encoded
				}
			}
			_ = s.track.WriteSample(media.Sample{Data: frame, Duration: opuscodec.FrameMS * time.Millisecond}, nil)
		}
	}
}

// startLiveCall publishes the agent's outgoing audio track, wires up the
// AudioSource/AudioSink adapters, and runs the engine's live-call turn loop
// until remote's track ends. It blocks, so callers invoke it in its own
// goroutine. sessionCtx carries the call's correlation ID (see
// telemetry.StartSession); every context derived from it below, and hence
// every log line the engine emits while running this call, carries it too.
func startLiveCall(sessionCtx context.Context, room *lksdk.Room, deps *engine.Deps, liveCfg engine.LiveConfig, identity string, remote *webrtc.TrackRemote) error {
	decoder, err := opuscodec.NewDecoder()
	if err != nil {
		return fmt.Errorf("opus decoder: %w", err)
	}
	encoder, err := opuscodec.NewEncoder()
	if err != nil {
		return fmt.Errorf("opus encoder: %w", err)
	}

	localTrack, err := lksdk.NewLocalSampleTrack(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: opuscodec.SampleRate,
		Channels:  opuscodec.RTPChannels,
	})
	if err != nil {
		return fmt.Errorf("create local track: %w", err)
	}
	pub, err := room.LocalParticipant.PublishTrack(localTrack, &lksdk.TrackPublicationOptions{
		Name:   "agent-voice",
		Source: lkproto.TrackSource_MICROPHONE,
	})
	if err != nil {
		return fmt.Errorf("publish local track: %w", err)
	}
	defer room.LocalParticipant.UnpublishTrack(pub.SID())

	// Persist initial call state in participant attributes so reconnecting
	// clients and official LiveKit SDK frontends discover it immediately.
	room.LocalParticipant.SetAttributes(map[string]string{
		roomprotocol.AttrLKAgentState: roomprotocol.AgentStateListening,
	})
	defer func() {
		room.LocalParticipant.SetAttributes(map[string]string{
			roomprotocol.AttrLKAgentState: roomprotocol.AgentStateIdle,
		})
	}()

	ctx, cancel := context.WithCancel(sessionCtx)
	defer cancel()

	sink, err := newTrackAudioSink(ctx, localTrack, encoder)
	if err != nil {
		return fmt.Errorf("audio sink: %w", err)
	}
	source := newTrackAudioSource(remote, decoder)

	ctrl := engine.NewLiveCallController(sessionCtx, deps, identity, liveCfg)
	// Live-call turns get the same treatment text/voice mode already give
	// theirs: what the agent heard goes out on roomprotocol.TopicTranscript, and the LLM
	// reply streams on roomprotocol.TopicChat, both alongside (not instead of) the
	// synthesized audio pushed to sink. A client already watching those two
	// topics sees live-call turns the same way it sees the other two modes'
	// turns; roomprotocol.TopicState adds the live-call-only bits (who's talking, and
	// when a reply was cut off) that the other modes have no equivalent of.
	var eventOrder atomic.Int64
	options := func(topic string, utteranceID uint64) lksdk.StreamTextOptions {
		return lksdk.StreamTextOptions{Topic: topic, DestinationIdentities: []string{identity}, Attributes: map[string]string{
			roomprotocol.AttrEventOrder:  strconv.FormatInt(eventOrder.Add(1), 10),
			roomprotocol.AttrUtteranceID: strconv.FormatUint(utteranceID, 10),
		}}
	}
	ctrl.OnTranscript = func(utteranceID uint64, transcript string) {
		room.LocalParticipant.SendText(transcript, options(roomprotocol.TopicTranscript, utteranceID))
		lkOpts := lksdk.StreamTextOptions{
			Topic:                 roomprotocol.TopicLKTranscription,
			DestinationIdentities: []string{identity},
			Attributes: map[string]string{
				roomprotocol.AttrEventOrder:           strconv.FormatInt(eventOrder.Add(1), 10),
				roomprotocol.AttrUtteranceID:          strconv.FormatUint(utteranceID, 10),
				roomprotocol.AttrLKSegmentID:          strconv.FormatUint(utteranceID, 10),
				roomprotocol.AttrLKTranscriptionFinal: "true",
				roomprotocol.AttrLKTranscribedTrackID: remote.ID(),
			},
		}
		room.LocalParticipant.SendText(transcript, lkOpts)
	}
	ctrl.NewReplySink = func() engine.ReplySink {
		return textReplySink{w: room.LocalParticipant.StreamText(options(roomprotocol.TopicChat, 0))}
	}
	ctrl.NewInterimSink = func(utteranceID uint64) engine.TranscriptSink {
		opts := options(roomprotocol.TopicTranscriptInterim, utteranceID)
		opts.Attributes[roomprotocol.AttrFormat] = roomprotocol.FormatSnapshotJSONL
		w := room.LocalParticipant.StreamText(opts)
		return interimReplySink{w: w}
	}
	ctrl.OnState = func(state engine.CallState, utteranceID uint64) {
		room.LocalParticipant.SendText(string(state), options(roomprotocol.TopicState, utteranceID))
		room.LocalParticipant.SetAttributes(map[string]string{
			roomprotocol.AttrLKAgentState: string(state),
		})
	}
	return ctrl.Run(source, sink)
}

// Each interim update replaces the previous hypothesis. JSONL preserves update
// boundaries even when transport chunks split or combine Unicode strings.
type interimReplySink struct{ w *lksdk.TextStreamWriter }

func (s interimReplySink) WriteSnapshot(snapshot string) {
	data, _ := json.Marshal(struct {
		Text string `json:"text"`
	}{snapshot})
	s.w.Write(string(data)+"\n", nil)
}
func (s interimReplySink) Close() { s.w.Close() }
