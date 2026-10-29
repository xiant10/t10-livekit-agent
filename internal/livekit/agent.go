package livekit

import (
	"context"
	"log/slog"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"

	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/roomprotocol"
	"t10-livekit-agent/internal/telemetry"
)

// Join connects to the room as the agent participant, registers the text
// and voice mode handlers, and arms live-call mode to start automatically
// the moment a participant's microphone track is subscribed — no separate
// signaling needed to pick a mode.
func Join(url, token string, deps *engine.Deps, liveCfg engine.LiveConfig) (*lksdk.Room, error) {
	// The callback must be built before ConnectToRoomWithToken returns the
	// *Room it needs, so it closes over this variable (set right after) and
	// reads it lazily when a track actually gets subscribed.
	var room *lksdk.Room

	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if track.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		identity := rp.Identity()
		// The live call is a session: one correlation ID (and one root
		// trace span) covers every utterance/turn logged for its whole
		// lifetime, not just its start.
		ctx, end := telemetry.StartSession(context.Background())
		slog.InfoContext(ctx, "live call: starting", "identity", identity)
		go func() {
			defer end()
			if err := startLiveCall(ctx, room, deps, liveCfg, identity, track); err != nil {
				slog.InfoContext(ctx, "live call: ended", "identity", identity, "reason", err)
			}
		}()
	}

	r, err := lksdk.ConnectToRoomWithToken(url, token, cb, lksdk.WithAutoSubscribe(true))
	if err != nil {
		return nil, err
	}
	room = r

	r.LocalParticipant.SetAttributes(map[string]string{
		roomprotocol.AttrLKAgentState: roomprotocol.AgentStateIdle,
	})

	messageQueue := engine.NewManager("")
	chatHandler := newTextStreamHandler(r, deps, messageQueue)
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicChat, chatHandler); err != nil {
		r.Disconnect()
		return nil, err
	}
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicLKChat, chatHandler); err != nil {
		r.Disconnect()
		return nil, err
	}
	if err := r.RegisterByteStreamHandler(roomprotocol.TopicVoice, newVoiceStreamHandler(r, deps, messageQueue)); err != nil {
		r.Disconnect()
		return nil, err
	}

	return r, nil
}
