package livekit

import (
	"context"
	"log/slog"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"

	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/roomprotocol"
	"t10-livekit-agent/internal/telemetry"
)

// grantAttribute is set by room admission, inside the LiveKit-signed token.
const grantAttribute = "t10.grant"

// grantWait bounds the wait for a sender's join event, which can arrive just
// after their first message.
const grantWait = 2 * time.Second

// withGrant puts the sender's agent grant on ctx. It's read per message, so
// someone who rejoins runs under the grant of their new token.
func withGrant(ctx context.Context, room *lksdk.Room, identity string) context.Context {
	return provider.WithGrant(ctx, awaitGrant(ctx, func() (string, bool) {
		rp := room.GetParticipantByIdentity(identity)
		if rp == nil {
			return "", false
		}
		return rp.Attributes()[grantAttribute], true
	}))
}

// awaitGrant polls until lookup finds the participant, and returns "" if it
// never does. That fails the turn only under a provider that needs a grant.
func awaitGrant(ctx context.Context, lookup func() (string, bool)) string {
	ctx, cancel := context.WithTimeout(ctx, grantWait)
	defer cancel()
	for {
		if grant, ok := lookup(); ok {
			return grant
		}
		select {
		case <-ctx.Done():
			return ""
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Join connects to the room as the agent participant, registers the text
// and voice mode handlers, and arms live-call mode to start automatically
// the moment a participant's microphone track is subscribed — no separate
// signaling needed to pick a mode.
func Join(url, token string, deps *engine.Deps, liveCfg engine.LiveConfig) (*lksdk.Room, error) {
	// The callback closes over this, set before joining: the agent arrives
	// after the caller, so their track can be subscribed before the join even
	// returns. NewRoom copies the callback, so it has to come after it.
	var room *lksdk.Room

	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if track.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		identity := rp.Identity()
		// One session ID covers everything logged for the call; each turn
		// in it is its own trace.
		ctx := provider.WithGrant(telemetry.WithSessionID(context.Background(), rp.SID()), rp.Attributes()[grantAttribute])
		slog.InfoContext(ctx, "live call: starting", "identity", identity)
		go func() {
			if err := startLiveCall(ctx, room, deps, liveCfg, identity, track); err != nil {
				slog.InfoContext(ctx, "live call: ended", "identity", identity, "reason", err)
			}
		}()
	}

	room = lksdk.NewRoom(cb)

	// Registered first, so nothing a waiting caller sends is missed.
	messageQueue := engine.NewManager("")
	chatHandler := newTextStreamHandler(room, deps, messageQueue)
	if err := room.RegisterTextStreamHandler(roomprotocol.TopicChat, chatHandler); err != nil {
		return nil, err
	}
	if err := room.RegisterTextStreamHandler(roomprotocol.TopicLKChat, chatHandler); err != nil {
		return nil, err
	}
	if err := room.RegisterByteStreamHandler(roomprotocol.TopicVoice, newVoiceStreamHandler(room, deps, messageQueue)); err != nil {
		return nil, err
	}

	if err := room.JoinWithToken(url, token, lksdk.WithAutoSubscribe(true)); err != nil {
		return nil, err
	}
	// Not over the listening state a live call set if it started during the join.
	if len(room.GetRemoteParticipants()) == 0 {
		room.LocalParticipant.SetAttributes(map[string]string{
			roomprotocol.AttrLKAgentState: roomprotocol.AgentStateIdle,
		})
	}
	return room, nil
}
