// Package roomauth mints development room credentials for agent and testclient.
package roomauth

import (
	"time"

	"github.com/livekit/protocol/auth"
)

// MintToken builds a JWT granting identity full publish/subscribe/data
// access to room, valid for validFor.
func MintToken(apiKey, apiSecret, identity, room string, validFor time.Duration) (string, error) {
	canPublish := true
	canSubscribe := true
	canPublishData := true

	grant := &auth.VideoGrant{
		RoomJoin:       true,
		Room:           room,
		CanPublish:     &canPublish,
		CanSubscribe:   &canSubscribe,
		CanPublishData: &canPublishData,
	}

	at := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity(identity).
		SetVideoGrant(grant).
		SetValidFor(validFor)

	return at.ToJWT()
}
