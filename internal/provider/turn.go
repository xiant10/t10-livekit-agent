package provider

import (
	"context"
	"errors"
)

// InterruptedMarker ends an assistant message in the history that was cut off
// by the caller, so the message holds only what they actually heard.
const InterruptedMarker = " [interrupted]"

// ErrSuperseded means a newer message took over this turn's output. It is not
// a failure: ChatStream returns it with the text delivered so far.
var ErrSuperseded = errors.New("provider: superseded by a newer message")

type (
	sessionKey      struct{}
	sessionNameKey  struct{}
	conversationKey struct{}
	liveCallKey     struct{}
	admittedKey     struct{}
	grantKey        struct{}
)

// WithSession names the conversation a stateful provider should continue.
// It lives on ctx rather than in ChatStream's signature because stateless
// providers have no use for it.
func WithSession(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, sessionKey{}, id)
}

// SessionFrom returns the conversation WithSession named, or "".
func SessionFrom(ctx context.Context) string {
	id, _ := ctx.Value(sessionKey{}).(string)
	return id
}

// WithSessionName is what a stateful provider finds the session under when it
// keeps one between stays, in place of the key SessionFrom returns.
func WithSessionName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, sessionNameKey{}, name)
}

// SessionNameFrom returns the name WithSessionName set, or else SessionFrom's key.
func SessionNameFrom(ctx context.Context) string {
	if name, ok := ctx.Value(sessionNameKey{}).(string); ok {
		return name
	}
	return SessionFrom(ctx)
}

// WithLiveCall marks a turn as said on a live call, so its reply is read aloud.
func WithLiveCall(ctx context.Context) context.Context {
	return context.WithValue(ctx, liveCallKey{}, true)
}

// OnLiveCall reports whether WithLiveCall marked the turn.
func OnLiveCall(ctx context.Context) bool {
	live, _ := ctx.Value(liveCallKey{}).(bool)
	return live
}

// WithConversation names an existing conversation the turn belongs to, as
// opposed to one the provider finds or creates for the caller.
func WithConversation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, conversationKey{}, id)
}

// ConversationFrom returns the conversation WithConversation named, or "".
func ConversationFrom(ctx context.Context) string {
	id, _ := ctx.Value(conversationKey{}).(string)
	return id
}

// Releaser is a provider that keeps state per session key, and can drop it.
type Releaser interface {
	Release(prefix string)
}

// roomLLM keeps one room's turns apart from another's. The same person can be
// in two rooms, and each has its own history for the provider to keep up with.
// A stay's ID changes every time the agent joins, so it keys only what lasts
// that long. The session outlives it, under the room's name.
type roomLLM struct {
	LLMProvider
	stay         string
	room         string
	conversation string
}

// RoomLLM is an LLMProvider scoped to one stay in a room.
type RoomLLM interface {
	LLMProvider
	Release()
}

// ForRoom scopes llm to one stay in a room. conversation, when set, is the
// conversation the room belongs to.
func ForRoom(llm LLMProvider, stay, room, conversation string) RoomLLM {
	return roomLLM{LLMProvider: llm, stay: stay, room: room, conversation: conversation}
}

// scoped names the stay's session for the identity on ctx, and the room's conversation.
func (l roomLLM) scoped(ctx context.Context) context.Context {
	identity := SessionFrom(ctx)
	ctx = WithSessionName(WithSession(ctx, l.stay+"/"+identity), l.room+"/"+identity)
	if l.conversation != "" {
		ctx = WithConversation(ctx, l.conversation)
	}
	return ctx
}

func (l roomLLM) ChatStream(ctx context.Context, msgs []Message, onToken func(delta string)) (string, error) {
	return l.LLMProvider.ChatStream(l.scoped(ctx), msgs, onToken)
}

// Greet asks the provider for the call's greeting in this room's session, when it
// writes greetings at all.
func (l roomLLM) Greet(ctx context.Context, req GreetRequest, onToken func(delta string)) (string, error) {
	greeter, ok := l.LLMProvider.(Greeter)
	if !ok {
		return "", ErrNoGreeting
	}
	return greeter.Greet(l.scoped(ctx), req, onToken)
}

// Release drops what the provider holds for this room, if it holds anything.
func (l roomLLM) Release() {
	if releaser, ok := l.LLMProvider.(Releaser); ok {
		releaser.Release(l.stay + "/")
	}
}

// WithGrant names the agent grant a turn runs under, read from the participant's
// LiveKit-signed attributes. It never comes from anything the caller typed.
func WithGrant(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, grantKey{}, id)
}

// GrantFrom returns the grant WithGrant named, or "".
func GrantFrom(ctx context.Context) string {
	id, _ := ctx.Value(grantKey{}).(string)
	return id
}

// WithAdmitted registers fn to run once the provider has durably accepted the
// input, after any fn already on ctx. A provider that never calls Admitted
// keeps the caller's turn held until ChatStream returns.
func WithAdmitted(ctx context.Context, fn func()) context.Context {
	previous, _ := ctx.Value(admittedKey{}).(func())
	return context.WithValue(ctx, admittedKey{}, func() {
		if previous != nil {
			previous()
		}
		fn()
	})
}

// Admitted runs what WithAdmitted registered on ctx, if anything.
func Admitted(ctx context.Context) {
	if fn, ok := ctx.Value(admittedKey{}).(func()); ok {
		fn()
	}
}
