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
	sessionKey  struct{}
	admittedKey struct{}
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
