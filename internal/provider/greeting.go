package provider

import (
	"context"
	"errors"
)

// GreetRequest is what a call says about itself when asking for its greeting.
type GreetRequest struct {
	// Locale and TimeZone are the caller's, when their app said so.
	Locale   string
	TimeZone string
	// Client is what the caller's app said about the call, from its t10.client.*
	// room attributes, such as the screen it was started from.
	Client map[string]string
	// Prepare makes the greeting ready without saying it, so it's said at once
	// when the call starts.
	Prepare bool
}

// Greeter is a provider that writes a call's greeting itself: agent-pi's
// session.greet (docs/GREETING.md). The text streams to onToken; "" with no
// error means the agent chose to say nothing, such as for a call coming back.
type Greeter interface {
	Greet(ctx context.Context, req GreetRequest, onToken func(delta string)) (string, error)
}

// ErrNoGreeting means the provider, or the agent behind it, has no greeting.
var ErrNoGreeting = errors.New("provider: no greeting method")
