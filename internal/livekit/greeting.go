package livekit

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/provider"
)

// clientAttribute prefixes what the caller's app said about the call, set by
// room admission inside the LiveKit-signed token (auth-server's t10.client.*).
const clientAttribute = "t10.client."

// prepareTimeout bounds a prepare, which nobody waits for.
const prepareTimeout = 10 * time.Second

// The greeting modes, after main resolves auto.
const (
	GreetingAgent = "agent"
	GreetingFixed = "fixed"
	GreetingOff   = "off"
)

// GreetingOptions is how a caller is greeted when their microphone arrives
// (docs/GREETING.md).
type GreetingOptions struct {
	// Mode is GreetingAgent, GreetingFixed or GreetingOff.
	Mode string
	// Text is the fixed greeting, and the agent mode's fallback. Empty says nothing.
	Text string
	// Timeout is how long the agent has to start its greeting before the fallback.
	Timeout time.Duration
	// Prepare asks the agent to get a caller's greeting ready when the worker
	// sees them, before their microphone is subscribed.
	Prepare bool
}

func (o GreetingOptions) on() bool {
	return o.Mode == GreetingAgent || (o.Mode == GreetingFixed && o.Text != "")
}

// greetRequest is what the caller's app said about the call: its t10.client.*
// attributes, with the language and time zone among them also passed on their own.
func greetRequest(attributes map[string]string) provider.GreetRequest {
	client := map[string]string{}
	for key, value := range attributes {
		if name, ok := strings.CutPrefix(key, clientAttribute); ok && name != "" && value != "" {
			client[name] = value
		}
	}
	return provider.GreetRequest{Locale: client["locale"], TimeZone: client["timeZone"], Client: client}
}

// greetings remembers who was greeted during one stay in a room, so a caller's
// track subscribed again isn't greeted twice, and who was prepared for.
type greetings struct {
	opts GreetingOptions
	llm  provider.LLMProvider

	mu       sync.Mutex
	greeted  map[string]bool
	prepared map[string]bool
}

func newGreetings(opts GreetingOptions, llm provider.LLMProvider) *greetings {
	return &greetings{opts: opts, llm: llm, greeted: map[string]bool{}, prepared: map[string]bool{}}
}

// forCall returns the greeter for a caller's live call, or nil when this caller
// was greeted already in this stay, or greetings are off.
func (g *greetings) forCall(identity string, attributes map[string]string) engine.Greeter {
	if g == nil || !g.opts.on() {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.greeted[identity] {
		return nil
	}
	g.greeted[identity] = true
	return callGreeter{opts: g.opts, llm: g.llm, req: greetRequest(attributes)}
}

// prepare asks the agent to get a caller's greeting ready, once per stay, in
// the background. A room used only for text never gets past this.
func (g *greetings) prepare(rp *lksdk.RemoteParticipant) {
	if g == nil || g.opts.Mode != GreetingAgent || !g.opts.Prepare {
		return
	}
	greeter, ok := g.llm.(provider.Greeter)
	if !ok {
		return
	}
	identity := rp.Identity()
	g.mu.Lock()
	if g.prepared[identity] {
		g.mu.Unlock()
		return
	}
	g.prepared[identity] = true
	g.mu.Unlock()
	attributes := rp.Attributes()
	req := greetRequest(attributes)
	req.Prepare = true
	go func() {
		ctx := provider.WithSession(provider.WithGrant(context.Background(), attributes[grantAttribute]), identity)
		ctx, cancel := context.WithTimeout(ctx, prepareTimeout)
		defer cancel()
		if _, err := greeter.Greet(ctx, req, nil); err != nil && !errors.Is(err, provider.ErrNoGreeting) {
			slog.DebugContext(ctx, "greeting: prepare failed", "identity", identity, "err", err)
		}
	}()
}

// callGreeter is one caller's greeting: the agent's, else the fixed text.
type callGreeter struct {
	opts GreetingOptions
	llm  provider.LLMProvider
	req  provider.GreetRequest
}

var errGreetingLate = errors.New("greeting: the agent didn't start in time")

func (g callGreeter) Greet(ctx context.Context, identity string, onToken func(string)) (string, error) {
	if g.opts.Mode == GreetingAgent {
		if greeter, ok := g.llm.(provider.Greeter); ok {
			text, spoke, err := g.fromAgent(ctx, greeter, identity, onToken)
			switch {
			case err == nil:
				// "" is the agent's choice, such as for a call coming back: no fallback.
				return text, nil
			case spoke || ctx.Err() != nil:
				// Words already went out, or the caller interrupted: nothing to add.
				return text, err
			case !errors.Is(err, provider.ErrNoGreeting):
				slog.WarnContext(ctx, "greeting: the agent's failed, saying the fixed text", "identity", identity, "err", err)
			}
		}
	}
	if g.opts.Text == "" {
		return "", nil
	}
	onToken(g.opts.Text)
	return g.opts.Text, nil
}

// fromAgent asks the agent, giving up if no word has come by the timeout. Once
// it gives up, a word arriving after all is dropped, so the caller never hears
// both the agent's start and the fallback.
func (g callGreeter) fromAgent(ctx context.Context, greeter provider.Greeter, identity string, onToken func(string)) (string, bool, error) {
	ctx, cancel := context.WithCancel(provider.WithSession(ctx, identity))
	defer cancel()
	const waiting, speaking, late = 0, 1, 2
	var mu sync.Mutex
	state := waiting
	if g.opts.Timeout > 0 {
		timer := time.AfterFunc(g.opts.Timeout, func() {
			mu.Lock()
			defer mu.Unlock()
			if state == waiting {
				state = late
				cancel()
			}
		})
		defer timer.Stop()
	}
	text, err := greeter.Greet(ctx, g.req, func(delta string) {
		mu.Lock()
		if state == late {
			mu.Unlock()
			return
		}
		state = speaking
		mu.Unlock()
		onToken(delta)
	})
	mu.Lock()
	defer mu.Unlock()
	if state == late {
		return "", false, errGreetingLate
	}
	return text, state == speaking, err
}
