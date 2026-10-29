package provider

import "context"

// Message is one turn in a chat conversation.
type Message struct {
	Role    string // "system", "user", or "assistant"
	Content string
}

// LLMProvider streams a chat completion token-by-token.
type LLMProvider interface {
	// ChatStream runs the conversation in msgs and invokes onToken for each
	// incremental text delta as it arrives. It returns the full assembled
	// reply once the stream completes.
	ChatStream(ctx context.Context, msgs []Message, onToken func(delta string)) (full string, err error)
}
