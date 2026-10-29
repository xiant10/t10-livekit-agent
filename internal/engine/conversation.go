// Package engine is the transport-agnostic STT→LLM→TTS pipeline: per-
// participant conversation state, a single-turn text chat runner, and the
// VAD/barge-in-aware live-call turn loop. It knows nothing about LiveKit (or
// any other transport) — it talks to the outside world only through the
// provider interfaces (internal/provider) and the ports in ports.go
// (ReplySink, AudioSource, AudioSink). A transport is anything that
// implements those ports; internal/livekit is the one this project ships.
package engine

import (
	"context"
	"sync"

	"t10-livekit-agent/internal/provider"
)

// Conversation is one participant's chat history, shared across whichever
// mode (text, voice, or live) they happen to be using.
type Conversation struct {
	turnMu  sync.Mutex
	tail    <-chan struct{}
	mu      sync.Mutex
	history []provider.Message
}

func newConversation(systemPrompt string) *Conversation {
	c := &Conversation{}
	if systemPrompt != "" {
		c.history = append(c.history, provider.Message{Role: "system", Content: systemPrompt})
	}
	return c
}

func (c *Conversation) Append(role, content string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.history = append(c.history, provider.Message{Role: role, Content: content})
}

// Snapshot returns a copy of the history safe to hand to an LLM call
// running outside the lock.
func (c *Conversation) Snapshot() []provider.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]provider.Message, len(c.history))
	copy(out, c.history)
	return out
}

// Manager lazily creates and retains one Conversation per participant
// identity for the lifetime of the process.
type Manager struct {
	mu            sync.Mutex
	systemPrompt  string
	conversations map[string]*Conversation
}

func NewManager(systemPrompt string) *Manager {
	return &Manager{systemPrompt: systemPrompt, conversations: map[string]*Conversation{}}
}

func (m *Manager) Get(identity string) *Conversation {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.conversations[identity]
	if !ok {
		c = newConversation(m.systemPrompt)
		m.conversations[identity] = c
	}
	return c
}

// Deps bundles the three pipeline providers and shared conversation state
// that both RunChatTurn and LiveCallController need. Built once in cmd/agent
// and handed to whichever transport adapter is in use.
type Deps struct {
	STT           provider.STTProvider
	LLM           provider.LLMProvider
	TTS           provider.TTSProvider
	Conversations *Manager
	Voice         string
}

// ReserveTurn fixes FIFO order at admission, before a transport starts a worker.
// A canceled waiter still passes the baton only after its predecessor finishes.
func (c *Conversation) ReserveTurn() func(context.Context) (func(), error) {
	c.turnMu.Lock()
	previous := c.tail
	done := make(chan struct{})
	c.tail = done
	c.turnMu.Unlock()
	return func(ctx context.Context) (func(), error) {
		if previous != nil {
			select {
			case <-previous:
			case <-ctx.Done():
				go func() { <-previous; close(done) }()
				return nil, ctx.Err()
			}
		}
		if err := ctx.Err(); err != nil {
			close(done)
			return nil, err
		}
		return func() { close(done) }, nil
	}
}
