package engine

import (
	"context"
	"testing"
)

func TestRunChatTurn(t *testing.T) {
	deps := &Deps{
		LLM:           &fakeLLM{tokens: []string{"Hello", ", ", "world!"}},
		Conversations: NewManager("you are a test bot"),
	}
	sink := &recordingSink{}

	full, err := RunChatTurn(context.Background(), deps, "alice", "hi there", sink)
	if err != nil {
		t.Fatalf("RunChatTurn: %v", err)
	}
	if want := "Hello, world!"; full != want {
		t.Fatalf("full reply = %q, want %q", full, want)
	}
	if !sink.closed {
		t.Fatalf("sink was never closed")
	}

	wantTokens := []string{"Hello", ", ", "world!"}
	if len(sink.tokens) != len(wantTokens) {
		t.Fatalf("tokens = %v, want %v", sink.tokens, wantTokens)
	}
	for i, want := range wantTokens {
		if sink.tokens[i] != want {
			t.Fatalf("tokens[%d] = %q, want %q", i, sink.tokens[i], want)
		}
	}

	hist := deps.Conversations.Get("alice").Snapshot()
	if len(hist) != 3 {
		t.Fatalf("history = %+v, want 3 entries (system, user, assistant)", hist)
	}
	if hist[0].Role != "system" {
		t.Errorf("hist[0].Role = %q, want %q", hist[0].Role, "system")
	}
	if hist[1].Role != "user" || hist[1].Content != "hi there" {
		t.Errorf("hist[1] = %+v, want {user, %q}", hist[1], "hi there")
	}
	if hist[2].Role != "assistant" || hist[2].Content != "Hello, world!" {
		t.Errorf("hist[2] = %+v, want {assistant, %q}", hist[2], "Hello, world!")
	}
}
