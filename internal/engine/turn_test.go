package engine

import (
	"context"
	"testing"
	"time"

	"t10-livekit-agent/internal/provider"
)

func TestSecondMessageReachesProviderOnceFirstIsAdmitted(t *testing.T) {
	reached := make(chan string, 2)
	finish := make(chan struct{})
	deps := &Deps{Conversations: NewManager(""), LLM: messageLLM(func(ctx context.Context, h []provider.Message, _ func(string)) (string, error) {
		if provider.SessionFrom(ctx) != "alice" {
			t.Errorf("session = %q", provider.SessionFrom(ctx))
		}
		reached <- h[len(h)-1].Content
		provider.Admitted(ctx)
		<-finish
		return "reply", nil
	})}
	run := func(text string) chan error {
		done := make(chan error, 1)
		go func() {
			_, err := RunChatTurn(context.Background(), deps, "alice", text, &recordingSink{})
			done <- err
		}()
		return done
	}

	first := run("one")
	if got := <-reached; got != "one" {
		t.Fatalf("first reached with %q", got)
	}
	second := run("two")
	select {
	case got := <-reached:
		if got != "two" {
			t.Fatalf("second reached with %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second message waited for the first reply")
	}
	close(finish)
	for _, done := range []chan error{first, second} {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

func TestProviderThatNeverSignalsAdmissionStaysSerial(t *testing.T) {
	reached := make(chan string, 2)
	finish := make(chan struct{})
	deps := &Deps{Conversations: NewManager(""), LLM: messageLLM(func(_ context.Context, h []provider.Message, _ func(string)) (string, error) {
		reached <- h[len(h)-1].Content
		<-finish
		return "reply", nil
	})}
	done := make(chan error, 2)
	for _, text := range []string{"one", "two"} {
		if text == "two" {
			<-reached
		}
		go func() {
			_, err := RunChatTurn(context.Background(), deps, "alice", text, &recordingSink{})
			done <- err
		}()
	}
	select {
	case got := <-reached:
		t.Fatalf("%q overtook the reply in progress", got)
	case <-time.After(20 * time.Millisecond):
	}
	close(finish)
	<-reached
	<-done
	<-done
}

func TestReleaseCanBeCalledTwice(t *testing.T) {
	c := NewManager("").Get("alice")
	release, err := c.ReserveTurn()(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	release()
}

func TestSupersededTurnIsNotAFailure(t *testing.T) {
	for _, partial := range []string{"", "Let me"} {
		t.Run("partial="+partial, func(t *testing.T) {
			deps := &Deps{Conversations: NewManager(""), LLM: messageLLM(func(_ context.Context, _ []provider.Message, emit func(string)) (string, error) {
				if partial != "" {
					emit(partial)
				}
				return partial, provider.ErrSuperseded
			})}
			sink := &recordingSink{}
			got, err := RunChatTurn(context.Background(), deps, "alice", "hi", sink)
			if err != nil || got != partial {
				t.Fatalf("RunChatTurn = %q, %v", got, err)
			}
			if !sink.Closed() {
				t.Fatal("stream not closed")
			}
			h := deps.Conversations.Get("alice").Snapshot()
			if partial == "" && len(h) != 1 {
				t.Fatalf("history = %+v", h)
			}
			if partial != "" && h[len(h)-1].Content != partial+" [incomplete]" {
				t.Fatalf("history = %+v", h)
			}
		})
	}
}

func TestLiveTurnNamesSessionAndFinishesWhatASupersededReplySaid(t *testing.T) {
	deps := &Deps{
		Conversations: NewManager(""),
		TTS: messageTTS(func(context.Context, string, string) ([]byte, int, error) {
			return []byte{1, 0}, 16000, nil
		}),
		LLM: messageLLM(func(ctx context.Context, _ []provider.Message, emit func(string)) (string, error) {
			if provider.SessionFrom(ctx) != "alice" {
				t.Errorf("session = %q", provider.SessionFrom(ctx))
			}
			emit("Unfinished thought")
			return "Unfinished thought", provider.ErrSuperseded
		}),
	}
	l := NewLiveCallController(context.Background(), deps, "alice", LiveConfig{})
	queue := make(chan pendingChunk, 4)
	err := l.streamAndSynthesize(context.Background(), deps.Conversations.Get("alice"), 16000, l.ttsSem, queue, func() bool { return false }, l.reply)
	if err != nil {
		t.Fatal(err)
	}
	if len(queue) != 1 || (<-queue).text != "Unfinished thought" {
		t.Fatal("what had been said was not flushed to speech")
	}
}
