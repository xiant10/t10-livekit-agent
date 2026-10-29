package engine

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"t10-livekit-agent/internal/provider"
	"testing"
	"time"
)

type messageLLM func(context.Context, []provider.Message, func(string)) (string, error)

func (f messageLLM) ChatStream(ctx context.Context, history []provider.Message, emit func(string)) (string, error) {
	return f(ctx, history, emit)
}

type messageTTS func(context.Context, string, string) ([]byte, int, error)

func (f messageTTS) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	return f(ctx, text, voice)
}

type chunkSink struct {
	recordingSink
	heard chan string
	audio chan []byte
}

func (s *chunkSink) WriteTranscript(text string)           { s.heard <- text }
func (s *chunkSink) WriteAudio(pcm []byte, rate int) error { s.audio <- pcm; return nil }

func TestVoiceAudioBeforeLLMCompletes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sink := &chunkSink{heard: make(chan string, 1), audio: make(chan []byte, 2)}
	deps := &Deps{Conversations: NewManager(""), STT: &batchSTT{text: "question"},
		TTS: messageTTS(func(context.Context, string, string) ([]byte, int, error) { return []byte{1, 0}, 24000, nil })}
	deps.LLM = messageLLM(func(ctx context.Context, _ []provider.Message, emit func(string)) (string, error) {
		select {
		case heard := <-sink.heard:
			if heard != "question" {
				t.Errorf("heard %q", heard)
			}
		default:
			t.Error("LLM started before transcript")
		}
		emit("First sentence.")
		select {
		case <-sink.audio:
		case <-ctx.Done():
			return "", errors.New("audio waited for complete LLM reply")
		}
		emit(" Second sentence.")
		return "First sentence. Second sentence.", nil
	})
	if _, _, err := RunVoiceTurn(ctx, deps, "alice", nil, sink); err != nil {
		t.Fatal(err)
	}
	if !sink.Closed() {
		t.Fatal("reply not closed")
	}
	if len(sink.audio) != 1 {
		t.Fatal("tail audio missing")
	}
}

func TestMessageQueueCanceledWaiterPreservesFIFO(t *testing.T) {
	c := NewManager("").Get("alice")
	first, second, third := c.ReserveTurn(), c.ReserveTurn(), c.ReserveTurn()
	release, err := first(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = second(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	go func() { done, _ := third(context.Background()); close(entered); done() }()
	select {
	case <-entered:
		t.Fatal("third bypassed active first turn")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queue stranded")
	}
}

func TestTextWaitsForVoiceTranscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sttEntered, unblock := make(chan struct{}), make(chan struct{})
	deps := &Deps{Conversations: NewManager(""), LLM: &fakeLLM{tokens: []string{"OK"}}, TTS: messageTTS(func(context.Context, string, string) ([]byte, int, error) { return []byte{1, 0}, 24000, nil })}
	deps.STT = &waitingBatchSTT{entered: sttEntered, unblock: unblock}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _, err := RunVoiceTurn(ctx, deps, "alice", nil, &chunkSink{heard: make(chan string, 1), audio: make(chan []byte, 1)})
		if err != nil {
			t.Error(err)
		}
	}()
	<-sttEntered
	go func() {
		defer wg.Done()
		_, err := RunChatTurn(ctx, deps, "alice", "second", &recordingSink{})
		if err != nil {
			t.Error(err)
		}
	}()
	// Another identity must remain independent of the blocked voice turn.
	if _, err := RunChatTurn(ctx, deps, "bob", "independent", &recordingSink{}); err != nil {
		t.Fatal(err)
	}
	close(unblock)
	wg.Wait()
	history := deps.Conversations.Get("alice").Snapshot()
	want := []provider.Message{{Role: "user", Content: "first"}, {Role: "assistant", Content: "OK"}, {Role: "user", Content: "second"}, {Role: "assistant", Content: "OK"}}
	if !reflect.DeepEqual(history, want) {
		t.Fatalf("history: %+v", history)
	}
}

type waitingBatchSTT struct {
	fakeSTTProvider
	entered, unblock chan struct{}
}

func (s *waitingBatchSTT) TranscribeBatch(ctx context.Context, _ []byte, _ string) (string, error) {
	close(s.entered)
	select {
	case <-s.unblock:
		return "first", nil
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestChatEmptyAndPartialFailure(t *testing.T) {
	for _, partial := range []string{"", "Partial answer"} {
		t.Run(partial, func(t *testing.T) {
			deps := &Deps{Conversations: NewManager(""), LLM: messageLLM(func(_ context.Context, _ []provider.Message, emit func(string)) (string, error) {
				if partial != "" {
					emit(partial)
					return "", errors.New("failed")
				}
				return "", nil
			})}
			sink := &recordingSink{}
			if _, err := RunChatTurn(context.Background(), deps, "alice", "hi", sink); err == nil {
				t.Fatal("missing error")
			}
			if !sink.Closed() {
				t.Fatal("stream not closed")
			}
			h := deps.Conversations.Get("alice").Snapshot()
			if partial != "" && (len(h) != 2 || h[1].Content != partial+" [incomplete]") {
				t.Fatalf("lost partial reply: %+v", h)
			}
		})
	}
}

func TestVoiceTTSFailureCancelsGeneratingLLM(t *testing.T) {
	failure := errors.New("synthesis failed")
	deps := &Deps{Conversations: NewManager(""), STT: &batchSTT{text: "hello"},
		TTS: messageTTS(func(context.Context, string, string) ([]byte, int, error) { return nil, 0, failure }),
		LLM: messageLLM(func(ctx context.Context, _ []provider.Message, emit func(string)) (string, error) {
			emit("First sentence.")
			<-ctx.Done()
			return "", ctx.Err()
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sink := &chunkSink{heard: make(chan string, 1), audio: make(chan []byte, 1)}
	if _, _, err := RunVoiceTurn(ctx, deps, "alice", nil, sink); !errors.Is(err, failure) {
		t.Fatalf("lost TTS error: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatal("waited for outer deadline instead of canceling generation")
	}
	// The failed voice turn must also release the shared turn queue.
	deps.LLM = &fakeLLM{tokens: []string{"Recovered"}}
	if _, err := RunChatTurn(ctx, deps, "alice", "retry", &recordingSink{}); err != nil {
		t.Fatal(err)
	}
}
