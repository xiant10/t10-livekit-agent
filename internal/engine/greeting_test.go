package engine

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type greeterFunc func(ctx context.Context, identity string, onToken func(string)) (string, error)

func (f greeterFunc) Greet(ctx context.Context, identity string, onToken func(string)) (string, error) {
	return f(ctx, identity, onToken)
}

// The agent speaks first: the greeting goes out as audio and as reply text, and
// is remembered like any reply.
func TestLiveCallController_GreetsFirst(t *testing.T) {
	const rate = 16000
	deps := &Deps{
		STT:           &fakeSTTProvider{},
		LLM:           &fakeLLM{tokens: []string{"ok."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "ann", testLiveConfig())
	var asked atomic.Value
	ctrl.Greeter = greeterFunc(func(_ context.Context, identity string, onToken func(string)) (string, error) {
		asked.Store(identity)
		onToken("Hi Ann.")
		onToken(" How can I help?")
		return "Hi Ann. How can I help?", nil
	})
	reply := &recordingSink{}
	ctrl.NewReplySink = func(context.Context) ReplySink { return reply }
	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	waitFor(t, time.Second, func() bool { return reply.Closed() })
	close(source.frames)
	<-runErr

	if asked.Load() != "ann" {
		t.Errorf("greeter asked for %v", asked.Load())
	}
	if got := strings.Join(reply.Tokens(), ""); got != "Hi Ann. How can I help?" {
		t.Errorf("reply text = %q", got)
	}
	if sink.pushCount() == 0 {
		t.Error("the greeting was never heard")
	}
	hist := deps.Conversations.Get("ann").Snapshot()
	if len(hist) != 1 || hist[0].Role != "assistant" || hist[0].Content != "Hi Ann. How can I help?" {
		t.Errorf("history = %+v", hist)
	}
}

// A caller who is already talking isn't talked over: the greeting gives way.
func TestLiveCallController_GreetingGivesWayToACallerWhoSpeaksFirst(t *testing.T) {
	deps := &Deps{TTS: &fakeTTS{sampleRate: 16000}, LLM: &fakeLLM{}, Conversations: NewManager("")}
	for name, speak := range map[string]func(*LiveCallController){
		"spoke":    func(c *LiveCallController) { c.turnVersion.Store(1) },
		"speaking": func(c *LiveCallController) { c.inFlight.Store(1) },
	} {
		ctrl := NewLiveCallController(context.Background(), deps, "ann", testLiveConfig())
		called := false
		ctrl.Greeter = greeterFunc(func(context.Context, string, func(string)) (string, error) {
			called = true
			return "", nil
		})
		speak(ctrl)
		if ctrl.handleTurn(nil, newFakeSink(16000), 0, ctrl.greeting) || called {
			t.Errorf("%s: the greeting started over the caller", name)
		}
		ctrl.stopCall()
	}
}

// Speech during the greeting interrupts it like any reply; what was heard is
// remembered as cut off, and the caller is answered.
func TestLiveCallController_SpeechInterruptsTheGreeting(t *testing.T) {
	const rate = 16000
	const frameN = rate / 50
	deps := &Deps{
		STT:           &fakeSTTProvider{transcripts: []string{"what's on today"}},
		LLM:           &fakeLLM{tokens: []string{"ok."}},
		TTS:           &fakeTTS{sampleRate: rate},
		Conversations: NewManager(""),
	}
	ctrl := NewLiveCallController(context.Background(), deps, "ann", testLiveConfig())
	ctrl.Greeter = greeterFunc(func(ctx context.Context, _ string, onToken func(string)) (string, error) {
		onToken("Good morning Ann, I'm Ti. ")
		onToken("You have three tasks due")
		<-ctx.Done()
		return "", ctx.Err()
	})
	source := newFakeSource(rate)
	sink := newFakeSink(rate)
	runErr := make(chan error, 1)
	go func() { runErr <- ctrl.Run(source, sink) }()

	waitFor(t, time.Second, func() bool { return sink.pushCount() >= 1 })
	source.frames <- loudFrame(frameN)
	source.frames <- silentFrame(frameN)
	waitFor(t, 2*time.Second, func() bool {
		hist := deps.Conversations.Get("ann").Snapshot()
		return len(hist) == 3
	})
	close(source.frames)
	<-runErr

	hist := deps.Conversations.Get("ann").Snapshot()
	if !strings.HasSuffix(hist[0].Content, interruptedMarker) || !strings.HasPrefix(hist[0].Content, "Good morning Ann") {
		t.Errorf("the greeting isn't remembered as cut off: %+v", hist)
	}
	if hist[1].Role != "user" || hist[2].Content != "ok." {
		t.Errorf("the caller wasn't answered: %+v", hist)
	}
	if sink.drainCount() == 0 {
		t.Error("the greeting's audio wasn't drained")
	}
}
