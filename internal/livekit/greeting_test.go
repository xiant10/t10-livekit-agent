package livekit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"t10-livekit-agent/internal/provider"
)

// agentGreeter is a provider that writes greetings: it says words, then answers.
type agentGreeter struct {
	provider.LLMProvider
	delay time.Duration
	words []string
	err   error
	asked *provider.GreetRequest
}

func (a agentGreeter) Greet(ctx context.Context, req provider.GreetRequest, onToken func(string)) (string, error) {
	if a.asked != nil {
		*a.asked = req
	}
	select {
	case <-time.After(a.delay):
	case <-ctx.Done():
		return "", ctx.Err()
	}
	for _, w := range a.words {
		onToken(w)
	}
	return strings.Join(a.words, ""), a.err
}

func greetWith(llm provider.LLMProvider, opts GreetingOptions) (string, string, error) {
	var heard strings.Builder
	g := callGreeter{opts: opts, llm: llm}
	text, err := g.Greet(context.Background(), "ann", func(d string) { heard.WriteString(d) })
	return text, heard.String(), err
}

func TestTheAgentsGreetingOrTheFixedText(t *testing.T) {
	agent := GreetingOptions{Mode: GreetingAgent, Text: "您好", Timeout: 200 * time.Millisecond}
	cases := map[string]struct {
		llm   provider.LLMProvider
		opts  GreetingOptions
		heard string
		err   bool
	}{
		"the agent's":                  {agentGreeter{words: []string{"Hi Ann.", " Ready?"}}, agent, "Hi Ann. Ready?", false},
		"the agent chose silence":      {agentGreeter{}, agent, "", false},
		"a harness without greetings":  {agentGreeter{err: provider.ErrNoGreeting}, agent, "您好", false},
		"the agent failed":             {agentGreeter{err: errors.New("boom")}, agent, "您好", false},
		"failed after its first words": {agentGreeter{words: []string{"Hi"}, err: errors.New("cut")}, agent, "Hi", true},
		"the agent was too slow":       {agentGreeter{delay: time.Second, words: []string{"late"}}, agent, "您好", false},
		"a provider with no greetings": {nil, agent, "您好", false},
		"fixed":                        {agentGreeter{words: []string{"agent"}}, GreetingOptions{Mode: GreetingFixed, Text: "您好"}, "您好", false},
	}
	for name, c := range cases {
		_, heard, err := greetWith(c.llm, c.opts)
		if heard != c.heard || (err != nil) != c.err {
			t.Errorf("%s: heard %q, err %v", name, heard, err)
		}
	}
}

func TestACallerIsGreetedOncePerStay(t *testing.T) {
	g := newGreetings(GreetingOptions{Mode: GreetingFixed, Text: "Hi"}, nil)
	if g.forCall("ann", nil) == nil || g.forCall("ann", nil) != nil || g.forCall("bob", nil) == nil {
		t.Error("each caller is greeted once")
	}
	if newGreetings(GreetingOptions{Mode: GreetingOff, Text: "Hi"}, nil).forCall("ann", nil) != nil {
		t.Error("off greets no one")
	}
	if newGreetings(GreetingOptions{Mode: GreetingFixed}, nil).forCall("ann", nil) != nil {
		t.Error("a fixed greeting needs its text")
	}
	var none *greetings
	if none.forCall("ann", nil) != nil {
		t.Error("no greetings greet no one")
	}
}

func TestTheCallersAppValuesReachTheAgent(t *testing.T) {
	var asked provider.GreetRequest
	g := newGreetings(GreetingOptions{Mode: GreetingAgent}, agentGreeter{asked: &asked})
	greeter := g.forCall("ann", map[string]string{
		grantAttribute: "grt_1", "t10.client.screen": "tasks", "t10.client.locale": "zh-Hans",
		"t10.client.timeZone": "Asia/Shanghai", "t10.client.": "x", "other": "y",
	})
	if _, err := greeter.Greet(context.Background(), "ann", func(string) {}); err != nil {
		t.Fatal(err)
	}
	if asked.Locale != "zh-Hans" || asked.TimeZone != "Asia/Shanghai" || len(asked.Client) != 3 || asked.Client["screen"] != "tasks" {
		t.Errorf("asked %+v", asked)
	}
}
