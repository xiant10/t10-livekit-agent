package agentpi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"t10-livekit-agent/internal/provider"
)

// greetServer is a fake agent answering session.greet with answer.
func greetServer(t *testing.T, answer func(w http.ResponseWriter, params map[string]any)) (*LLM, *[]map[string]any) {
	t.Helper()
	var mu sync.Mutex
	var greets []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Method string         `json:"method"`
			Params map[string]any `json:"params"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		switch request.Method {
		case "session.list":
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"sessions": []any{}}})
		case "session.create":
			_ = json.NewEncoder(w).Encode(map[string]any{"result": map[string]any{"sessionId": "ti.r0.1"}})
		case "session.greet":
			mu.Lock()
			request.Params["accept"] = r.Header.Get("Accept")
			greets = append(greets, request.Params)
			mu.Unlock()
			answer(w, request.Params)
		}
	}))
	t.Cleanup(srv.Close)
	l, err := New(Options{URL: srv.URL, Tokens: fixedToken("token"), Profile: "voice", WhenBusy: WhenBusyAuto, AdmissionTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return l, &greets
}

func greetCtx() context.Context {
	return provider.WithConversation(provider.WithSession(context.Background(), "stay/ann"), "ti.r0.7")
}

func TestGreetStreamsTheAgentsWords(t *testing.T) {
	l, greets := greetServer(t, func(w http.ResponseWriter, _ map[string]any) {
		w.Header().Set("Content-Type", "text/event-stream")
		for _, text := range []string{"张伟，下午好！", "要我先说说吗？"} {
			fmt.Fprintf(w, "data: {\"type\":\"greeting.delta\",\"text\":%q}\n\n", text)
		}
		fmt.Fprint(w, "data: {\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"text\":\"张伟，下午好！要我先说说吗？\",\"by\":\"model\"}}\n\n")
	})
	var heard []string
	text, err := l.Greet(greetCtx(), provider.GreetRequest{Locale: "zh-Hans", Client: map[string]string{"screen": "tasks"}}, func(d string) { heard = append(heard, d) })
	if err != nil || text != "张伟，下午好！要我先说说吗？" || strings.Join(heard, "") != text {
		t.Fatalf("text %q, heard %v, err %v", text, heard, err)
	}
	got := (*greets)[0]
	if got["sessionId"] != "ti.r0.7" || got["locale"] != "zh-Hans" || got["client"].(map[string]any)["screen"] != "tasks" {
		t.Errorf("asked %v", got)
	}
	if !strings.Contains(got["accept"].(string), "text/event-stream") {
		t.Errorf("a greeting is streamed: %v", got["accept"])
	}
}

func TestGreetSaysNothingWhenTheAgentSkips(t *testing.T) {
	l, _ := greetServer(t, func(w http.ResponseWriter, _ map[string]any) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"skipped":"recent"}}`)
	})
	text, err := l.Greet(greetCtx(), provider.GreetRequest{}, func(string) { t.Error("nothing to say") })
	if err != nil || text != "" {
		t.Fatalf("text %q, err %v", text, err)
	}
}

func TestGreetPreparesWithoutStreaming(t *testing.T) {
	l, greets := greetServer(t, func(w http.ResponseWriter, _ map[string]any) {
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"prepared":true}}`)
	})
	// No conversation in the room: the session is found or made first.
	ctx := provider.WithSession(context.Background(), "stay/ann")
	if _, err := l.Greet(ctx, provider.GreetRequest{Prepare: true}, nil); err != nil {
		t.Fatal(err)
	}
	got := (*greets)[0]
	if got["prepare"] != true || got["sessionId"] != "ti.r0.1" || strings.Contains(got["accept"].(string), "event-stream") {
		t.Errorf("asked %v", got)
	}
}

func TestGreetKnowsAHarnessWithoutGreetings(t *testing.T) {
	for name, answer := range map[string]func(http.ResponseWriter){
		"agent-pi": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unknown method session.greet"}}`)
		},
		"agent-ti before greetings": func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"method not implemented"}}`)
		},
	} {
		l, _ := greetServer(t, func(w http.ResponseWriter, _ map[string]any) { answer(w) })
		if _, err := l.Greet(greetCtx(), provider.GreetRequest{}, nil); !errors.Is(err, provider.ErrNoGreeting) {
			t.Errorf("%s: %v", name, err)
		}
	}
}
