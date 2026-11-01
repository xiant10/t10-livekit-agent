package agentpi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

// The event orderings below were recorded from the real agent-pi harness (see
// docs/AGENT-PI-PROVIDER.md). A stream carries every session event, including
// the tail of the run ahead of it, and that is what these tests pin down.

type rpcCall struct {
	Method      string
	Params      map[string]any
	Traceparent string
}

// server is a fake agent-pi. Session calls answer themselves; each input call
// is handed to the next entry of inputs, which writes that call's response.
type server struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	calls  []rpcCall
	inputs []func(w *stream)
	listed []map[string]any
}

// stream writes one SSE response.
type stream struct {
	w http.ResponseWriter
	r *http.Request
}

func (s *stream) open(submission int) {
	s.w.Header().Set("Content-Type", "text/event-stream")
	if submission != 0 {
		s.w.Header().Set(submissionHeader, fmt.Sprint(submission))
	}
	s.w.WriteHeader(http.StatusOK)
	s.w.(http.Flusher).Flush()
}

func (s *stream) event(fields map[string]any) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": "session/event", "params": fields})
	fmt.Fprintf(s.w, "id: x-1\nevent: message\ndata: %s\n\n", data)
	s.w.(http.Flusher).Flush()
}

func (s *stream) delta(text string) { s.event(map[string]any{"type": "text.delta", "text": text}) }

func (s *stream) submission(id int, status string) {
	s.event(map[string]any{"type": "submission", "submissionId": id, "status": status})
}

func (s *stream) result(fields map[string]any) {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "result": fields})
	fmt.Fprintf(s.w, "event: message\ndata: %s\n\n", data)
	s.w.(http.Flusher).Flush()
}

func newServer(t *testing.T, inputs ...func(w *stream)) *server {
	s := &server{t: t, inputs: inputs}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.srv.CloseClientConnections)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *server) handle(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Method string         `json:"method"`
		Params map[string]any `json:"params"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		s.t.Errorf("decode request: %v", err)
		return
	}
	if got := r.Header.Get("Authorization"); got != "Bearer token" {
		s.t.Errorf("Authorization = %q", got)
	}
	s.mu.Lock()
	s.calls = append(s.calls, rpcCall{request.Method, request.Params, r.Header.Get("traceparent")})
	var next func(*stream)
	switch request.Method {
	case "session.interrupt", "session.prompt":
		if len(s.inputs) == 0 {
			s.mu.Unlock()
			s.t.Errorf("unexpected %s: %v", request.Method, request.Params)
			return
		}
		next, s.inputs = s.inputs[0], s.inputs[1:]
	}
	listed := s.listed
	s.mu.Unlock()

	writeResult := func(result any) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
	}
	switch request.Method {
	case "session.list":
		writeResult(map[string]any{"sessions": listed})
	case "session.create":
		writeResult(map[string]any{"sessionId": "s1", "profile": request.Params["profile"]})
	case "session.cancel":
		writeResult(map[string]any{"aborted": true})
	default:
		next(&stream{w: w, r: r})
	}
}

// inputs returns what was submitted, in order.
func (s *server) submitted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, call := range s.calls {
		if call.Method == "session.interrupt" || call.Method == "session.prompt" {
			out = append(out, call.Params["input"].(string))
		}
	}
	return out
}

// lastNote is the note on the latest input call.
func (s *server) lastNote() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	note := ""
	for _, call := range s.calls {
		if _, ok := call.Params["input"]; ok {
			note, _ = call.Params["note"].(string)
		}
	}
	return note
}

func (s *server) count(method string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, call := range s.calls {
		if call.Method == method {
			n++
		}
	}
	return n
}

func newLLM(t *testing.T, s *server, whenBusy string) *LLM {
	t.Helper()
	l, err := New(Options{
		URL: s.srv.URL, Tokens: fixedToken("token"), Profile: "voice", WhenBusy: whenBusy,
		AdmissionTimeout: 2 * time.Second, IdleTimeout: 2 * time.Second, StuckGrace: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

func user(texts ...string) []provider.Message {
	var msgs []provider.Message
	for _, text := range texts {
		msgs = append(msgs, provider.Message{Role: "user", Content: text})
	}
	return msgs
}

var alice = provider.WithSession(context.Background(), "alice")

type reply struct {
	text   string
	tokens []string
	err    error
}

// start runs ChatStream in the background, reporting admission on admitted.
func start(ctx context.Context, l *LLM, msgs []provider.Message) (<-chan reply, <-chan struct{}) {
	done, admitted := make(chan reply, 1), make(chan struct{})
	ctx = provider.WithAdmitted(ctx, func() { close(admitted) })
	go func() {
		var r reply
		r.text, r.err = l.ChatStream(ctx, msgs, func(delta string) { r.tokens = append(r.tokens, delta) })
		done <- r
	}()
	return done, admitted
}

func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out")
		panic("unreachable")
	}
}

// waitDelivered blocks until the active call has delivered some text.
func waitDelivered(t *testing.T, l *LLM) {
	t.Helper()
	s := l.session("alice")
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		s.mu.Lock()
		active := s.active
		s.mu.Unlock()
		if active != nil && active.text() != "" {
			return
		}
	}
	t.Fatal("nothing delivered")
}

func TestNewValidates(t *testing.T) {
	if _, err := New(Options{URL: "http://x", Tokens: fixedToken("t"), WhenBusy: "barge"}); err == nil || !strings.Contains(err.Error(), "AGENT_PI_WHEN_BUSY") {
		t.Fatalf("unknown WhenBusy: err = %v", err)
	}
	if _, err := New(Options{URL: "http://x", WhenBusy: WhenBusyInterrupt}); err == nil {
		t.Fatal("missing token: want an error")
	}
}

func TestIdleFirstMessage(t *testing.T) {
	for _, tc := range []struct {
		whenBusy, method string
	}{
		{WhenBusyAuto, "session.interrupt"},
		{WhenBusyInterrupt, "session.interrupt"},
		{WhenBusySteer, "session.prompt"},
		{WhenBusyFollowUp, "session.prompt"},
	} {
		t.Run(tc.whenBusy, func(t *testing.T) {
			s := newServer(t, func(w *stream) {
				w.open(13)
				w.submission(13, "placed")
				w.event(map[string]any{"type": "run.started", "submissionIds": []int{13}})
				w.delta("Hel")
				w.delta("lo.")
				w.submission(13, "done")
				w.result(map[string]any{"status": "done", "submissionId": 13, "text": "Hello."})
			})
			got, err := newLLM(t, s, tc.whenBusy).ChatStream(alice, append([]provider.Message{{Role: "system", Content: "ignored"}}, user("hi")...), nil)
			if err != nil || got != "Hello." {
				t.Fatalf("ChatStream = %q, %v", got, err)
			}
			if inputs := s.submitted(); len(inputs) != 1 || inputs[0] != "hi" {
				t.Fatalf("submitted = %q", inputs)
			}
			if s.count(tc.method) != 1 {
				t.Fatalf("calls = %+v, want one %s", s.calls, tc.method)
			}
			params := s.calls[len(s.calls)-1].Params
			if tc.method == "session.prompt" && params["whenBusy"] != tc.whenBusy {
				t.Fatalf("whenBusy = %v", params["whenBusy"])
			}
			if got, want := params["duringTools"], map[string]any{WhenBusyAuto: "steer"}[tc.whenBusy]; got != want {
				t.Fatalf("duringTools = %v, want %v", got, want)
			}
			if s.calls[1].Method != "session.create" || s.calls[1].Params["metadata"].(map[string]any)["identity"] != "alice" {
				t.Fatalf("session.create = %+v", s.calls[1])
			}
		})
	}
}

func TestInterruptDuringText(t *testing.T) {
	second, aborted := make(chan struct{}), make(chan struct{})
	s := newServer(t,
		func(w *stream) {
			w.open(13)
			w.submission(13, "placed")
			w.delta("First answer")
			close(second)
			<-aborted
			w.event(map[string]any{"type": "message.end", "text": "First answer", "stopReason": "aborted"})
			w.submission(13, "unanswered")
			w.result(map[string]any{"status": "unanswered", "reason": "aborted"})
		},
		func(w *stream) {
			close(aborted)
			w.open(19)
			// The tail of A's run reaches B too, and must not be taken for B's.
			w.delta(" tail of A")
			w.event(map[string]any{"type": "message.end", "text": "First answer tail of A", "stopReason": "aborted"})
			w.submission(13, "unanswered")
			w.submission(19, "placed")
			w.delta("Second.")
			w.submission(19, "done")
			w.result(map[string]any{"status": "done", "text": "Second."})
		},
	)
	l := newLLM(t, s, WhenBusyInterrupt)
	first, _ := start(alice, l, user("one"))
	wait(t, second)
	waitDelivered(t, l) // the partial reply goes into the next input
	got, err := l.ChatStream(alice, user("one", "two"), nil)
	if err != nil || got != "Second." {
		t.Fatalf("second = %q, %v", got, err)
	}
	a := wait(t, first)
	if !errors.Is(a.err, provider.ErrSuperseded) || a.text != "First answer" {
		t.Fatalf("first = %q, %v, want its partial text and ErrSuperseded", a.text, a.err)
	}
	if inputs := s.submitted(); len(inputs) != 2 || inputs[1] != "two" {
		t.Fatalf("submitted = %q", inputs)
	}
	if note := s.lastNote(); note != "Your previous reply was cut off. The caller heard at most: \"First answer\". Do not mention this." {
		t.Fatalf("note = %q", note)
	}
}

func TestSteerDuringText(t *testing.T) {
	second := make(chan struct{})
	finish := make(chan struct{})
	s := newServer(t,
		func(w *stream) {
			w.open(13)
			w.submission(13, "placed")
			w.delta("First ")
			close(second)
			<-finish
			w.delta("answer.")
			w.submission(13, "done")
			w.submission(17, "placed")
			w.result(map[string]any{"status": "done", "text": "First answer."})
		},
		func(w *stream) {
			w.open(17)
			w.submission(17, "queued")
			close(finish)
			w.delta("answer.")
			w.submission(13, "done")
			w.submission(17, "placed")
			w.delta("Second.")
			w.submission(17, "done")
			w.result(map[string]any{"status": "done", "text": "Second."})
		},
	)
	l := newLLM(t, s, WhenBusySteer)
	first, _ := start(alice, l, user("one"))
	wait(t, second)
	got, err := l.ChatStream(alice, user("one", "two"), nil)
	if err != nil || got != "Second." {
		t.Fatalf("second = %q, %v", got, err)
	}
	if a := wait(t, first); a.err != nil || a.text != "First answer." {
		t.Fatalf("first = %q, %v", a.text, a.err)
	}
	// The first reply is delivered in full, so there is nothing to correct.
	if inputs := s.submitted(); inputs[1] != "two" {
		t.Fatalf("submitted = %q", inputs)
	}
}

func TestSteerDuringToolsHandsOverAtPlacement(t *testing.T) {
	second, placed := make(chan struct{}), make(chan struct{})
	s := newServer(t,
		func(w *stream) {
			w.open(13)
			w.submission(13, "placed")
			w.delta("Let me check.")
			close(second)
			<-placed
			w.submission(19, "placed")
			// The client should have gone by now; this must not reach A's reply.
			w.delta("Joined answer.")
			w.result(map[string]any{"status": "done", "text": "Joined answer."})
		},
		func(w *stream) {
			w.open(19)
			w.submission(19, "queued")
			close(placed)
			w.submission(19, "placed")
			w.delta("Joined answer.")
			w.result(map[string]any{"status": "done", "text": "Joined answer."})
		},
	)
	l := newLLM(t, s, WhenBusySteer)
	first, _ := start(alice, l, user("one"))
	wait(t, second)
	got, err := l.ChatStream(alice, user("one", "two"), nil)
	if err != nil || got != "Joined answer." {
		t.Fatalf("second = %q, %v", got, err)
	}
	if a := wait(t, first); !errors.Is(a.err, provider.ErrSuperseded) || a.text != "Let me check." {
		t.Fatalf("first = %q, %v", a.text, a.err)
	}
}

func TestQueuedBehindFailedRunRecovers(t *testing.T) {
	for _, whenBusy := range []string{WhenBusyAuto, WhenBusySteer} {
		t.Run(whenBusy, func(t *testing.T) { testQueuedBehindFailedRun(t, whenBusy) })
	}
}

func testQueuedBehindFailedRun(t *testing.T, whenBusy string) {
	s := newServer(t,
		func(w *stream) {
			w.open(17)
			w.submission(17, "queued")
			w.event(map[string]any{"type": "message.end", "text": "Partial", "stopReason": "error"})
			w.event(map[string]any{"type": "submission", "submissionId": 13, "status": "unanswered", "reason": "model_error"})
			// Durable never places 17 now; the stream just stays open.
			<-w.r.Context().Done()
		},
		func(w *stream) {
			w.open(20)
			w.submission(20, "placed")
			w.delta("Recovered.")
			w.result(map[string]any{"status": "done", "text": "Recovered."})
		},
	)
	got, err := newLLM(t, s, whenBusy).ChatStream(alice, user("two"), nil)
	if err != nil || got != "Recovered." {
		t.Fatalf("ChatStream = %q, %v", got, err)
	}
	if s.count("session.cancel") != 1 {
		t.Fatalf("cancels = %d, want 1", s.count("session.cancel"))
	}
	if inputs := s.submitted(); len(inputs) != 2 || inputs[0] != "two" || inputs[1] != "two" {
		t.Fatalf("submitted = %q", inputs)
	}
	calls := s.calls
	var ids []any
	for _, call := range calls {
		if call.Method == "session.prompt" || call.Method == "session.interrupt" {
			ids = append(ids, call.Params["requestId"])
		}
	}
	if ids[0] == ids[1] {
		t.Fatalf("resubmission reused requestId %v, so durable would answer with the first outcome", ids[0])
	}
}

func TestEveryCallCarriesTheTurn(t *testing.T) {
	s := newServer(t,
		func(w *stream) {
			w.open(17)
			w.submission(17, "queued")
			w.event(map[string]any{"type": "submission", "submissionId": 13, "status": "unanswered", "reason": "model_error"})
			<-w.r.Context().Done()
		},
		func(w *stream) {
			w.open(20)
			w.submission(20, "placed")
			w.result(map[string]any{"status": "done", "text": "Recovered."})
		},
	)
	turn := telemetry.StartTurn(alice, telemetry.TurnOptions{})
	defer turn.End(telemetry.OutcomeCompleted, nil)
	id := telemetry.TurnID(turn.Context())

	if _, err := newLLM(t, s, WhenBusyAuto).ChatStream(turn.Context(), user("two"), nil); err != nil {
		t.Fatal(err)
	}
	inputs := 0
	for _, call := range s.calls {
		if !strings.Contains(call.Traceparent, id) {
			t.Errorf("%s traceparent = %q, want turn %s", call.Method, call.Traceparent, id)
		}
		if call.Method == "session.interrupt" {
			inputs++
			if call.Params["turnId"] != id {
				t.Errorf("turnId = %v, want %s: a resubmission is the same turn", call.Params["turnId"], id)
			}
		}
	}
	if inputs != 2 {
		t.Fatalf("inputs = %d, want 2", inputs)
	}
}

func TestWithdrawnInputIsResubmittedWithoutCancelling(t *testing.T) {
	s := newServer(t,
		func(w *stream) {
			w.open(18)
			w.submission(18, "queued")
			w.result(map[string]any{"status": "unanswered", "reason": "aborted"})
		},
		func(w *stream) {
			w.open(21)
			w.submission(21, "placed")
			w.delta("Answer.")
			w.result(map[string]any{"status": "done", "text": "Answer."})
		},
	)
	got, err := newLLM(t, s, WhenBusySteer).ChatStream(alice, user("three"), nil)
	if err != nil || got != "Answer." {
		t.Fatalf("ChatStream = %q, %v", got, err)
	}
	if s.count("session.cancel") != 0 {
		t.Fatal("a withdrawn input needs no cancel")
	}
}

func TestBargeInDuringAdmissionSendsInputOnce(t *testing.T) {
	admitting, proceed := make(chan struct{}), make(chan struct{})
	s := newServer(t,
		func(w *stream) {
			close(admitting)
			<-proceed
			w.open(13)
			w.submission(13, "placed")
			<-w.r.Context().Done()
		},
		func(w *stream) {
			w.open(19)
			w.submission(19, "placed")
			w.delta("Second.")
			w.result(map[string]any{"status": "done", "text": "Second."})
		},
	)
	l := newLLM(t, s, WhenBusyInterrupt)
	ctx, cancel := context.WithCancel(alice)
	first, _ := start(ctx, l, user("one"))
	wait(t, admitting)
	cancel()
	close(proceed)
	if a := wait(t, first); !errors.Is(a.err, context.Canceled) {
		t.Fatalf("first err = %v", a.err)
	}
	if _, err := l.ChatStream(alice, user("one", "two"), nil); err != nil {
		t.Fatalf("second: %v", err)
	}
	if inputs := s.submitted(); len(inputs) != 2 || inputs[1] != "two" {
		t.Fatalf("submitted = %q, want one's words only once", inputs)
	}
}

func TestBargeInCarriesWhatWasHeard(t *testing.T) {
	one := func(w *stream) {
		w.open(13)
		w.submission(13, "placed")
		w.delta("Hangzhou is the capital of Zhejiang and has a famous lake.")
		<-w.r.Context().Done()
	}
	two := func(w *stream) {
		w.open(19)
		w.submission(19, "placed")
		w.result(map[string]any{"status": "done", "text": "Shanghai."})
	}
	s := newServer(t, one, two)
	l := newLLM(t, s, WhenBusyInterrupt)
	ctx, cancel := context.WithCancel(alice)
	first, _ := start(ctx, l, user("one"))
	waitDelivered(t, l)
	cancel()
	wait(t, first)

	msgs := append(user("one"),
		provider.Message{Role: "assistant", Content: "Hangzhou is the capital" + provider.InterruptedMarker},
		provider.Message{Role: "user", Content: "two"})
	if _, err := l.ChatStream(alice, msgs, nil); err != nil {
		t.Fatalf("second: %v", err)
	}
	// What the engine says was spoken, not the longer text that was generated.
	if inputs := s.submitted(); inputs[1] != "two" {
		t.Fatalf("submitted = %q", inputs)
	}
	if note := s.lastNote(); note != "Your previous reply was cut off. The caller heard at most: \"Hangzhou is the capital\". Do not mention this." {
		t.Fatalf("note = %q", note)
	}
}

func TestRefusedInputIsCarriedIntoTheNextTurn(t *testing.T) {
	s := newServer(t,
		func(w *stream) {
			w.w.Header().Set("Content-Type", "application/json")
			w.w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprint(w.w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32029,"message":"the server is at capacity"}}`)
		},
		func(w *stream) {
			w.open(13)
			w.result(map[string]any{"status": "done", "text": "OK."})
		},
	)
	l := newLLM(t, s, WhenBusyInterrupt)
	if _, err := l.ChatStream(alice, user("one"), nil); err == nil || !strings.Contains(err.Error(), "at capacity") {
		t.Fatalf("err = %v", err)
	}
	if _, err := l.ChatStream(alice, user("one", "two"), nil); err != nil {
		t.Fatalf("second: %v", err)
	}
	if inputs := s.submitted(); inputs[1] != "one\ntwo" {
		t.Fatalf("submitted = %q", inputs)
	}
}

func TestLostSessionIsRecreatedOnce(t *testing.T) {
	s := newServer(t,
		func(w *stream) {
			w.w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w.w, `{"error":{"message":"session not found"}}`)
		},
		func(w *stream) {
			w.open(1)
			w.result(map[string]any{"status": "done", "text": "OK."})
		},
	)
	if _, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("one"), nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if s.count("session.create") != 2 {
		t.Fatalf("creates = %d, want 2", s.count("session.create"))
	}
}

func TestExistingSessionIsReused(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(1)
		w.result(map[string]any{"status": "done", "text": "OK."})
	})
	s.listed = []map[string]any{
		{"sessionId": "other", "profile": "voice", "metadata": map[string]any{"identity": "bob"}},
		{"sessionId": "chat", "profile": "chat", "metadata": map[string]any{"identity": "alice"}},
		{"sessionId": "mine", "profile": "voice", "metadata": map[string]any{"identity": "alice"}},
	}
	if _, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("one"), nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if s.count("session.create") != 0 {
		t.Fatal("created a session instead of reusing one")
	}
	if got := s.calls[len(s.calls)-1].Params["sessionId"]; got != "mine" {
		t.Fatalf("sessionId = %v", got)
	}
}

func TestPluginReplyIsDeliveredFromTheResult(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(0)
		// Busy session: events of the run in progress, none of them the reply.
		w.delta("somebody else's text")
		w.result(map[string]any{"status": "done", "answeredBy": "shortcuts", "text": "Hi, I'm Ava."})
	})
	var tokens []string
	got, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("hello"), func(d string) { tokens = append(tokens, d) })
	if err != nil || got != "Hi, I'm Ava." || len(tokens) != 1 {
		t.Fatalf("ChatStream = %q, %v, tokens %q", got, err, tokens)
	}
}

func TestReplyIncludesTextBeforeTools(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(13)
		w.submission(13, "placed")
		w.delta("Let me check. ")
		w.event(map[string]any{"type": "message.end", "text": "Let me check. ", "stopReason": "toolUse"})
		w.delta("It's sunny.")
		w.submission(13, "done")
		w.result(map[string]any{"status": "done", "text": "It's sunny."})
	})
	got, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("weather?"), nil)
	if err != nil || got != "Let me check. It's sunny." {
		t.Fatalf("ChatStream = %q, %v, want what was spoken, not just the final message", got, err)
	}
}

func TestModelErrorKeepsPartialText(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(13)
		w.submission(13, "placed")
		w.delta("Partial")
		w.result(map[string]any{"status": "unanswered", "reason": "model_error", "detail": "boom"})
	})
	got, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("one"), nil)
	if err == nil || !strings.Contains(err.Error(), "boom") || got != "Partial" {
		t.Fatalf("ChatStream = %q, %v", got, err)
	}
}

func TestSilentStreamFails(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(13)
		w.submission(13, "placed")
		<-w.r.Context().Done()
	})
	l := newLLM(t, s, WhenBusyInterrupt)
	l.opts.IdleTimeout = 30 * time.Millisecond
	if _, err := l.ChatStream(alice, user("one"), nil); !errors.Is(err, errIdle) {
		t.Fatalf("err = %v, want errIdle", err)
	}
}

func TestStreamEndingWithoutAResultFails(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(13)
		w.submission(13, "placed")
	})
	if _, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(alice, user("one"), nil); err == nil {
		t.Fatal("want an error")
	}
}

func TestWordsAlreadyCarriedAreSuperseded(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.open(1)
		w.result(map[string]any{"status": "done", "text": "OK."})
	})
	l := newLLM(t, s, WhenBusyInterrupt)
	msgs := user("one", "two")
	if _, err := l.ChatStream(alice, msgs, nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if _, err := l.ChatStream(alice, msgs[:1], nil); !errors.Is(err, provider.ErrSuperseded) {
		t.Fatalf("err = %v", err)
	}
}

func TestMissingSessionFails(t *testing.T) {
	s := newServer(t)
	if _, err := newLLM(t, s, WhenBusyInterrupt).ChatStream(context.Background(), user("one"), nil); err == nil {
		t.Fatal("want an error")
	}
}

type fixedToken string

func (f fixedToken) Token(context.Context, string) (string, error) { return string(f), nil }

// recordingTokens answers with a token named after the grant it was asked for.
type recordingTokens struct {
	mu     sync.Mutex
	grants []string
	err    error
}

func (r *recordingTokens) Token(_ context.Context, grantID string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.grants = append(r.grants, grantID)
	return "token-of-" + grantID, r.err
}

func TestEveryCallerReachesAgentPiWithTheirOwnToken(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Authorization"))
		mu.Unlock()
		http.Error(w, "stop here", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	tokens := &recordingTokens{}
	l, err := New(Options{URL: srv.URL, Tokens: tokens, Profile: "voice", WhenBusy: WhenBusyAuto})
	if err != nil {
		t.Fatal(err)
	}
	for _, caller := range []string{"ada", "grace"} {
		ctx := provider.WithGrant(provider.WithSession(context.Background(), caller), "grt_"+caller)
		_, _ = l.ChatStream(ctx, []provider.Message{{Role: "user", Content: "hi"}}, func(string) {})
	}
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(seen, []string{"Bearer token-of-grt_ada", "Bearer token-of-grt_grace"}) {
		t.Fatalf("agent-pi saw %q", seen)
	}
}

func TestATokenFailureFailsTheTurnBeforeAnythingIsSent(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
	defer srv.Close()
	stopped := errors.New("grant stopped")
	l, err := New(Options{URL: srv.URL, Tokens: &recordingTokens{err: stopped}, Profile: "voice", WhenBusy: WhenBusyAuto})
	if err != nil {
		t.Fatal(err)
	}
	ctx := provider.WithSession(context.Background(), "ada")
	_, err = l.ChatStream(ctx, []provider.Message{{Role: "user", Content: "hi"}}, func(string) {})
	if !errors.Is(err, stopped) {
		t.Fatalf("got %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("reached agent-pi with no token")
	}
}

func answered(w *stream) {
	w.open(1)
	w.result(map[string]any{"status": "done", "text": "OK."})
}

func TestACallJoinsTheConversationItNames(t *testing.T) {
	s := newServer(t, answered)
	ctx := provider.WithLiveCall(provider.WithConversation(alice, "chat-1"))
	if _, err := newLLM(t, s, WhenBusyAuto).ChatStream(ctx, user("one"), nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if s.count("session.list") != 0 || s.count("session.create") != 0 {
		t.Fatalf("list = %d, create = %d, want neither", s.count("session.list"), s.count("session.create"))
	}
	var input map[string]any
	for _, call := range s.calls {
		if call.Method == "session.interrupt" {
			input = call.Params
		}
	}
	if input["sessionId"] != "chat-1" || input["input"] != "one" {
		t.Fatalf("params = %v", input)
	}
	if note, _ := input["note"].(string); !strings.Contains(note, "read aloud") {
		t.Fatalf("note = %q", note)
	}
}

func TestAVoiceSessionNeedsNoSpokenNote(t *testing.T) {
	s := newServer(t, answered)
	if _, err := newLLM(t, s, WhenBusyAuto).ChatStream(alice, user("one"), nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if note := s.lastNote(); note != "" {
		t.Fatalf("note = %q", note)
	}
}

func TestAMessageInACallRoomIsNotSpoken(t *testing.T) {
	s := newServer(t, answered)
	if _, err := newLLM(t, s, WhenBusyAuto).ChatStream(provider.WithConversation(alice, "chat-1"), user("one"), nil); err != nil {
		t.Fatalf("ChatStream: %v", err)
	}
	if note := s.lastNote(); note != "" {
		t.Fatalf("note = %q", note)
	}
}

func TestACutOffCallTurnKeepsBothNotes(t *testing.T) {
	s := newServer(t, answered, answered)
	l := newLLM(t, s, WhenBusyAuto)
	ctx := provider.WithLiveCall(provider.WithConversation(alice, "chat-1"))
	if _, err := l.ChatStream(ctx, user("one"), nil); err != nil {
		t.Fatalf("first: %v", err)
	}
	msgs := append(user("one"), provider.Message{Role: "assistant", Content: "Hang" + provider.InterruptedMarker}, provider.Message{Role: "user", Content: "two"})
	if _, err := l.ChatStream(ctx, msgs, nil); err != nil {
		t.Fatalf("second: %v", err)
	}
	if note := s.lastNote(); !strings.Contains(note, "read aloud") || !strings.Contains(note, `heard at most: "Hang"`) {
		t.Fatalf("note = %q", note)
	}
}

func TestANamedConversationIsNeverReplaced(t *testing.T) {
	s := newServer(t, func(w *stream) {
		w.w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w.w, `{"error":{"message":"session not found"}}`)
	})
	_, err := newLLM(t, s, WhenBusyAuto).ChatStream(provider.WithConversation(alice, "someone-elses"), user("one"), nil)
	if err == nil || !strings.Contains(err.Error(), "session not found") {
		t.Fatalf("err = %v", err)
	}
	if s.count("session.create") != 0 {
		t.Fatal("created a session in place of the one the room named")
	}
}

func TestARoomKeepsItsSessionAcrossStays(t *testing.T) {
	s := newServer(t, answered, answered)
	s.listed = []map[string]any{{"sessionId": "kept", "profile": "voice", "metadata": map[string]any{"identity": "voice-agent-demo/alice"}}}
	l := newLLM(t, s, WhenBusyAuto)
	first := provider.ForRoom(l, "1", "voice-agent-demo", "")
	if _, err := first.ChatStream(alice, user("one"), nil); err != nil {
		t.Fatalf("first stay: %v", err)
	}
	first.Release()
	if _, err := provider.ForRoom(l, "2", "voice-agent-demo", "").ChatStream(alice, user("two"), nil); err != nil {
		t.Fatalf("second stay: %v", err)
	}
	if s.count("session.create") != 0 {
		t.Fatalf("creates = %d, want 0", s.count("session.create"))
	}
}

func TestRoomsKeepTheirOwnPlace(t *testing.T) {
	s := newServer(t, answered, answered, answered)
	l := newLLM(t, s, WhenBusyAuto)
	one, two := provider.ForRoom(l, "1", "room-1", "chat-1"), provider.ForRoom(l, "2", "room-2", "chat-2")
	if _, err := one.ChatStream(alice, user("one"), nil); err != nil {
		t.Fatalf("room one: %v", err)
	}
	if _, err := two.ChatStream(alice, user("two"), nil); err != nil {
		t.Fatalf("room two: %v", err)
	}
	if inputs := s.submitted(); len(inputs) != 2 || inputs[1] != "two" {
		t.Fatalf("submitted = %q", inputs)
	}

	// A new stay starts a new history, so an old cursor would skip its first words.
	one.Release()
	if _, err := one.ChatStream(alice, user("back"), nil); err != nil {
		t.Fatalf("after release: %v", err)
	}
	if inputs := s.submitted(); len(inputs) != 3 || inputs[2] != "back" {
		t.Fatalf("submitted = %q", inputs)
	}
	if got := len(l.sessions); got != 2 {
		t.Fatalf("sessions held = %d, want 2", got)
	}
}
