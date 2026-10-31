// Package agentpi implements provider.LLMProvider against an agent-pi server
// over Streamable HTTP. Unlike openaicompat it is stateful: agent-pi owns the
// transcript, system prompt, tools and model, so each call submits only the
// user messages it has not sent yet and follows the stream that answers them.
//
// Every message is submitted the same way whether or not the agent is busy
// (see Options.WhenBusy), so there is no busy check to race against. See
// docs/AGENT-PI-PROVIDER.md for the design and the event orderings it relies on.
package agentpi

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

func init() {
	provider.RegisterLLM("agent_pi", func(cfg config.Config) (provider.LLMProvider, error) {
		return New(Options{URL: cfg.AgentPiURL, Token: cfg.AgentPiToken, Profile: cfg.AgentPiProfile, WhenBusy: cfg.AgentPiWhenBusy})
	})
}

// What a message does when the agent is already working.
const (
	WhenBusyAuto      = "auto"      // abort text being generated, but let running tools finish and join them
	WhenBusyInterrupt = "interrupt" // abort the work, tools included, and answer the new message
	WhenBusySteer     = "steer"     // join at the next tool-round boundary
	WhenBusyFollowUp  = "followUp"  // wait for the current run to finish
)

const (
	sessionHeader    = "Agent-Session-Id"
	submissionHeader = "Agent-Submission-Id"

	defaultAdmissionTimeout = 10 * time.Second
	// Three of agent-pi's 15-second keep-alives.
	defaultIdleTimeout = 45 * time.Second
	// How long a queued input may stay unplaced after the run ahead of it
	// failed. A healthy handover places it in the same commit.
	defaultStuckGrace = 500 * time.Millisecond
)

// Options configures New. The timeouts default when zero.
type Options struct {
	URL      string
	Token    string
	Profile  string
	WhenBusy string

	AdmissionTimeout time.Duration
	IdleTimeout      time.Duration
	StuckGrace       time.Duration
}

// LLM is a client of one agent-pi endpoint, under one service identity.
type LLM struct {
	opts   Options
	client *http.Client

	mu       sync.Mutex
	sessions map[string]*session
}

// session is one conversation's agent-pi session. mu is held until an input is
// admitted, which is what lets the next interrupt find the previous input
// placed and stop it: two sent at once would queue instead.
type session struct {
	mu     sync.Mutex
	id     string
	sent   int   // how far into the msgs snapshot has been submitted
	active *call // the newest call, while its stream is open
}

func New(opts Options) (*LLM, error) {
	if opts.URL == "" {
		return nil, errors.New("agentpi: AGENT_PI_URL is required")
	}
	if opts.Token == "" {
		return nil, errors.New("agentpi: AGENT_PI_TOKEN is required")
	}
	switch opts.WhenBusy {
	case WhenBusyAuto, WhenBusyInterrupt, WhenBusySteer, WhenBusyFollowUp:
	default:
		return nil, fmt.Errorf("agentpi: AGENT_PI_WHEN_BUSY must be %s, %s, %s or %s, got %q",
			WhenBusyAuto, WhenBusyInterrupt, WhenBusySteer, WhenBusyFollowUp, opts.WhenBusy)
	}
	if opts.AdmissionTimeout == 0 {
		opts.AdmissionTimeout = defaultAdmissionTimeout
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = defaultIdleTimeout
	}
	if opts.StuckGrace == 0 {
		opts.StuckGrace = defaultStuckGrace
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = opts.AdmissionTimeout
	// No Client.Timeout: it would cover the whole body, and a tool round can
	// legitimately outlast it. The idle watchdog in follow bounds the stream.
	return &LLM{opts: opts, client: &http.Client{Transport: transport}, sessions: map[string]*session{}}, nil
}

// ChatStream submits the unsent user messages in msgs and streams the reply.
// Cancelling ctx closes this stream and nothing else: the run goes on until
// the next message reaches it. A barge-in cancels at speech start, before
// anyone knows whether it was words or a cough, so the abort has to wait for
// the transcript.
func (l *LLM) ChatStream(ctx context.Context, msgs []provider.Message, onToken func(delta string)) (string, error) {
	key := provider.SessionFrom(ctx)
	if key == "" {
		return "", errors.New("agentpi: no session on context")
	}
	s := l.session(key)

	s.mu.Lock()
	input, cursor := s.pending(msgs, l.opts.WhenBusy == WhenBusyAuto || l.opts.WhenBusy == WhenBusyInterrupt)
	if input == "" {
		s.mu.Unlock()
		// A concurrent turn already carried these words.
		return "", provider.ErrSuperseded
	}
	c, err := l.admit(ctx, s, input)
	if err == nil {
		s.sent = cursor
	}
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	provider.Admitted(ctx)

	full, err := c.follow(ctx, l.opts, onToken)
	if !errors.Is(err, errStuck) && !errors.Is(err, errWithdrawn) {
		return full, err
	}

	// Durable leaves a queued input in the inbox when the run ahead of it
	// fails, until the next submission. Withdraw it and submit it again, once.
	s.mu.Lock()
	if errors.Is(err, errStuck) {
		err = l.rpc(ctx, "session.cancel", map[string]any{"sessionId": s.id}, nil)
	} else {
		err = nil
	}
	if err == nil {
		c, err = l.admit(ctx, s, input)
	}
	s.mu.Unlock()
	if err != nil {
		return "", err
	}
	return c.follow(ctx, l.opts, onToken)
}

func (l *LLM) session(key string) *session {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.sessions[key]
	if !ok {
		s = &session{}
		l.sessions[key] = s
	}
	return s
}

// pending returns the next input and the cursor to store once it is admitted.
// A cursor rather than "the trailing user messages" because an interruption
// before anything was spoken appends no assistant message, and a second message
// arrives before the first has a reply: both would resend the first message.
func (s *session) pending(msgs []provider.Message, interrupts bool) (string, int) {
	if s.sent >= len(msgs) {
		return "", s.sent
	}
	var users []string
	heard := ""
	for _, m := range msgs[s.sent:] {
		switch m.Role {
		case "user":
			users = append(users, m.Content)
		case "assistant":
			if said, ok := strings.CutSuffix(m.Content, provider.InterruptedMarker); ok {
				heard = said
			}
		}
	}
	if len(users) == 0 {
		return "", len(msgs)
	}
	if heard == "" && interrupts && s.active.open() {
		// The turn being cut off is still running, so its partial reply is
		// not in the history yet.
		heard = s.active.text()
	}
	input := strings.Join(users, "\n")
	if heard != "" {
		// The model never sees an aborted answer, and a live reply generated
		// further than the caller heard: tell it what actually arrived. "At
		// most" because the text is whole chunks, some cut off mid-playback.
		input = "[Your previous reply was cut off. The caller heard at most: \"" + heard + "\". Do not mention this.]\n" + input
	}
	return input, len(msgs)
}

// admit makes sure the session exists and submits input. Callers hold s.mu.
//
// Admission ignores ctx: agent-pi admits a request even if the client leaves,
// so a barge-in during the POST would leave it unknown whether the input went
// in, and the next turn would send it twice. SSE headers are sent only after
// admission, so a response means the input is durably in.
func (l *LLM) admit(ctx context.Context, s *session, input string) (*call, error) {
	if s.id == "" {
		if err := l.openSession(ctx, s); err != nil {
			return nil, err
		}
	}
	resp, err := l.submit(ctx, s, input)
	var refused *refusedError
	if errors.As(err, &refused) && refused.status == http.StatusNotFound {
		s.id = ""
		if err := l.openSession(ctx, s); err != nil {
			return nil, err
		}
		resp, err = l.submit(ctx, s, input)
	}
	if err != nil {
		return nil, err
	}
	setSpanAttributes(ctx, attribute.String("agentpi.session_id", s.id))
	c := &call{body: resp.Body, admittedAt: time.Now()}
	if id := resp.Header.Get(submissionHeader); id != "" {
		if c.mine, err = strconv.ParseInt(id, 10, 64); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("agentpi: bad %s header %q", submissionHeader, id)
		}
	}
	s.active = c
	return c, nil
}

// openSession finds this conversation's session or creates one. Reuse matters
// because every identity shares one token, and agent-pi caps sessions per user:
// creating one on every start would exhaust the limit after a few restarts.
func (l *LLM) openSession(ctx context.Context, s *session) error {
	key := provider.SessionFrom(ctx)
	var list struct {
		Sessions []struct {
			SessionID string         `json:"sessionId"`
			Profile   string         `json:"profile"`
			Metadata  map[string]any `json:"metadata"`
		} `json:"sessions"`
	}
	if err := l.rpc(ctx, "session.list", map[string]any{}, &list); err != nil {
		return err
	}
	for _, info := range list.Sessions {
		if info.Profile == l.opts.Profile && info.Metadata["identity"] == key {
			s.id = info.SessionID
			return nil
		}
	}
	var created struct {
		SessionID string `json:"sessionId"`
	}
	params := map[string]any{"profile": l.opts.Profile, "metadata": map[string]any{"identity": key}}
	if err := l.rpc(ctx, "session.create", params, &created); err != nil {
		return err
	}
	s.id = created.SessionID
	return nil
}

// submit POSTs input and returns once it is admitted. It retries a failed
// connection once with the same requestId: durable deduplicates on it, and a
// retried interrupt does not abort a second time.
func (l *LLM) submit(ctx context.Context, s *session, input string) (*http.Response, error) {
	params := map[string]any{"sessionId": s.id, "input": input, "requestId": newRequestID()}
	if id := telemetry.TurnID(ctx); id != "" {
		params["turnId"] = id
	}
	method := "session.interrupt"
	switch l.opts.WhenBusy {
	case WhenBusyAuto:
		// agent-pi decides: by the time a barge-in's transcript arrives this
		// client's stream is closed, so only the server knows if tools are running.
		params["duringTools"] = "steer"
	case WhenBusySteer, WhenBusyFollowUp:
		method = "session.prompt"
		params["whenBusy"] = l.opts.WhenBusy
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return nil, fmt.Errorf("agentpi: marshal %s: %w", method, err)
	}
	var resp *http.Response
	for attempt := 0; attempt < 2; attempt++ {
		var req *http.Request
		req, err = http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, l.opts.URL, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("agentpi: build %s: %w", method, err)
		}
		l.setHeaders(req, s.id, "application/json, text/event-stream")
		setTraceparent(ctx, req)
		if resp, err = l.client.Do(req); err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("agentpi: %s: %w", method, err)
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, refusal(method, resp)
	}
	if !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf("agentpi: %s answered %q, not a stream", method, resp.Header.Get("Content-Type"))
	}
	return resp, nil
}

// rpc makes one JSON call, decoding its result into out when out is not nil.
// Like admission, it runs to the end whatever happens to ctx.
func (l *LLM) rpc(ctx context.Context, method string, params map[string]any, out any) error {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
	if err != nil {
		return fmt.Errorf("agentpi: marshal %s: %w", method, err)
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.opts.AdmissionTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.opts.URL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("agentpi: build %s: %w", method, err)
	}
	l.setHeaders(req, "", "application/json")
	setTraceparent(ctx, req)
	resp, err := l.client.Do(req)
	if err != nil {
		return fmt.Errorf("agentpi: %s: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return refusal(method, resp)
	}
	var message struct {
		Result json.RawMessage `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&message); err != nil {
		return fmt.Errorf("agentpi: decode %s: %w", method, err)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(message.Result, out); err != nil {
		return fmt.Errorf("agentpi: decode %s result: %w", method, err)
	}
	return nil
}

func (l *LLM) setHeaders(req *http.Request, sessionID, accept string) {
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+l.opts.Token)
	if sessionID != "" {
		req.Header.Set(sessionHeader, sessionID)
	}
}

// setTraceparent makes agent-pi's work for this request part of the turn's trace.
func setTraceparent(ctx context.Context, req *http.Request) {
	if traceparent := telemetry.Traceparent(ctx); traceparent != "" {
		req.Header.Set("traceparent", traceparent)
	}
}

// refusedError is a request agent-pi answered with an error status, so nothing
// was admitted.
type refusedError struct {
	method  string
	status  int
	message string
}

func (e *refusedError) Error() string {
	return fmt.Sprintf("agentpi: %s refused with status %d: %s", e.method, e.status, e.message)
}

func refusal(method string, resp *http.Response) error {
	payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	message := string(payload)
	if json.Unmarshal(payload, &body) == nil && body.Error.Message != "" {
		message = body.Error.Message
	}
	return &refusedError{method: method, status: resp.StatusCode, message: message}
}

func newRequestID() string {
	var id [16]byte
	_, _ = rand.Read(id[:])
	return hex.EncodeToString(id[:])
}

func setSpanAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}
