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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"t10-livekit-agent/internal/authclient"
	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

func init() {
	provider.RegisterLLM("agent_pi", func(cfg config.Config) (provider.LLMProvider, error) {
		tokens, err := authclient.New(authclient.Options{URL: cfg.AuthURL, ClientID: cfg.AuthClientID, ClientSecret: cfg.AuthClientSecret})
		if err != nil {
			return nil, err
		}
		replicas, err := parseReplicas(cfg.AgentURLs)
		if err != nil {
			return nil, err
		}
		return New(Options{URL: cfg.AgentPiURL, Replicas: replicas, RoutesFile: cfg.AgentRoutesFile, Tokens: tokens, Profile: cfg.AgentPiProfile, WhenBusy: cfg.AgentPiWhenBusy})
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

	// A voice session's profile says this already. A conversation started by
	// typing doesn't, and keeps its model: only the style changes for a call.
	// It stays in the history, so it speaks only of this message: later typed
	// messages in the conversation aren't bound by it.
	spokenNote = "This message was said on a live call, and your reply to it is read aloud as you write it. " +
		"Answer it in one to three short sentences. Use plain sentences with no markdown, lists, tables, URLs or code. " +
		"Say numbers, dates and times the way a person would. Answer in the language they are speaking."
)

// TokenSource turns the grant on a turn into a token for agent-pi. Tokens last
// minutes, so one is fetched for each request rather than held.
type TokenSource interface {
	Token(ctx context.Context, grantID string) (string, error)
}

// Options configures New. The timeouts default when zero.
type Options struct {
	URL string
	// Replicas maps a session ID's prefix to its replica's URL. URL takes the rest.
	Replicas map[string]string
	// RoutesFile is a routing document. When set, Replicas isn't read: sources
	// don't fall through to each other.
	RoutesFile string
	Tokens     TokenSource
	Profile    string
	WhenBusy   string

	AdmissionTimeout time.Duration
	IdleTimeout      time.Duration
	StuckGrace       time.Duration
}

// LLM is a client of one agent-pi endpoint. Each caller reaches it as themselves,
// through the grant on their turn.
type LLM struct {
	opts   Options
	client *http.Client

	routes   *routeFile
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
	if opts.Tokens == nil {
		return nil, errors.New("agentpi: a token source is required")
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
	var routes *routeFile
	if opts.RoutesFile != "" {
		var err error
		if routes, err = openRouteFile(opts.RoutesFile, time.Now); err != nil {
			return nil, err
		}
	}
	return &LLM{opts: opts, client: &http.Client{Transport: transport}, routes: routes, sessions: map[string]*session{}}, nil
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
	input, note, cursor := s.pending(msgs, l.opts.WhenBusy == WhenBusyAuto || l.opts.WhenBusy == WhenBusyInterrupt)
	if provider.OnLiveCall(ctx) && provider.ConversationFrom(ctx) != "" {
		note = strings.TrimSpace(spokenNote + " " + note)
	}
	if input == "" {
		s.mu.Unlock()
		// A concurrent turn already carried these words.
		return "", provider.ErrSuperseded
	}
	c, err := l.admit(ctx, s, input, note)
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
		c, err = l.admit(ctx, s, input, note)
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

// Release forgets the sessions whose key starts with prefix. A key's cursor only
// means anything alongside the history it counted, and that went with the room.
func (l *LLM) Release(prefix string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for key := range l.sessions {
		if strings.HasPrefix(key, prefix) {
			delete(l.sessions, key)
		}
	}
}

// pending returns the next input, a note for the model, and the cursor to store
// once it is admitted.
// A cursor rather than "the trailing user messages" because an interruption
// before anything was spoken appends no assistant message, and a second message
// arrives before the first has a reply: both would resend the first message.
func (s *session) pending(msgs []provider.Message, interrupts bool) (string, string, int) {
	if s.sent >= len(msgs) {
		return "", "", s.sent
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
		return "", "", len(msgs)
	}
	if heard == "" && interrupts && s.active.open() {
		// The turn being cut off is still running, so its partial reply is
		// not in the history yet.
		heard = s.active.text()
	}
	input := strings.Join(users, "\n")
	note := ""
	if heard != "" {
		// The model never sees an aborted answer, and a live reply generated
		// further than the caller heard: tell it what actually arrived. "At
		// most" because the text is whole chunks, some cut off mid-playback.
		note = "Your previous reply was cut off. The caller heard at most: \"" + heard + "\". Do not mention this."
	}
	return input, note, len(msgs)
}

// admit makes sure the session exists and submits input. Callers hold s.mu.
//
// Admission ignores ctx: agent-pi admits a request even if the client leaves,
// so a barge-in during the POST would leave it unknown whether the input went
// in, and the next turn would send it twice. SSE headers are sent only after
// admission, so a response means the input is durably in.
func (l *LLM) admit(ctx context.Context, s *session, input, note string) (*call, error) {
	if s.id == "" {
		if err := l.openSession(ctx, s); err != nil {
			return nil, err
		}
	}
	resp, err := l.submit(ctx, s, input, note)
	var refused *refusedError
	// A named conversation is the only one this turn may land in. If it has
	// gone, or isn't the caller's, a new one would hide that.
	if errors.As(err, &refused) && refused.status == http.StatusNotFound && provider.ConversationFrom(ctx) == "" {
		s.id = ""
		if err := l.openSession(ctx, s); err != nil {
			return nil, err
		}
		resp, err = l.submit(ctx, s, input, note)
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
// because agent-pi caps sessions per user: creating one on every start would
// exhaust the limit after a few restarts.
func (l *LLM) openSession(ctx context.Context, s *session) error {
	if id := provider.ConversationFrom(ctx); id != "" {
		s.id = id
		return nil
	}
	key := provider.SessionNameFrom(ctx)
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

// urlFor picks the replica a session lives on. A session ID is
// <agent>.<replica>.<n>, and a session stays on its replica for life, so
// everything before the last dot names it. A new session goes to URL.
func (l *LLM) urlFor(sessionID string) string {
	replicas := l.opts.Replicas
	if l.routes != nil {
		replicas = l.routes.current()
	}
	if at := strings.LastIndex(sessionID, "."); at > 0 {
		if url, ok := replicas[sessionID[:at]]; ok {
			return url
		}
	}
	return l.opts.URL
}

// parseReplicas reads AGENT_URLS: name=url pairs, as in workbench, where a name
// is a session ID's prefix and a url is the replica's base.
func parseReplicas(raw string) (map[string]string, error) {
	replicas := map[string]string{}
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		name, base, ok := strings.Cut(pair, "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("agentpi: AGENT_URLS: %q isn't name=url", pair)
		}
		target, err := url.JoinPath(base, "agent")
		if err != nil || !strings.Contains(base, "://") {
			return nil, fmt.Errorf("agentpi: AGENT_URLS: %q has no valid url", pair)
		}
		replicas[name] = target
	}
	return replicas, nil
}

// submit POSTs input and returns once it is admitted. It retries a failed
// connection once with the same requestId: durable deduplicates on it, and a
// retried interrupt does not abort a second time.
func (l *LLM) submit(ctx context.Context, s *session, input, note string) (*http.Response, error) {
	params := map[string]any{"sessionId": s.id, "input": input, "requestId": newRequestID()}
	if note != "" {
		params["note"] = note
	}
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
		req, err = http.NewRequestWithContext(context.WithoutCancel(ctx), http.MethodPost, l.urlFor(s.id), bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("agentpi: build %s: %w", method, err)
		}
		if err := l.setHeaders(context.WithoutCancel(ctx), req, s.id, "application/json, text/event-stream"); err != nil {
			return nil, err
		}
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
	if err := l.setHeaders(ctx, req, "", "application/json"); err != nil {
		return err
	}
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

func (l *LLM) setHeaders(ctx context.Context, req *http.Request, sessionID, accept string) error {
	token, err := l.opts.Tokens.Token(ctx, provider.GrantFrom(ctx))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", accept)
	req.Header.Set("Authorization", "Bearer "+token)
	if sessionID != "" {
		req.Header.Set(sessionHeader, sessionID)
	}
	return nil
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
