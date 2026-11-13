package agentpi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

// greetResult is session.greet's answer: the text said, or why nothing was.
type greetResult struct {
	Text     string `json:"text"`
	By       string `json:"by"`
	Skipped  string `json:"skipped"`
	Prepared bool   `json:"prepared"`
}

// Greet asks the agent for the call's greeting with session.greet, on the call's
// session and under the caller's grant, as a turn would. The agent writes it into
// the conversation; its words stream to onToken as they come. A harness without
// the method answers provider.ErrNoGreeting. See docs/GREETING.md.
func (l *LLM) Greet(ctx context.Context, req provider.GreetRequest, onToken func(string)) (string, error) {
	key := provider.SessionFrom(ctx)
	if key == "" {
		return "", errors.New("agentpi: no session on context")
	}
	s := l.session(key)
	s.mu.Lock()
	if s.id == "" {
		if err := l.openSession(ctx, s); err != nil {
			s.mu.Unlock()
			return "", err
		}
	}
	id := s.id
	s.mu.Unlock()

	params := map[string]any{"sessionId": id, "channel": "call", "requestId": newRequestID()}
	if req.Locale != "" {
		params["locale"] = req.Locale
	}
	if req.TimeZone != "" {
		params["timeZone"] = req.TimeZone
	}
	if len(req.Client) > 0 {
		params["client"] = req.Client
	}
	if req.Prepare {
		params["prepare"] = true
	}
	if turn := telemetry.TurnID(ctx); turn != "" {
		params["turnId"] = turn
	}
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "session.greet", "params": params})
	if err != nil {
		return "", fmt.Errorf("agentpi: marshal session.greet: %w", err)
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, l.urlFor(id), bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("agentpi: build session.greet: %w", err)
	}
	accept := "application/json, text/event-stream"
	if req.Prepare {
		accept = "application/json"
	}
	if err := l.setHeaders(ctx, httpReq, id, accept); err != nil {
		return "", err
	}
	setTraceparent(ctx, httpReq)
	resp, err := l.client.Do(httpReq)
	if err != nil {
		return "", fmt.Errorf("agentpi: session.greet: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		refused := refusal("session.greet", resp).(*refusedError)
		// agent-pi, or an agent-ti from before greetings, has no such method.
		if refused.status == http.StatusBadRequest && strings.Contains(refused.message, "not implemented") ||
			refused.status == http.StatusNotFound && strings.Contains(refused.message, "method") {
			return "", provider.ErrNoGreeting
		}
		return "", refused
	}
	var result greetResult
	if strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
		result, err = readGreeting(resp.Body, onToken)
	} else {
		var message struct {
			Result greetResult `json:"result"`
		}
		err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&message)
		result = message.Result
		if err == nil && result.Text != "" && onToken != nil && !req.Prepare {
			onToken(result.Text)
		}
	}
	if err != nil {
		return "", err
	}
	setSpanAttributes(ctx, attribute.String("agentpi.session_id", id),
		attribute.String("greeting.by", result.By), attribute.String("greeting.skipped", result.Skipped))
	return result.Text, nil
}

// readGreeting follows a streamed session.greet: greeting.delta events, then the
// JSON-RPC response.
func readGreeting(body io.Reader, onToken func(string)) (greetResult, error) {
	reader := bufio.NewReader(body)
	var data strings.Builder
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return greetResult{}, fmt.Errorf("agentpi: greeting stream: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if rest, ok := strings.CutPrefix(line, "data:"); ok {
			data.WriteString(strings.TrimPrefix(rest, " "))
			continue
		}
		if line != "" || data.Len() == 0 {
			continue
		}
		var m struct {
			Type   string       `json:"type"`
			Text   string       `json:"text"`
			Result *greetResult `json:"result"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		jerr := json.Unmarshal([]byte(data.String()), &m)
		data.Reset()
		switch {
		case jerr != nil:
		case m.Error != nil:
			return greetResult{}, fmt.Errorf("agentpi: session.greet: %s", m.Error.Message)
		case m.Result != nil:
			return *m.Result, nil
		case m.Type == "greeting.delta" && m.Text != "" && onToken != nil:
			onToken(m.Text)
		}
	}
}
