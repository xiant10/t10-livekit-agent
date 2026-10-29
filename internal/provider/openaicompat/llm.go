// Package openaicompat implements provider.LLMProvider against any
// OpenAI-compatible chat-completions endpoint (OpenAI itself, DashScope's
// compatible-mode endpoint for Qwen, a local gateway, etc.). Switching
// models/providers is a config change (base URL, key, model), never a code
// change — that's the extensibility point requested for the LLM stage.
package openaicompat

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
)

func init() {
	provider.RegisterLLM("openai_compat", func(cfg config.Config) (provider.LLMProvider, error) {
		return New(cfg.OpenAIBaseURL, cfg.OpenAIAPIKey, cfg.OpenAIModel), nil
	})
}

// LLM is a minimal OpenAI-compatible streaming chat client.
type LLM struct {
	baseURL string
	apiKey  string
	model   string
	client  *http.Client
}

func New(baseURL, apiKey, model string) *LLM {
	return &LLM{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		model:   model,
		client:  &http.Client{Timeout: 120 * time.Second},
	}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model    string        `json:"model"`
	Messages []chatMessage `json:"messages"`
	Stream   bool          `json:"stream"`
}

type chatStreamChunk struct {
	Choices []struct {
		Delta struct {
			Content string `json:"content"`
		} `json:"delta"`
		FinishReason *string `json:"finish_reason"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (l *LLM) ChatStream(ctx context.Context, msgs []provider.Message, onToken func(delta string)) (string, error) {
	reqMsgs := make([]chatMessage, len(msgs))
	for i, m := range msgs {
		reqMsgs[i] = chatMessage{Role: m.Role, Content: m.Content}
	}
	body, err := json.Marshal(chatRequest{Model: l.model, Messages: reqMsgs, Stream: true})
	if err != nil {
		return "", fmt.Errorf("openaicompat: marshal request: %w", err)
	}

	url := l.baseURL + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("openaicompat: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+l.apiKey)
	req.Header.Set("Accept", "text/event-stream")

	resp, err := l.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("openaicompat: request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		payload, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return "", fmt.Errorf("openaicompat: unexpected status %d: %s", resp.StatusCode, string(payload))
	}

	var full strings.Builder
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			line = strings.TrimSpace(line)
			if data, ok := strings.CutPrefix(line, "data:"); ok {
				data = strings.TrimSpace(data)
				if data == "" {
					// keep-alive / blank event
				} else if data == "[DONE]" {
					return full.String(), nil
				} else {
					var chunk chatStreamChunk
					if jerr := json.Unmarshal([]byte(data), &chunk); jerr != nil {
						continue
					}
					if chunk.Error != nil {
						return full.String(), fmt.Errorf("openaicompat: provider error: %s", chunk.Error.Message)
					}
					for _, c := range chunk.Choices {
						if c.Delta.Content == "" {
							continue
						}
						full.WriteString(c.Delta.Content)
						if onToken != nil {
							onToken(c.Delta.Content)
						}
					}
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				return full.String(), nil
			}
			return full.String(), fmt.Errorf("openaicompat: stream read: %w", err)
		}
	}
}
