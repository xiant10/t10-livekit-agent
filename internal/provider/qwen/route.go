package qwen

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"

	"t10-livekit-agent/internal/authclient"
	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
)

// route is where a speech socket goes: DashScope with this worker's key, or,
// with SPEECH_URL, ai-gateway-llm's realtime proxy, which chooses the key by
// the call's tenant from the run token each socket carries.
type route struct {
	wsURL  string
	apiKey string
	// use is stt or tts, which the gateway chooses a policy by.
	use string
	// tokens mints the call's run token from its grant; nil sends none.
	tokens interface {
		Token(ctx context.Context, grantID string) (string, error)
	}
}

func newRoute(cfg config.Config, use string) (route, error) {
	if cfg.SpeechURL == "" {
		if cfg.DashScopeAPIKey == "" {
			return route{}, fmt.Errorf("qwen: DASHSCOPE_API_KEY is required")
		}
		return route{wsURL: cfg.DashScopeWSURL, apiKey: cfg.DashScopeAPIKey, use: use}, nil
	}
	if cfg.SpeechAPIKey == "" {
		return route{}, fmt.Errorf("qwen: SPEECH_API_KEY is required with SPEECH_URL")
	}
	r := route{wsURL: cfg.SpeechURL, apiKey: cfg.SpeechAPIKey, use: use}
	if cfg.AuthClientSecret != "" {
		tokens, err := authclient.New(authclient.Options{URL: cfg.AuthURL, ClientID: cfg.AuthClientID, ClientSecret: cfg.AuthClientSecret})
		if err != nil {
			return route{}, err
		}
		r.tokens = tokens
	}
	return r, nil
}

// dial opens a socket for model, as the call in ctx.
func (r route) dial(ctx context.Context, model string) (*realtimeClient, error) {
	base := r.wsURL
	header := http.Header{}
	if r.tokens != nil {
		u, err := url.Parse(base)
		if err != nil {
			return nil, err
		}
		q := u.Query()
		q.Set("use", r.use)
		u.RawQuery = q.Encode()
		base = u.String()
		token, err := r.tokens.Token(ctx, provider.GrantFrom(ctx))
		switch {
		case err == nil:
			header.Set("X-Run-Token", token)
		case errors.Is(err, authclient.ErrNoGrant):
			// Nobody's call: the gateway uses the platform's key.
		default:
			// Without the token the gateway can't tell whose key to use, and
			// a platform key must not stand in for a tenant that chose its own.
			slog.WarnContext(ctx, "speech: no run token for the call", "error", err)
			return nil, fmt.Errorf("qwen: run token: %w", err)
		}
	}
	return dialRealtime(ctx, base, model, r.apiKey, header)
}
