package engine

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"t10-livekit-agent/internal/telemetry"
)

// RunChatTurn appends userText to identity's conversation, streams the LLM
// reply through sink token-by-token, appends the reply to history, and
// returns the full reply text. It's transport-agnostic: text mode, voice
// mode, and anything else that has produced a piece of user text all call
// this the same way, differing only in what ReplySink they pass in.
func RunChatTurn(ctx context.Context, deps *Deps, identity, userText string, sink ReplySink) (string, error) {
	defer sink.Close()
	convo := deps.Conversations.Get(identity)
	release, err := convo.ReserveTurn()(ctx)
	if err != nil {
		return "", err
	}
	defer release()
	return runChatTurn(ctx, deps, convo, userText, sink)
}

func runChatTurn(ctx context.Context, deps *Deps, convo *Conversation, userText string, sink ReplySink) (string, error) {
	convo.Append("user", userText)

	llmCtx, end := telemetry.StartStage(ctx, "llm.chat_stream", attribute.Int("user_text.chars", len(userText)))
	var streamed strings.Builder
	full, err := deps.LLM.ChatStream(llmCtx, convo.Snapshot(), func(delta string) {
		streamed.WriteString(delta)
		sink.WriteToken(delta)
	})
	if err == nil {
		err = ctx.Err()
	}
	if err == nil && strings.TrimSpace(full) == "" {
		err = fmt.Errorf("model returned an empty reply")
	}
	end(err)
	if err != nil {
		if streamed.Len() > 0 {
			convo.Append("assistant", streamed.String()+" [incomplete]")
		}
		return "", err
	}
	convo.Append("assistant", full)
	return full, nil
}
