package livekit

import (
	"context"
	"encoding/json"
	"errors"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"io"
	"log/slog"
	"t10-livekit-agent/internal/roomprotocol"
	"time"
)

type messageStatus struct {
	ctx context.Context
	id  string
	w   *lksdk.TextStreamWriter
}

func requestID(attrs map[string]string, fallback string) string {
	if id := attrs[roomprotocol.AttrRequestID]; id != "" {
		return id
	}
	return fallback
}
func newMessageStatus(ctx context.Context, room *lksdk.Room, identity, id string) *messageStatus {
	return &messageStatus{ctx: ctx, id: id, w: room.LocalParticipant.StreamText(lksdk.StreamTextOptions{
		Topic: roomprotocol.TopicMessageStatus, DestinationIdentities: []string{identity},
		Attributes: map[string]string{roomprotocol.AttrRequestID: id},
	})}
}
func (s *messageStatus) send(state, message string, chunks int) {
	data, _ := json.Marshal(roomprotocol.MessageStatus{State: state, Message: message, AudioChunks: chunks})
	// Terminal errors must still be sent after the provider deadline expires.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(s.ctx), 5*time.Second)
	defer cancel()
	_ = writeMessage(ctx, s.w.Write, string(data)+"\n")
}
func (s *messageStatus) finish(err error, chunks int) {
	if err != nil {
		slog.ErrorContext(s.ctx, "message failed", "request_id", s.id, "err", err)
		message := "Could not complete your reply. Please try again."
		if errors.Is(err, context.DeadlineExceeded) {
			message = "Your reply timed out. Please try again."
		}
		s.send("error", message, chunks)
	} else {
		s.send("done", "", chunks)
	}
}

// The SDK reader has no context or public Close. Bound our wait even if a
// sender never supplies a trailer; its reader is reclaimed on disconnection.
func readMessage(ctx context.Context, reader io.Reader) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() { data, err := io.ReadAll(reader); done <- result{data, err} }()
	select {
	case r := <-done:
		return r.data, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
