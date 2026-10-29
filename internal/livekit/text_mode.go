package livekit

import (
	"context"
	"fmt"
	"strings"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/roomprotocol"
	"t10-livekit-agent/internal/telemetry"
)

// textReplySink adapts a LiveKit text-stream writer to engine.ReplySink.
type textReplySink struct {
	w *lksdk.TextStreamWriter
}

func (s textReplySink) WriteToken(delta string) { s.w.Write(delta, nil) }
func (s textReplySink) Close()                  { s.w.Close() }

// Message writes await the SDK callback before closing: Close drops queued writes.
func writeMessage[T any](ctx context.Context, write func(T, *func()), data T) error {
	done := make(chan struct{})
	callback := func() { close(done) }
	write(data, &callback)
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type messageReplySink struct {
	ctx context.Context
	w   *lksdk.TextStreamWriter
}

func (s *messageReplySink) WriteToken(text string) { _ = writeMessage(s.ctx, s.w.Write, text) }
func (s *messageReplySink) Close()                 { s.w.Close() }

func newChatReplySink(ctx context.Context, room *lksdk.Room, identity, requestID string) engine.ReplySink {
	return &messageReplySink{ctx: ctx, w: room.LocalParticipant.StreamText(lksdk.StreamTextOptions{
		Topic: roomprotocol.TopicChat, DestinationIdentities: []string{identity},
		Attributes: map[string]string{roomprotocol.AttrRequestID: requestID},
	})}
}

// The shared admission queue is reserved before reading uploads or starting
// workers, so a slow voice upload cannot be overtaken by a later text message.
func newTextStreamHandler(room *lksdk.Room, deps *engine.Deps, queue *engine.Manager) lksdk.TextStreamHandler {
	return func(reader *lksdk.TextStreamReader, identity string) {
		enter := queue.Get(identity).ReserveTurn()
		ctx, end := telemetry.StartSession(context.Background())
		ctx, cancel := context.WithTimeout(ctx, 50*time.Second)
		status := newMessageStatus(ctx, room, identity, requestID(reader.Info.Attributes, reader.Info.Id))
		status.send("queued", "", 0)
		go func() {
			defer end()
			defer cancel()
			defer status.w.Close()
			release, err := enter(ctx)
			if err == nil {
				defer release()
				status.send("thinking", "", 0)
				data, readErr := readMessage(ctx, reader)
				text := string(data)
				if readErr != nil {
					err = readErr
				} else if strings.TrimSpace(text) == "" {
					err = fmt.Errorf("empty text message")
				} else {
					_, err = engine.RunChatTurn(ctx, deps, identity, text, newChatReplySink(ctx, room, identity, status.id))
				}
			}
			status.finish(err, 0)
		}()
	}
}
