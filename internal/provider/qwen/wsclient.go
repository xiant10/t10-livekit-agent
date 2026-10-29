// Package qwen implements provider.STTProvider and provider.TTSProvider
// against DashScope's Qwen realtime WebSocket APIs. There is no official Go
// SDK, so this talks the wire protocol directly (reverse-engineered from the
// Python `dashscope` SDK): a single WebSocket endpoint
// (wss://dashscope.aliyuncs.com/api-ws/v1/realtime?model=<model>) carrying
// OpenAI-Realtime-shaped JSON events, with DashScope-specific event types
// for transcription and speech synthesis.
package qwen

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// realtimeEvent is a loosely-typed DashScope realtime protocol message.
type realtimeEvent map[string]any

func (e realtimeEvent) typ() string {
	t, _ := e["type"].(string)
	return t
}

func (e realtimeEvent) str(key string) string {
	v, _ := e[key].(string)
	return v
}

func genEventID() string {
	return fmt.Sprintf("event_%x", rand.Uint64())
}

// realtimeClient owns one DashScope realtime WebSocket connection and fans
// incoming JSON events out on a channel.
type realtimeClient struct {
	conn   *websocket.Conn
	events chan realtimeEvent
}

func dialRealtime(ctx context.Context, baseURL, model, apiKey string) (*realtimeClient, error) {
	u, _, _ := strings.Cut(baseURL, "?")
	u = u + "?model=" + model

	header := http.Header{}
	header.Set("Authorization", "Bearer "+apiKey)
	header.Set("User-Agent", "t10-livekit-agent-go/0.1")

	conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, fmt.Errorf("qwen: dial %s: %w", u, err)
	}
	conn.SetReadLimit(32 << 20) // realtime audio events can be large base64 blobs

	c := &realtimeClient{conn: conn, events: make(chan realtimeEvent, 256)}
	go c.readLoop()
	return c, nil
}

func (c *realtimeClient) readLoop() {
	defer close(c.events)
	for {
		_, data, err := c.conn.Read(context.Background())
		if err != nil {
			return
		}
		var ev realtimeEvent
		if json.Unmarshal(data, &ev) == nil {
			c.events <- ev
		}
	}
}

func (c *realtimeClient) send(ctx context.Context, v map[string]any) error {
	if v["event_id"] == nil {
		v["event_id"] = genEventID()
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.conn.Write(ctx, websocket.MessageText, data)
}

func (c *realtimeClient) close() {
	_ = c.conn.Close(websocket.StatusNormalClosure, "")
}

// waitForType blocks until an event of type wantType arrives (returning it)
// or an "error" event / timeout occurs.
func waitForType(ctx context.Context, c *realtimeClient, wantType string, timeout time.Duration) (realtimeEvent, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		select {
		case ev, ok := <-c.events:
			if !ok {
				return nil, fmt.Errorf("qwen: connection closed waiting for %q", wantType)
			}
			switch ev.typ() {
			case wantType:
				return ev, nil
			case "error":
				return nil, fmt.Errorf("qwen: provider error while waiting for %q: %v", wantType, ev)
			}
		case <-deadline.C:
			return nil, fmt.Errorf("qwen: timed out after %s waiting for %q", timeout, wantType)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
