package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"sync"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/roomprotocol"
)

// Streams on different topics can finish in either order. Terminal status
// alone is insufficient: wait until all advertised reply data is received.
type messageResult struct {
	mu                 sync.Mutex
	done               chan struct{}
	once               sync.Once
	terminal, text     bool
	expected, received int
}

func (m *messageResult) check() {
	if m.terminal && m.text && m.received == m.expected {
		m.once.Do(func() { close(m.done) })
	}
}
func (m *messageResult) textDone()  { m.mu.Lock(); defer m.mu.Unlock(); m.text = true; m.check() }
func (m *messageResult) audioDone() { m.mu.Lock(); defer m.mu.Unlock(); m.received++; m.check() }
func (m *messageResult) status(reader *lksdk.TextStreamReader, _ string) {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var status roomprotocol.MessageStatus
		if json.Unmarshal(scanner.Bytes(), &status) != nil {
			continue
		}
		m.mu.Lock()
		switch status.State {
		case "error":
			fmt.Println("agent error:", status.Message)
			m.once.Do(func() { close(m.done) })
		case "done":
			m.terminal = true
			m.expected = status.AudioChunks
			m.check()
		default:
			fmt.Println("agent:", status.State)
		}
		m.mu.Unlock()
	}
}
