package agentpi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"t10-livekit-agent/internal/provider"
)

var (
	// errStuck: the run ahead of this input failed and left it in the inbox.
	errStuck = errors.New("agentpi: input stuck behind a failed run")
	// errWithdrawn: this input was aborted before it was ever placed, so
	// unlike an interrupted reply, the model has not seen its words.
	errWithdrawn = errors.New("agentpi: input withdrawn before it was placed")
	errIdle      = errors.New("agentpi: stream went silent")
)

// call is one submission's stream. Every stream receives every session event,
// so a call decides for itself what is its own: it owns the output from the
// moment its input is placed until a newer input is placed or its result
// arrives.
type call struct {
	body       io.ReadCloser
	mine       int64 // 0 when a plugin answered: there is no submission to follow
	admittedAt time.Time

	mu        sync.Mutex
	delivered strings.Builder
	closed    bool
}

// event is the part of a session/event notification this client reads.
type event struct {
	Type         string `json:"type"`
	Text         string `json:"text"`
	SubmissionID int64  `json:"submissionId"`
	Status       string `json:"status"`
	Inbox        []struct {
		ID int64 `json:"id"`
	} `json:"inbox"`
}

// result is the JSON-RPC result that ends a stream.
type result struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
	Detail string `json:"detail"`
	Text   string `json:"text"`
}

type message struct {
	Params *event  `json:"params"`
	Result *result `json:"result"`
	Error  *struct {
		Message string `json:"message"`
	} `json:"error"`
}

func (c *call) open() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.closed
}

func (c *call) text() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.delivered.String()
}

func (c *call) deliver(delta string, onToken func(string)) {
	c.mu.Lock()
	c.delivered.WriteString(delta)
	c.mu.Unlock()
	if onToken != nil {
		onToken(delta)
	}
}

// follow reads the stream to its end and returns the concatenated deltas, not
// the result's text: that holds only the final message, so it would omit
// "Let me check." and disagree with what was spoken.
func (c *call) follow(ctx context.Context, opts Options, onToken func(string)) (string, error) {
	var cause atomic.Pointer[error]
	abandon := func(err error) {
		cause.CompareAndSwap(nil, &err)
		c.body.Close()
	}
	defer func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.body.Close()
	}()
	stop := context.AfterFunc(ctx, func() { abandon(ctx.Err()) })
	defer stop()
	idle := time.AfterFunc(opts.IdleTimeout, func() { abandon(errIdle) })
	defer idle.Stop()
	var stuck *time.Timer
	defer func() {
		if stuck != nil {
			stuck.Stop()
		}
	}()

	owner, settled := false, false
	place := func() {
		if owner {
			return
		}
		owner = true
		if stuck != nil {
			stuck.Stop()
		}
		wait := time.Since(c.admittedAt)
		setSpanAttributes(ctx,
			attribute.Int64("agentpi.submission_id", c.mine),
			attribute.Bool("agentpi.queued", wait > 50*time.Millisecond),
			attribute.Int64("agentpi.wait_ms", wait.Milliseconds()))
	}
	finish := func(err error) (string, error) { return c.text(), err }

	reader := bufio.NewReader(c.body)
	var data strings.Builder
	for {
		line, err := reader.ReadString('\n')
		idle.Reset(opts.IdleTimeout)
		if err != nil {
			if cause := cause.Load(); cause != nil {
				return finish(*cause)
			}
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return finish(fmt.Errorf("agentpi: stream read: %w", err))
		}
		line = strings.TrimRight(line, "\r\n")
		if rest, ok := strings.CutPrefix(line, "data:"); ok {
			data.WriteString(strings.TrimPrefix(rest, " "))
			continue
		}
		if line != "" || data.Len() == 0 {
			continue
		}

		var m message
		jerr := json.Unmarshal([]byte(data.String()), &m)
		data.Reset()
		if jerr != nil {
			continue
		}
		if m.Error != nil {
			return finish(fmt.Errorf("agentpi: %s", m.Error.Message))
		}
		if m.Result != nil {
			return finish(c.settle(*m.Result, owner, onToken))
		}
		if m.Params == nil || c.mine == 0 {
			continue
		}

		switch e := m.Params; e.Type {
		case "text.delta":
			if owner && !settled {
				c.deliver(e.Text, onToken)
			}
		case "submission":
			switch {
			case e.SubmissionID == c.mine && e.Status == "placed":
				place()
			case e.SubmissionID == c.mine && (e.Status == "done" || e.Status == "unanswered"):
				settled = true
			case e.SubmissionID > c.mine && e.Status == "placed" && owner && !settled:
				return finish(provider.ErrSuperseded)
			case e.SubmissionID < c.mine && e.Status == "unanswered" && !owner && stuck == nil && opts.WhenBusy != WhenBusyInterrupt:
				stuck = time.AfterFunc(opts.StuckGrace, func() { abandon(errStuck) })
			}
		case "snapshot":
			// A slow stream can be resynced past the event that placed us.
			queued := false
			for _, item := range e.Inbox {
				queued = queued || item.ID == c.mine
			}
			if !queued {
				place()
			}
		}
	}
}

// settle turns the final result into an error, delivering the text of a reply
// that had no deltas of its own: a plugin's answer, or one a resynced stream
// skipped.
func (c *call) settle(r result, owner bool, onToken func(string)) error {
	switch {
	case r.Status != "unanswered":
		if c.text() == "" && r.Text != "" {
			c.deliver(r.Text, onToken)
		}
		return nil
	case r.Reason != "aborted":
		return fmt.Errorf("agentpi: unanswered: %s: %s", r.Reason, r.Detail)
	case owner:
		// A newer message interrupted this reply.
		return provider.ErrSuperseded
	default:
		return errWithdrawn
	}
}
