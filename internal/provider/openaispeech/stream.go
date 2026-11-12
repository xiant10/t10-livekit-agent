package openaispeech

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/coder/websocket"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/provider"
)

// The Realtime transcription socket takes 24kHz PCM16, not the 16kHz the
// pipeline carries.
const realtimeSampleRate = 24000

// The endpoint transcribes a segment only once it is committed, and its own
// VAD commits only at a pause, so a sentence spoken in one breath would show
// nothing until it ended. The stream therefore commits itself: after
// minChunk of audio at the next quiet moment, and at maxChunk regardless.
const (
	minChunk   = 16000 * 2 * 3 / 2 // 1.5s of 16kHz PCM16
	maxChunk   = 16000 * 2 * 4     // 4s
	quietRMS   = 400.0
	quietBytes = 16000 * 2 / 50 // the last 20ms decide whether it is quiet
)

// resultTimeout bounds how long Commit waits for the final transcripts once
// the last audio is in.
const resultTimeout = 10 * time.Second

// item is one committed audio segment, transcribed on its own.
type item struct {
	text string
	done bool
}

// rtStream streams audio to OpenAI's Realtime transcription socket while the
// user is still talking, reporting running snapshots through onInterim. The
// audio is also kept locally: if the socket cannot be opened or fails, Commit
// falls back to one batch request, so streaming is only ever an improvement.
type rtStream struct {
	stt       *STT
	lang      string
	onInterim func(snapshot string)

	mu      sync.Mutex
	pcm     []byte // everything pushed, kept for the batch fallback
	sent    int    // bytes of pcm already handed to the sender
	aborted bool
	done    bool

	notify chan struct{} // new audio to send
	stop   chan struct{} // closed by Commit/Abort: no more audio is coming
	ready  chan struct{} // closed once the dial has finished
	conn   *websocket.Conn
	dialEr error

	senderDone  chan struct{}
	uncommitted int // bytes appended since the last commit; the sender owns it

	// Transcript state, written by the event reader.
	em      sync.Mutex
	order   []string
	items   map[string]*item
	commits int // commits sent
	replies int // commits answered
	failed  error
	last    string
	changed chan struct{}
}

func (s *rtStream) dial(ctx context.Context) {
	defer close(s.ready)
	conn, err := s.stt.dialRealtime(ctx, s.lang)
	if err != nil {
		s.dialEr = err
		close(s.senderDone)
		return
	}
	s.conn = conn
	go s.runSender(ctx)
	go s.runReader()
}

func (p *STT) dialRealtime(ctx context.Context, lang string) (*websocket.Conn, error) {
	u, err := url.Parse(p.client.baseURL + "/realtime")
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	case "http":
		u.Scheme = "ws"
	}
	u.RawQuery = url.Values{"intent": {"transcription"}}.Encode()

	h := http.Header{}
	h.Set("Authorization", "Bearer "+p.client.apiKey)
	dctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, u.String(), &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		return nil, err
	}
	conn.SetReadLimit(4 << 20)

	transcription := map[string]any{"model": p.model}
	if lang != "" {
		transcription["language"] = lang
	}
	update, _ := json.Marshal(map[string]any{
		"type": "session.update",
		"session": map[string]any{
			"type": "transcription",
			"audio": map[string]any{"input": map[string]any{
				"format":         map[string]any{"type": "audio/pcm", "rate": realtimeSampleRate},
				"transcription":  transcription,
				"turn_detection": nil, // the stream commits itself
			}},
		},
	})
	if err := conn.Write(dctx, websocket.MessageText, update); err != nil {
		conn.CloseNow()
		return nil, err
	}
	return conn, nil
}

func (s *rtStream) PushPCM(pcm []byte) {
	s.mu.Lock()
	if s.aborted || s.done {
		s.mu.Unlock()
		return
	}
	s.pcm = append(s.pcm, pcm...)
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// runSender forwards pushed audio, including whatever piled up while the
// socket was still opening, until Commit or Abort says no more is coming.
func (s *rtStream) runSender(ctx context.Context) {
	defer close(s.senderDone)
	rs := audio.NewStreamResampler(sttSampleRate, realtimeSampleRate)
	flush := func() {
		s.mu.Lock()
		end := len(s.pcm) &^ 1
		if end <= s.sent { // nothing new, or Abort dropped the audio
			s.mu.Unlock()
			return
		}
		chunk := s.pcm[s.sent:end]
		s.sent = end
		s.mu.Unlock()
		out := rs.Push(audio.BytesToInt16(chunk))
		msg, _ := json.Marshal(map[string]any{
			"type":  "input_audio_buffer.append",
			"audio": base64.StdEncoding.EncodeToString(audio.Int16ToBytes(out)),
		})
		if err := s.conn.Write(ctx, websocket.MessageText, msg); err != nil {
			s.fail(err)
			return
		}
		s.uncommitted += len(chunk)
		if s.uncommitted >= maxChunk || (s.uncommitted >= minChunk && quiet(chunk)) {
			s.commit(ctx)
		}
	}
	for {
		select {
		case <-s.notify:
			flush()
		case <-s.stop:
			flush()
			return
		}
	}
}

// quiet reports whether the end of chunk is silence, a safe place to cut.
func quiet(chunk []byte) bool {
	tail := chunk[max(0, len(chunk)-quietBytes):]
	samples := audio.BytesToInt16(tail[:len(tail)&^1])
	var sum float64
	for _, v := range samples {
		sum += float64(v) * float64(v)
	}
	return len(samples) > 0 && math.Sqrt(sum/float64(len(samples))) < quietRMS
}

// commit closes the segment being sent so the server starts transcribing it.
func (s *rtStream) commit(ctx context.Context) {
	s.em.Lock()
	s.commits++
	s.em.Unlock()
	s.uncommitted = 0
	if err := s.conn.Write(ctx, websocket.MessageText, []byte(`{"type":"input_audio_buffer.commit"}`)); err != nil {
		s.fail(err)
	}
}

// runReader folds server events into the transcript state.
func (s *rtStream) runReader() {
	for {
		_, data, err := s.conn.Read(context.Background())
		if err != nil {
			s.fail(err)
			return
		}
		var ev struct {
			Type       string `json:"type"`
			ItemID     string `json:"item_id"`
			Delta      string `json:"delta"`
			Transcript string `json:"transcript"`
			Error      struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(data, &ev) != nil {
			continue
		}
		s.em.Lock()
		switch ev.Type {
		case "input_audio_buffer.committed":
			s.item(ev.ItemID)
			s.replies++
		case "conversation.item.input_audio_transcription.delta":
			it := s.item(ev.ItemID)
			it.text += ev.Delta
		case "conversation.item.input_audio_transcription.completed":
			it := s.item(ev.ItemID)
			it.text, it.done = ev.Transcript, true
		case "conversation.item.input_audio_transcription.failed":
			s.item(ev.ItemID).done = true
		case "error":
			if ev.Error.Code == "input_audio_buffer_commit_empty" {
				s.replies++
			} else if s.failed == nil {
				s.failed = fmt.Errorf("provider error: %s", ev.Error.Message)
			}
		}
		snap := s.snapshot()
		var interim func(string)
		if snap != s.last && snap != "" {
			s.last, interim = snap, s.onInterim
		}
		s.em.Unlock()
		if interim != nil {
			interim(snap)
		}
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
}

// item returns the segment with id, adding it in arrival order. Caller holds em.
func (s *rtStream) item(id string) *item {
	it, ok := s.items[id]
	if !ok {
		it = &item{}
		s.items[id] = it
		s.order = append(s.order, id)
	}
	return it
}

// snapshot is the transcript of every segment so far. Caller holds em.
func (s *rtStream) snapshot() string {
	var b strings.Builder
	for _, id := range s.order {
		t := strings.TrimSpace(s.items[id].text)
		if t == "" {
			continue
		}
		if b.Len() > 0 && !cjkJoin(b.String(), t) {
			b.WriteByte(' ')
		}
		b.WriteString(t)
	}
	return b.String()
}

// cjkJoin reports whether two pieces meet at a CJK character, where no space
// belongs between them.
func cjkJoin(left, right string) bool {
	l := []rune(left)
	r := []rune(right)
	return unicode.Is(unicode.Han, l[len(l)-1]) || unicode.Is(unicode.Han, r[0])
}

func (s *rtStream) fail(err error) {
	s.em.Lock()
	if s.failed == nil {
		s.failed = err
	}
	s.em.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *rtStream) Commit(ctx context.Context) (string, error) {
	s.mu.Lock()
	if s.aborted || s.done {
		aborted := s.aborted
		s.mu.Unlock()
		if aborted {
			return "", context.Canceled
		}
		return "", fmt.Errorf("openaispeech stt: stream already finished")
	}
	s.done = true
	s.mu.Unlock()
	close(s.stop)

	select {
	case <-s.ready:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if s.dialEr == nil {
		defer s.conn.CloseNow()
		<-s.senderDone
		if text, err := s.finish(ctx); err == nil {
			return text, nil
		}
	}
	// No socket, or it failed part way: transcribe what was heard in one go.
	s.mu.Lock()
	pcm := s.pcm
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return s.stt.TranscribeBatch(ctx, pcm, s.lang)
}

// finish commits the audio still in the server's buffer and waits for every
// segment's final transcript.
func (s *rtStream) finish(ctx context.Context) (string, error) {
	if s.uncommitted > 0 {
		s.commit(ctx)
	}
	timeout := time.NewTimer(resultTimeout)
	defer timeout.Stop()
	for {
		s.em.Lock()
		ready := s.replies >= s.commits
		for _, it := range s.items {
			ready = ready && it.done
		}
		text, err := s.snapshot(), s.failed
		s.em.Unlock()
		if err != nil {
			return "", fmt.Errorf("openaispeech stt: %w", err)
		}
		if ready {
			return text, nil
		}
		select {
		case <-s.changed:
		case <-timeout.C:
			return "", fmt.Errorf("openaispeech stt: result timeout")
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

func (s *rtStream) Abort() {
	s.mu.Lock()
	if s.aborted || s.done {
		s.mu.Unlock()
		return
	}
	s.aborted, s.pcm = true, nil
	s.mu.Unlock()
	close(s.stop)
	// Barge-in calls this from the audio loop: don't wait on a dial in flight.
	go func() {
		<-s.ready
		if s.conn != nil {
			s.conn.CloseNow()
		}
	}()
}

var _ provider.STTStream = (*rtStream)(nil)
