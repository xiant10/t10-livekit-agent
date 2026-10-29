package engine

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/provider"
)

// fakeLLM emits a fixed sequence of tokens, ignoring the conversation it's
// given — enough to prove RunChatTurn's plumbing without a real model.
type fakeLLM struct {
	tokens []string
}

func (f *fakeLLM) ChatStream(_ context.Context, _ []provider.Message, onToken func(string)) (string, error) {
	var full string
	for _, t := range f.tokens {
		onToken(t)
		full += t
	}
	return full, nil
}

// recordingSink records every token written to it, standing in for a real
// transport's engine.ReplySink implementation. Guarded by mu: live-call
// tests run handleTurn in its own goroutine and poll Closed()/Tokens() from
// the test goroutine concurrently.
type recordingSink struct {
	mu     sync.Mutex
	tokens []string
	closed bool
}

func (s *recordingSink) WriteToken(delta string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokens = append(s.tokens, delta)
}
func (s *recordingSink) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
}
func (s *recordingSink) Closed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}
func (s *recordingSink) Tokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.tokens...)
}

// blockingLLM blocks its first ChatStream call until ctx is cancelled —
// simulating a reply that's still in flight when a barge-in happens.
// Subsequent calls answer immediately with a fixed short reply, standing in
// for the turn that follows an interruption.
type blockingLLM struct {
	calls   atomic.Int32
	started chan struct{}
}

func newBlockingLLM() *blockingLLM {
	return &blockingLLM{started: make(chan struct{})}
}

func (f *blockingLLM) ChatStream(ctx context.Context, _ []provider.Message, onToken func(string)) (string, error) {
	if f.calls.Add(1) == 1 {
		close(f.started)
		<-ctx.Done()
		return "", ctx.Err()
	}
	onToken("ok.")
	return "ok.", nil
}

// recordingLLM records the conversation it was handed on each call, so a
// test can check what the model was actually asked (e.g. that two utterances
// were coalesced into one turn), and answers with a fixed token sequence.
type recordingLLM struct {
	tokens []string

	mu    sync.Mutex
	calls [][]provider.Message
}

func (f *recordingLLM) ChatStream(_ context.Context, msgs []provider.Message, onToken func(string)) (string, error) {
	f.mu.Lock()
	f.calls = append(f.calls, msgs)
	f.mu.Unlock()
	var full string
	for _, t := range f.tokens {
		onToken(t)
		full += t
	}
	return full, nil
}

func (f *recordingLLM) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func (f *recordingLLM) lastCall() []provider.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.calls) == 0 {
		return nil
	}
	return f.calls[len(f.calls)-1]
}

// fakeSTTProvider hands out a canned transcript per call to NewStream, in
// order — one call per VAD-bounded utterance in a live-call test.
// interimDeltas[i], if present, is sent (synchronously, before NewStream
// returns) through the i-th call's onInterim callback. If commitGate is
// non-nil, every Commit blocks until it is closed, which is what lets a test
// hold an utterance in transcription while driving more speech at the
// controller.
type fakeSTTProvider struct {
	mu            sync.Mutex
	transcripts   []string
	interimDeltas [][]string
	commitGate    chan struct{}
	calls         int
	pushedBytes   int
}

func (f *fakeSTTProvider) TranscribeBatch(context.Context, []byte, string) (string, error) {
	return "", nil
}

func (f *fakeSTTProvider) NewStream(_ context.Context, _ string, onInterim func(string)) (provider.STTStream, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i := f.calls
	var transcript string
	if i < len(f.transcripts) {
		transcript = f.transcripts[i]
	}
	f.calls++
	if onInterim != nil && i < len(f.interimDeltas) {
		var preview string
		for _, d := range f.interimDeltas[i] {
			preview += d
			onInterim(preview)
		}
	}
	return &fakeSTTStream{provider: f, transcript: transcript}, nil
}

// streamCount is how many utterances have been opened so far.
func (f *fakeSTTProvider) streamCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// pushed is the total audio, in bytes, handed to every stream this provider
// has made — what a pre-roll test counts.
func (f *fakeSTTProvider) pushed() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pushedBytes
}

type fakeSTTStream struct {
	provider   *fakeSTTProvider
	transcript string
}

func (s *fakeSTTStream) PushPCM(pcm []byte) {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	s.provider.pushedBytes += len(pcm)
}

func (s *fakeSTTStream) Commit(ctx context.Context) (string, error) {
	s.provider.mu.Lock()
	gate := s.provider.commitGate
	s.provider.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return s.transcript, nil
}

func (s *fakeSTTStream) Abort() {}

// fakeTTS "synthesizes" one PCM16 sample per rune of text — enough to prove
// non-empty audio flowed through, without needing real speech content.
type fakeTTS struct{ sampleRate int }

func (f *fakeTTS) Synthesize(_ context.Context, text, _ string) ([]byte, int, error) {
	samples := make([]int16, len([]rune(text)))
	return audio.Int16ToBytes(samples), f.sampleRate, nil
}

// fakeSource is a test-driven engine.AudioSource: the test pushes frames
// onto its channel at whatever pace it likes (VAD timing is derived from
// frame length/SampleRate, not wall-clock, so there's no need for the test
// to sleep in real time). Closing frames ends the call loop with io.EOF.
type fakeSource struct {
	rate   int
	frames chan []int16
}

func newFakeSource(rate int) *fakeSource {
	return &fakeSource{rate: rate, frames: make(chan []int16, 16)}
}

func (s *fakeSource) SampleRate() int { return s.rate }

func (s *fakeSource) ReadFrame() ([]int16, error) {
	f, ok := <-s.frames
	if !ok {
		return nil, io.EOF
	}
	return f, nil
}

// fakeSink is a test-driven engine.AudioSink recording every PushPCM/Drain
// call. It reports an empty backlog by default (as if playback were
// instantaneous) — call hold() before a test needs to simulate audio that's
// still physically playing after handleTurn finished generating it, then
// releasePlayback() to let the waits on it return.
type fakeSink struct {
	rate int

	mu      sync.Mutex
	pushed  [][]int16
	drains  int
	holding bool
	queued  time.Duration
}

func newFakeSink(rate int) *fakeSink { return &fakeSink{rate: rate} }

func (s *fakeSink) SampleRate() int { return s.rate }

func (s *fakeSink) PushPCM(samples []int16) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pushed = append(s.pushed, append([]int16(nil), samples...))
	if s.holding {
		s.queued = time.Hour // "this is still playing" until releasePlayback
	}
}

func (s *fakeSink) Drain() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drains++
	s.pushed = nil
	s.queued = 0
}

func (s *fakeSink) QueuedDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queued
}

// hold makes the sink report a never-emptying backlog once something has
// been pushed to it, so a turn stays "speaking" (and barge-in-eligible)
// after its generation has finished.
func (s *fakeSink) hold() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holding = true
}

func (s *fakeSink) releasePlayback() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holding = false
	s.queued = 0
}

func (s *fakeSink) pushCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pushed)
}

func (s *fakeSink) drainCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drains
}

// loudFrame/silentFrame build VAD test frames: constant high amplitude
// (well above any reasonable EnergyThreshold) or silence.
func loudFrame(n int) []int16 {
	f := make([]int16, n)
	for i := range f {
		f[i] = 20000
	}
	return f
}

func silentFrame(n int) []int16 {
	return make([]int16, n)
}

func (s *recordingSink) WriteSnapshot(text string) { s.WriteToken(text) }
