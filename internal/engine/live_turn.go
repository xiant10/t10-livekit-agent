package engine

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"go.opentelemetry.io/otel/attribute"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/telemetry"
)

// VADConfig tunes the energy-based voice-activity detector driving live-call
// turn-taking.
type VADConfig struct {
	EnergyThreshold float64
	MinSpeechMS     int
	MinSilenceMS    int
	// BargeInMinSpeechMS is the speech-start debounce applied *while the
	// agent is audibly speaking*, in place of MinSpeechMS. It wants to be
	// the longer of the two: starting a fresh turn should be as responsive
	// as possible, but cutting the agent off mid-word is disruptive enough
	// that it should take real, sustained speech — not a cough, a door, or
	// (with no echo cancellation anywhere in this pipeline) the agent's own
	// voice coming back in through an open speaker.
	BargeInMinSpeechMS int
}

// LiveConfig is everything LiveCallController needs tuned: how speech is
// segmented, how far ahead of the speaker the pipeline may run, and how hard
// it leans on the TTS provider.
type LiveConfig struct {
	VAD VADConfig

	// PreRollMS is how much audio from *before* VAD declared speech is
	// replayed into the STT stream. See preRoll.
	PreRollMS int
	// MaxLeadMS caps how much synthesized audio may sit queued in the sink
	// ahead of what the caller is actually hearing. See playSpeech.
	MaxLeadMS int

	// TTSConcurrency bounds how many synthesis calls may be in flight at
	// once, and TTSTimeout how long any one of them may take.
	TTSConcurrency int
	TTSTimeout     time.Duration

	// MinChunkRunes/MaxChunkRunes size the units handed to TTS. See
	// speechChunker.
	MinChunkRunes int
	MaxChunkRunes int

	// VADOverride, if set, is used instead of the default energy-based
	// detector. This allows injecting a neural VAD or a fake for testing.
	VADOverride VADProcessor
}

// withDefaults fills in anything left zero, so a caller can set only the
// knobs it cares about (and so the tests don't have to restate all of them).
func (c LiveConfig) withDefaults() LiveConfig {
	if c.VAD.EnergyThreshold == 0 {
		c.VAD.EnergyThreshold = 500
	}
	if c.VAD.MinSpeechMS == 0 {
		c.VAD.MinSpeechMS = 200
	}
	if c.VAD.MinSilenceMS == 0 {
		c.VAD.MinSilenceMS = 700
	}
	if c.VAD.BargeInMinSpeechMS == 0 {
		c.VAD.BargeInMinSpeechMS = c.VAD.MinSpeechMS * 2
	}
	if c.PreRollMS == 0 {
		c.PreRollMS = 400
	}
	c.PreRollMS = max(c.PreRollMS, c.VAD.MinSpeechMS, c.VAD.BargeInMinSpeechMS+audio.MaxSpeechGapMS)
	if c.MaxLeadMS == 0 {
		c.MaxLeadMS = 700
	}
	if c.TTSConcurrency <= 0 {
		c.TTSConcurrency = 2
	}
	if c.TTSTimeout == 0 {
		c.TTSTimeout = 8 * time.Second
	}
	if c.MinChunkRunes == 0 {
		c.MinChunkRunes = 40
	}
	if c.MaxChunkRunes == 0 {
		c.MaxChunkRunes = 180
	}
	return c
}

// CallState is a coarse "what is the agent doing right now" signal, reported
// to the transport through LiveCallController.OnState so a client can do the
// things only it can do: drop the audio still sitting in its own speaker
// buffer the instant a barge-in happens, and show the caller whether they're
// being listened to, waited on, or talked to.
type CallState string

const (
	// StateListening: VAD has detected speech; an utterance is being captured.
	StateListening CallState = "listening"
	// StateThinking: the utterance is transcribed and a reply is being generated.
	StateThinking CallState = "thinking"
	// StateSpeaking: the first audio of the reply is on its way out.
	StateSpeaking CallState = "speaking"
	// StateInterrupted: a barge-in cancelled the reply. Everything already
	// queued downstream — including audio the client has buffered but not
	// yet played — should be discarded now.
	StateInterrupted CallState = "interrupted"
	// StateIdle: the reply finished playing normally.
	StateIdle CallState = "idle"
)

// interruptedMarker is appended to the part of a reply that was actually
// spoken before a barge-in cut it off, so the model can tell a finished
// answer from an abandoned one. It's a bracketed note rather than an
// ellipsis on purpose: an ellipsis reads as *style*, and the model picked it
// up and started ending its own replies with one.
const interruptedMarker = provider.InterruptedMarker

const (
	sttSampleRate = 16000
	// sttCommitTimeout bounds one utterance's final transcription.
	sttCommitTimeout = 15 * time.Second
	// sinkPollInterval is how often the wait loops below re-check how much
	// audio the sink still has to play.
	sinkPollInterval = 20 * time.Millisecond
	// chunkAudioBuffer is how many pieces of one chunk's audio can wait for
	// playback. It has to hold a whole chunk: a chunk behind one still playing
	// sits here, and blocking its synthesis would spend its TTS timeout.
	chunkAudioBuffer = 1024
)

// LiveCallController runs one participant's live call. Audio arrives from an
// AudioSource, VAD segments it into utterances, each utterance is
// transcribed, and the reply is synthesized in chunks and pushed to an
// AudioSink so speech starts before the whole reply exists.
//
// It has no transport dependency: source/sink are supplied by whatever
// adapter is running it (see internal/livekit/live_mode.go).
//
// # Three stages, deliberately decoupled
//
// The work of a call splits into three things that run at completely
// different speeds, and the controller runs each on its own goroutine:
//
//  1. Run — the real-time audio loop. Must never block: it is draining a
//     live RTP track.
//  2. commitUtterance — closes out one utterance's STT stream. Sub-second,
//     and its output (the transcript) is what the caller most wants to see
//     immediately.
//  3. handleTurn — LLM + TTS + playback for one reply. Tens of seconds.
//
// These used to be two stages, with 2 and 3 sharing a worker so that turns
// could not overlap. That serialization was correct but far too coarse: on a
// real call, the transcript of what someone said sat un-committed behind the
// *previous* reply's entire playback, so their own words appeared on screen
// up to 45 seconds after they spoke them. Splitting 2 from 3 means the
// transcript is always ~700ms behind the speaker, no matter how long the
// agent is talking; only reply generation stays strictly serial.
type LiveCallController struct {
	deps     *Deps
	identity string
	cfg      LiveConfig

	// callCtx carries the call's session ID (see telemetry.WithSessionID in
	// internal/livekit) for the whole lifetime of the call, and every turn
	// descends from it. It is also the cancellation root for every stage: Run
	// cancels it on the way out, and the commit/turn workers unwind rather
	// than finishing work for a call whose audio track has already gone away.
	callCtx  context.Context
	stopCall context.CancelFunc

	vad VADProcessor

	// OnTranscript, if set, is called exactly once per captured utterance
	// with the utterance ID and STT's final, authoritative transcript, as soon as it is
	// available — before, and independently of, whatever reply it leads to.
	// Called even when the utterance turned out to carry no speech
	// (transcript ""), so a client can clear whatever interim caption it was
	// showing. ctx carries the turn, as it does for the other hooks below.
	OnTranscript func(ctx context.Context, utteranceID uint64, transcript string)
	// NewReplySink, if set, is called once per *turn* to obtain a sink for
	// the reply's text. Note "per turn", not "per utterance": two utterances
	// that arrive close enough together are answered by one turn (see
	// coalesce), and an utterance that transcribes to nothing produces no
	// turn at all.
	//
	// The text arrives chunk by chunk, paced to the audio actually going out
	// of the AudioSink rather than to the LLM's generation speed — see
	// playSpeech for why that matters.
	NewReplySink func(ctx context.Context) ReplySink
	// NewInterimSink, if set, is called once per utterance the moment speech
	// starts (before STT has produced anything) to obtain a sink for the
	// STT's best-guess-so-far text as replacement snapshots. Always
	// closed exactly once, before OnTranscript fires with the authoritative
	// (and possibly corrected) text for the same utterance.
	NewInterimSink func(ctx context.Context, utteranceID uint64) TranscriptSink
	// OnState, if set, is called with each CallState transition and speech version. It may be
	// called from several goroutines over the life of a call, though never
	// two at once. Callbacks must return promptly and must not reenter the
	// controller; they serialize with floor changes.
	OnState func(context.Context, CallState, uint64)

	// turnVersion is bumped every time the caller starts talking. A turn
	// captures it at the start and stops touching the sink (or the reply
	// sink) the moment it no longer matches — sink.Drain() only clears what
	// is already queued, so without this a chunk still synthesizing at
	// barge-in time would land in the sink just after the drain and play
	// anyway.
	turnVersion atomic.Uint64
	// inFlight counts utterances between speech-start and their transcript
	// being handed to the turn worker. It is what stops the agent talking
	// over someone who is mid-sentence: a turn only begins once this is
	// zero, i.e. the caller has actually stopped and everything they said
	// has been transcribed. See waitForQuiet.
	inFlight atomic.Int64
	// speaking is true while a reply is audible — from the first chunk
	// pushed until the sink has actually played everything out. working is
	// true for the whole of handleTurn, including the silent stretch before
	// the first chunk is ready. Both matter: "speaking" selects the stricter
	// barge-in debounce, and either one makes a new utterance an
	// interruption worth telling the client about.
	speaking atomic.Bool
	working  atomic.Bool

	mu        sync.Mutex
	curCancel context.CancelFunc
	// turnStarted is when the current turn began thinking. Guarded by mu.
	turnStarted time.Time
	// pending is the turn the caller is speaking into, and current the one
	// being answered. Guarded by mu, so a turn's boundary is decided by the same
	// lock that decides barge-ins. See liveTurn.
	pending, current *liveTurn

	utterances chan finishedUtterance // Run → commit worker
	turns      chan string            // commit worker → turn worker

	// ttsSem bounds concurrent synthesis calls. Sized from
	// LiveConfig.TTSConcurrency; see synthesizeChunk.
	ttsSem chan struct{}
}

// finishedUtterance is one VAD-bounded speech segment whose STT stream has
// stopped receiving audio and is ready to be committed.
type finishedUtterance struct {
	id          uint64
	turn        *liveTurn
	ctx         context.Context // the utterance's span, under turn
	end         func(error)
	endedAt     time.Time
	stream      provider.STTStream
	interimSink TranscriptSink // from NewInterimSink at speech-start; nil if unset
	capturedMS  int
	partial     *atomic.Pointer[string]
}

// liveTurn is one turn of a live call, which starts earlier than the reply: the
// speech that opens it is what the caller's wait is measured from, and
// everything they say before the reply starts is part of it, however many
// utterances that was. So the first speech with no turn pending opens one, the
// reply claims it, and speech after that opens the next. Speech that yields no
// words ends it there, as OutcomeNoSpeech.
//
// Fields are guarded by the controller's mu.
type liveTurn struct {
	*telemetry.Turn
	heard       int // utterances that produced words
	speechEnded time.Time
	endPlayback func(error)
}

// latestSnapshot returns the last interim transcript stored in p, or "".
func latestSnapshot(p *atomic.Pointer[string]) string {
	if p == nil {
		return ""
	}
	if s := p.Load(); s != nil {
		return *s
	}
	return ""
}

// pendingChunk is one chunk of a reply queued for playback: the text that
// will be captioned, and a channel carrying its audio as TTS produces it. The
// channel closes when synthesis ends; a chunk that failed before any audio
// closes it without sending.
type pendingChunk struct {
	text   string
	result chan speechResult
}

// speechResult is one piece of a chunk's audio, already resampled to the
// sink's rate.
type speechResult struct {
	samples []int16
}

func buildVAD(cfg LiveConfig) VADProcessor {
	if cfg.VADOverride != nil {
		return cfg.VADOverride
	}
	return &audio.VAD{
		EnergyThreshold: cfg.VAD.EnergyThreshold,
		MinSpeechMS:     cfg.VAD.MinSpeechMS,
		MinSilenceMS:    cfg.VAD.MinSilenceMS,
	}
}

func NewLiveCallController(sessionCtx context.Context, deps *Deps, identity string, cfg LiveConfig) *LiveCallController {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(sessionCtx)
	return &LiveCallController{
		deps:     deps,
		identity: identity,
		cfg:      cfg,
		callCtx:  ctx,
		stopCall: cancel,
		vad:      buildVAD(cfg),
		// Both sized generously so that in practice neither producer blocks:
		// an utterance is at minimum MinSpeechMS+MinSilenceMS of talking, so
		// these would take a minute of nonstop speech to fill.
		utterances: make(chan finishedUtterance, 32),
		turns:      make(chan string, 32),
		ttsSem:     make(chan struct{}, cfg.TTSConcurrency),
	}
}

// Run reads audio from source until it errors (e.g. the participant's track
// ends), driving VAD/STT/LLM/TTS turns and pushing replies to sink. It
// blocks; callers should invoke it in its own goroutine.
func (l *LiveCallController) Run(source AudioSource, sink AudioSink) error {
	var (
		curStream      provider.STTStream
		curInterim     TranscriptSink
		curUtteranceID uint64
		curSamples     int
		curPartial     *atomic.Pointer[string]
		curTurn        *liveTurn
		curCtx         context.Context
		curEnd         func(error)
	)
	capturedMS := func() int { return curSamples * 1000 / sttSampleRate }
	pre := newPreRoll(l.cfg.PreRollMS, sttSampleRate)

	turnsDone := make(chan struct{})
	go func() {
		defer close(turnsDone)
		l.runTurns(sink)
	}()
	commitsDone := make(chan struct{})
	go func() {
		defer close(commitsDone)
		for u := range l.utterances {
			l.commitUtterance(u)
		}
		close(l.turns)
	}()

	defer func() {
		if curStream != nil {
			// A partial tells these apart: words mean the caller was still
			// talking, none mean noise was holding the VAD open.
			slog.WarnContext(curCtx, "live call: speech discarded", "identity", l.identity,
				"reason", "call ended before end of speech", "utterance_id", curUtteranceID,
				"captured_ms", capturedMS(), "partial", latestSnapshot(curPartial))
			curStream.Abort()
			curEnd(nil)
			l.settleUtterance(curTurn, telemetry.OutcomeCallEnded)
		}
		if curInterim != nil {
			curInterim.Close()
		}
		// The track is gone: cancel anything still in flight rather than
		// generating and "speaking" a reply into a room nobody is listening
		// on, then wait for both workers to unwind so nothing outlives the
		// sink they're writing to.
		l.stopCall()
		close(l.utterances)
		<-commitsDone
		<-turnsDone

		// Words that never got a reply: the caller hung up before it started.
		l.mu.Lock()
		if l.pending != nil {
			l.pending.End(telemetry.OutcomeCallEnded, nil)
			l.pending = nil
		}
		l.mu.Unlock()
	}()

	for {
		frame, err := source.ReadFrame()
		if err != nil {
			return err
		}

		samples16k := frame
		if source.SampleRate() != sttSampleRate {
			samples16k = audio.Resample(frame, source.SampleRate(), sttSampleRate)
		}

		// Interrupting audible speech is held to the stricter debounce; a
		// fresh turn to the responsive one. Swapping this between frames
		// only moves the bar for an onset still being counted; speech that
		// has already started is unaffected.
		if l.speaking.Load() {
			l.vad.SetMinSpeechMS(l.cfg.VAD.BargeInMinSpeechMS)
		} else {
			l.vad.SetMinSpeechMS(l.cfg.VAD.MinSpeechMS)
		}

		switch l.vad.Process(samples16k, sttSampleRate) {
		case audio.EventSpeechStart:
			// Unconditional: a barge-in that arrives while the previous
			// utterance is still being transcribed — i.e. before any reply
			// exists to cancel — must still bump the turn version, or the
			// turn that starts a moment later would inherit a version the
			// interruption already invalidated.
			l.inFlight.Add(1) // Claim the floor before callbacks or STT setup.
			curTurn = l.bargeIn(sink)
			curUtteranceID = l.turnVersion.Load()
			curCtx, curEnd = telemetry.StartStage(curTurn.Context(), "utterance", attribute.Int64("utterance_id", int64(curUtteranceID)))
			l.setState(StateListening)

			var interim TranscriptSink
			if l.NewInterimSink != nil {
				interim = l.NewInterimSink(curCtx, curUtteranceID)
			}
			partial := new(atomic.Pointer[string])
			stream, err := l.deps.STT.NewStream(l.callCtx, "", func(delta string) {
				partial.Store(&delta)
				if interim != nil {
					interim.WriteSnapshot(delta)
				}
			})
			if err != nil {
				slog.ErrorContext(curCtx, "live call: failed to open STT stream", "err", err)
				if interim != nil {
					interim.Close()
				}
				curEnd(err)
				l.settleUtterance(curTurn, telemetry.OutcomeFailed)
				pre.reset()
				continue
			}
			curSamples = 0
			if lead := pre.take(); len(lead) > 0 {
				stream.PushPCM(audio.Int16ToBytes(lead))
				curSamples = len(lead)
			}
			curStream = stream
			curInterim = interim
			curPartial = partial

		case audio.EventSpeechEnd:
			if curStream != nil {
				l.utterances <- finishedUtterance{id: curUtteranceID, turn: curTurn, ctx: curCtx, end: curEnd, endedAt: time.Now(), stream: curStream, interimSink: curInterim, capturedMS: capturedMS(), partial: curPartial}
				curStream = nil
				curInterim = nil
				curPartial = nil
			}
		}

		if curStream != nil {
			curStream.PushPCM(audio.Int16ToBytes(samples16k))
			curSamples += len(samples16k)
		} else {
			pre.add(samples16k)
		}
	}
}

// commitUtterance closes out one utterance: final transcript, interim
// caption closed, transcript published, and (if there were any words in it)
// a turn queued. It runs on its own worker precisely so that none of this
// waits on the previous turn's reply still playing.
func (l *LiveCallController) commitUtterance(u finishedUtterance) {
	// Released only once the transcript has been handed on (or discarded),
	// so a turn waiting on waitForQuiet can't start in the gap between this
	// utterance ending and its words being available to answer.
	outcome := telemetry.OutcomeNoSpeech
	defer func() { l.settleUtterance(u.turn, outcome) }()

	ctx, cancel := context.WithTimeout(u.ctx, sttCommitTimeout)
	defer cancel()

	sttStarted := time.Now()
	sttCtx, sttEnd := telemetry.StartStage(ctx, "stt.commit")
	transcript, err := u.stream.Commit(sttCtx)
	sttEnd(err)
	u.end(err)
	sttMS := time.Since(sttStarted).Milliseconds()

	// The interim caption is done regardless of how Commit turned out — from
	// here on OnTranscript carries this utterance's authoritative (possibly
	// corrected, possibly empty) text.
	if u.interimSink != nil {
		u.interimSink.Close()
	}
	reason := "empty transcript"
	if err != nil {
		slog.ErrorContext(ctx, "live call: stt commit failed", "identity", l.identity, "err", err)
		transcript = ""
		reason = "stt commit failed"
		outcome = telemetry.OutcomeFailed
	}
	transcript = strings.TrimSpace(transcript)

	if l.OnTranscript != nil {
		l.OnTranscript(ctx, u.id, transcript)
	}
	if transcript == "" {
		// Usually a false VAD trigger on background noise, with nothing to
		// answer, but a failed commit loses real words the same way.
		slog.WarnContext(ctx, "live call: speech discarded", "identity", l.identity, "reason", reason,
			"utterance_id", u.id, "captured_ms", u.capturedMS, "stt_ms", sttMS, "partial", latestSnapshot(u.partial))
		return
	}
	slog.InfoContext(ctx, "live call: heard", "identity", l.identity, "utterance_id", u.id, "text", transcript, "captured_ms", u.capturedMS, "stt_ms", sttMS)

	l.mu.Lock()
	u.turn.heard++
	u.turn.speechEnded = u.endedAt
	l.mu.Unlock()

	select {
	case l.turns <- transcript:
	case <-l.callCtx.Done():
	}
}

// settleUtterance releases the utterance's hold on the floor. If it was the
// last one and the pending turn got no words out of any of them, no reply is
// coming and the turn ends here.
func (l *LiveCallController) settleUtterance(turn *liveTurn, outcome string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.inFlight.Add(-1) > 0 || l.pending != turn || turn.heard > 0 {
		return
	}
	l.pending = nil
	turn.End(outcome, nil)
}

// runTurns answers transcripts one turn at a time, coalescing any that
// piled up while the previous turn was running.
func (l *LiveCallController) runTurns(sink AudioSink) {
	for {
		first, ok := <-l.turns
		if !ok {
			return
		}
		// Don't start answering until the caller has actually stopped — then
		// coalesce, so anything they said in the meantime is answered by
		// this same turn rather than queued behind it.
		batch := []string{first}
		for {
			version := l.turnVersion.Load()
			l.waitForQuiet()
			if l.callCtx.Err() != nil {
				return
			}
			if version != l.turnVersion.Load() {
				continue
			}
			batch = l.coalesce(batch)
			if l.handleTurn(batch, sink, version) {
				break
			}
		}
	}
}

// waitForQuiet blocks until no utterance is being captured or transcribed.
// It is bounded in practice by the VAD's own end-of-speech detection and by
// sttCommitTimeout; a caller who simply keeps talking keeps the agent
// listening, which is the intended behaviour.
//
// Turn registration rechecks both the version and inFlight under the same
// mutex used by bargeIn, so speech resuming after this check cannot lend
// its newer version to an old reply.
func (l *LiveCallController) waitForQuiet() {
	ticker := time.NewTicker(sinkPollInterval)
	defer ticker.Stop()
	for l.inFlight.Load() > 0 {
		select {
		case <-ticker.C:
		case <-l.callCtx.Done():
			return
		}
	}
}

// coalesce collects every transcript already waiting behind batch and
// answers them as a single turn.
//
// This is what makes a mid-thought pause survivable. The energy VAD ends an
// utterance after MinSilenceMS of quiet, which a speaker gathering their
// thoughts crosses all the time — real calls produce transcripts like "嗯，
// 我今天的计划是。" followed seconds later by the rest of the sentence. Answering
// the fragment on its own is the wrong thing twice over: the reply is to
// half a question, and it arrives while the caller is still finishing the
// other half. Between waitForQuiet holding the turn back and this putting
// both halves to the model together, the pause costs nothing but the pause.
// (If the agent had already started replying when the caller resumed, the
// resumption's speech-start barged in and cancelled that reply first; what
// it did manage to say is recorded in the history either way.)
func (l *LiveCallController) coalesce(batch []string) []string {
	for {
		select {
		case next, ok := <-l.turns:
			if !ok {
				return batch
			}
			batch = append(batch, next)
		default:
			return batch
		}
	}
}

// bargeIn invalidates the in-flight reply (if any), cancels whatever LLM/TTS
// call it is blocked on, and drops audio queued but not yet played. It returns
// the turn this speech belongs to.
func (l *LiveCallController) bargeIn(sink AudioSink) *liveTurn {
	l.mu.Lock()
	defer l.mu.Unlock()
	interrupting := l.speaking.Load() || l.working.Load()
	l.turnVersion.Add(1)
	cancel := l.curCancel
	l.curCancel = nil
	if cancel != nil {
		cancel()
	}
	sink.Drain()

	// Only announce an interruption that actually interrupted something —
	// StateInterrupted tells the client to throw away its own buffered
	// audio, which is meaningless (and, on screen, confusing) when the agent
	// wasn't doing anything.
	if interrupting {
		l.setStateLocked(StateInterrupted)
	}
	if l.pending == nil {
		var follows *telemetry.Turn
		if interrupting && l.current != nil {
			follows = l.current.Turn
		}
		l.pending = l.newTurn(follows)
	}
	return l.pending
}

func (l *LiveCallController) newTurn(follows *telemetry.Turn) *liveTurn {
	return &liveTurn{Turn: telemetry.StartTurn(l.callCtx, telemetry.TurnOptions{
		Mode:    "live",
		Follows: follows,
		Attrs:   []attribute.KeyValue{attribute.String("identity", l.identity)},
	})}
}

// turnCtx is the context of the turn the floor is about right now, which is
// the one being spoken into, if any, ahead of one still unwinding. Callers hold mu.
func (l *LiveCallController) turnCtx() context.Context {
	switch {
	case l.pending != nil:
		return l.pending.Context()
	case l.current != nil:
		return l.current.Context()
	}
	return l.callCtx
}

func (l *LiveCallController) setState(s CallState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.setStateLocked(s)
}

func (l *LiveCallController) setStateLocked(s CallState) {
	ctx := l.turnCtx()
	slog.InfoContext(ctx, "live call: state", "identity", l.identity, "state", s, "turn_version", l.turnVersion.Load(), "in_flight", l.inFlight.Load())
	if l.OnState != nil {
		l.OnState(ctx, s, l.turnVersion.Load())
	}
}

func (l *LiveCallController) handleTurn(transcripts []string, sink AudioSink, version uint64) bool {
	l.mu.Lock()
	if l.pending == nil {
		l.pending = l.newTurn(nil)
	}
	ctx, cancel := context.WithCancel(l.pending.Context())
	defer cancel()

	superseded := func() bool { return ctx.Err() != nil || l.turnVersion.Load() != version }

	// Recheck after quiet/coalescing; a failed admission leaves the batch
	// with runTurns so even a continuation that transcribes empty loses no
	// words, and the turn pending, for that speech to join.
	if superseded() || l.inFlight.Load() > 0 {
		l.mu.Unlock()
		return false
	}
	turn := l.pending
	l.pending = nil
	l.current = turn
	l.curCancel = cancel
	l.turnStarted = time.Now()
	l.working.Store(true)
	l.setStateLocked(StateThinking)
	l.mu.Unlock()

	// Opening a provider session costs about as long as the model takes to
	// reach its first chunk, so run the two side by side.
	if tts, ok := l.deps.TTS.(provider.StreamingTTS); ok {
		tts.Warm(l.deps.Voice)
	}

	convo := l.deps.Conversations.Get(l.identity)
	for _, t := range transcripts {
		convo.Append("user", t)
	}

	joined := strings.Join(transcripts, " ")
	ctx, endTurn := telemetry.StartStage(ctx, "turn",
		attribute.String("identity", l.identity),
		attribute.Int("transcript.chars", len(joined)),
		attribute.Int("utterances", len(transcripts)))

	var replySink ReplySink
	if l.NewReplySink != nil {
		replySink = l.NewReplySink(ctx)
	}

	// Chunks are dispatched in order, with backpressure when every TTS slot
	// is occupied. Transcript delivery stays independent. They are *played*
	// in submission order even though a later chunk's TTS call may finish
	// first: speechQueue carries one pending chunk per entry in order, and
	// playSpeech blocks on each in turn.
	sinkRate := sink.SampleRate()
	speechQueue := make(chan pendingChunk, 64)
	playerDone := make(chan struct{})
	var spoken strings.Builder
	go l.playSpeech(ctx, version, speechQueue, sink, replySink, &spoken, playerDone)

	var replyErr error
	defer func() {
		// Stop feeding the player, let it hand off whatever was already
		// synthesized, then wait for the speaker to actually catch up.
		// Waiting matters: PushPCM returns as soon as audio is *queued*, so
		// without this the turn would look finished — and stop being
		// barge-in-eligible — seconds before the caller had heard it. The
		// wait returns immediately if a barge-in already cancelled ctx, so
		// it never delays an interruption.
		close(speechQueue)
		<-playerDone
		waitSinkBelow(ctx, sink, 0)
		l.speaking.Store(false)
		l.mu.Lock()
		endPlayback := turn.endPlayback
		l.mu.Unlock()
		if endPlayback != nil {
			endPlayback(nil)
		}

		l.recordTurnResult(convo, spoken.String(), superseded())

		if replySink != nil {
			replySink.Close()
		}
		l.working.Store(false)
		endTurn(nil) // per-stage (llm/tts) spans below carry their own errors
		l.mu.Lock()
		l.curCancel = nil

		// Emitted after the reply sink is closed so a client can't see
		// "done" while the last of the text is still arriving. A barge-in
		// already sent StateInterrupted, immediately, from bargeIn.
		if !superseded() {
			l.setStateLocked(StateIdle)
		}
		l.current = nil
		l.mu.Unlock()

		switch {
		case l.callCtx.Err() != nil:
			turn.End(telemetry.OutcomeCallEnded, nil)
		case superseded():
			turn.End(telemetry.OutcomeInterrupted, nil)
		default:
			turn.End(telemetry.OutcomeCompleted, replyErr)
		}
	}()

	replyErr = l.streamAndSynthesize(ctx, convo, sinkRate, l.ttsSem, speechQueue, superseded)
	return true
}

// recordTurnResult appends what the caller actually heard to conversation
// history. On a barge-in, only the spoken portion plus an [interrupted]
// marker is recorded; on a normal completion, the full text is recorded.
func (l *LiveCallController) recordTurnResult(convo *Conversation, spoken string, interrupted bool) {
	said := strings.TrimSpace(spoken)
	if said == "" {
		return
	}
	if interrupted {
		convo.Append("assistant", said+interruptedMarker)
	} else {
		convo.Append("assistant", said)
	}
}

// streamAndSynthesize runs the LLM, chunks its output, and dispatches
// concurrent TTS synthesis calls into speechQueue. Returns when the LLM
// stream is exhausted or ctx is cancelled.
func (l *LiveCallController) streamAndSynthesize(
	ctx context.Context,
	convo *Conversation,
	sinkRate int,
	ttsSem chan struct{},
	speechQueue chan<- pendingChunk,
	superseded func() bool,
) error {
	chunker := newSpeechChunker(l.cfg.MinChunkRunes, l.cfg.MaxChunkRunes)
	queue := func(text string) {
		if superseded() {
			return
		}
		text = speechText(text)
		if !hasSpeakableContent(text) {
			return
		}
		select {
		case ttsSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		chunk := pendingChunk{text: text, result: make(chan speechResult, chunkAudioBuffer)}
		select {
		case speechQueue <- chunk:
		case <-ctx.Done():
			<-ttsSem
			return
		}
		go l.synthesizeChunk(ctx, text, sinkRate, chunk.result)
	}

	llmStarted := time.Now()
	var firstToken sync.Once
	llmCtx, llmEnd := telemetry.StartStage(ctx, "llm.chat_stream")
	_, err := l.deps.LLM.ChatStream(provider.WithLiveCall(provider.WithSession(llmCtx, l.identity)), convo.Snapshot(), func(delta string) {
		firstToken.Do(func() {
			slog.InfoContext(ctx, "live call: llm first token", "identity", l.identity, "ms", time.Since(llmStarted).Milliseconds())
		})
		for _, chunk := range chunker.push(delta) {
			queue(chunk)
		}
	})
	if errors.Is(err, provider.ErrSuperseded) {
		err = nil // a typed message took over; finish what was already said
		telemetry.MarkOutcome(ctx, telemetry.OutcomeSuperseded)
	}
	if err != nil && superseded() {
		llmEnd(nil)
	} else {
		llmEnd(err)
	}
	if err != nil {
		if !superseded() {
			slog.ErrorContext(ctx, "live call: llm failed", "identity", l.identity, "err", err)
			if ctx.Err() == nil {
				// Silence is the failure mode: say so, whatever was said before it.
				queue(chunker.flush())
				queue(failureSpeech(convo))
			}
		}
		return err
	}
	if rest := chunker.flush(); rest != "" {
		queue(rest)
	}
	return nil
}

// failureSpeech is the reason a caller hears when no answer is coming, in the
// language they last spoke. Provider details stay in the log.
func failureSpeech(convo *Conversation) string {
	history := convo.Snapshot()
	for i := len(history) - 1; i >= 0; i-- {
		if history[i].Role != "user" {
			continue
		}
		for _, r := range history[i].Content {
			if unicode.Is(unicode.Han, r) {
				return "抱歉，我现在无法回答，请稍后再试。"
			}
		}
		break
	}
	return "Sorry, I can't answer that right now. Please try again."
}

// synthesizeChunk runs one chunk's TTS call in its own goroutine so it can
// overlap with the LLM still generating the rest of the reply.
//
// The semaphore is not an optimization, it's a fix. Unbounded, a single
// reply fired every chunk's synthesis at once — eighteen simultaneous
// provider WebSockets on one measured call — and they queued behind each
// other at the far end: three-character chunks took seven seconds, and one
// hit the provider's 20s idle timeout and was lost. Nothing was gained by
// it, either, since playback is strictly ordered: audio synthesized twelve
// chunks ahead just sits in a channel. Two in flight keeps the pipeline fed
// while leaving the *first* chunk — the only one whose latency the caller
// can feel — competing with almost nothing.
func (l *LiveCallController) synthesizeChunk(ctx context.Context, text string, sinkRate int, resultCh chan<- speechResult) {
	defer func() { <-l.ttsSem }()
	defer close(resultCh)

	// Re-checked after the wait: a barge-in may well have landed while this
	// chunk was parked on the semaphore.
	if ctx.Err() != nil {
		return
	}

	callCtx, cancelCall := context.WithTimeout(ctx, l.cfg.TTSTimeout)
	defer cancelCall()
	started := time.Now()
	var firstAudio time.Duration
	var resampler *audio.StreamResampler
	deliver := func(pcm []byte, sampleRate int) {
		if resampler == nil {
			firstAudio = time.Since(started)
			resampler = audio.NewStreamResampler(sampleRate, sinkRate)
		}
		select {
		case resultCh <- speechResult{samples: resampler.Push(audio.BytesToInt16(pcm))}:
		case <-ctx.Done():
		}
	}

	ttsCtx, ttsEnd := telemetry.StartStage(callCtx, "tts.synthesize", attribute.Int("text.chars", len([]rune(text))))
	var err error
	if tts, ok := l.deps.TTS.(provider.StreamingTTS); ok {
		err = tts.SynthesizeStream(ttsCtx, text, l.deps.Voice, deliver)
	} else {
		var pcm []byte
		var sampleRate int
		pcm, sampleRate, err = l.deps.TTS.Synthesize(ttsCtx, text, l.deps.Voice)
		if err == nil && len(pcm) > 0 {
			deliver(pcm, sampleRate)
		}
	}
	if err != nil && ctx.Err() != nil {
		ttsEnd(nil) // cancelled by a barge-in; not a real error
	} else {
		ttsEnd(err)
	}
	if err != nil {
		if ctx.Err() == nil {
			slog.ErrorContext(ctx, "live call: tts failed", "identity", l.identity, "chars", len([]rune(text)), "err", err)
		}
		return
	}
	slog.InfoContext(ctx, "live call: tts", "identity", l.identity, "chars", len([]rune(text)),
		"first_audio_ms", firstAudio.Milliseconds(), "total_ms", time.Since(started).Milliseconds())
}

// playSpeech drains speechQueue in submission order — the order chunks were
// generated in, not the order their TTS calls happen to finish in — writing
// each chunk's text to replySink and pushing its audio to sink.
//
// Two things here are deliberate and worth not undoing:
//
// Text is written *here*, next to the audio, rather than from the LLM
// callback as tokens arrive. Generation runs far ahead of playback — that's
// the whole point of pipelining — so a reply captioned at token speed was
// fully on screen while barely a sentence of it had been spoken, and a
// barge-in then left the caller looking at three paragraphs the agent never
// said. Captioning at playback speed costs a little of the "typing" feel and
// buys captions that are actually true.
//
// And each chunk waits for the sink's backlog to fall under MaxLeadMS before
// being handed over at all, rather than being dumped in as soon as TTS
// returns. That keeps the caption within MaxLeadMS of the audio, and keeps
// the amount of already-committed audio small, so a barge-in has little left
// to throw away. It can't starve the speaker: a chunk is released the moment
// there's room, and the very first chunk of a reply never waits.
func (l *LiveCallController) playSpeech(ctx context.Context, version uint64, queue <-chan pendingChunk, sink AudioSink, replySink ReplySink, spoken *strings.Builder, done chan<- struct{}) {
	defer close(done)

	maxLead := time.Duration(l.cfg.MaxLeadMS) * time.Millisecond
	var started bool
	for chunk := range queue {
		l.playChunk(ctx, version, chunk, sink, replySink, spoken, maxLead, &started)
		if ctx.Err() != nil {
			return
		}
	}
}

// playChunk plays one chunk's audio as it arrives. The chunk's text is released
// with its first audio: no audio, no text, since unspoken text must never
// become a caption or memory. Each piece rechecks the floor, so an interruption
// takes effect in the middle of a chunk still being synthesized.
func (l *LiveCallController) playChunk(ctx context.Context, version uint64, chunk pendingChunk, sink AudioSink, replySink ReplySink, spoken *strings.Builder, maxLead time.Duration, started *bool) {
	stale := func() bool { return ctx.Err() != nil || l.turnVersion.Load() != version }
	released := false
	samples := 0
	defer func() {
		if released {
			slog.InfoContext(ctx, "live call: spoken", "identity", l.identity, "turn_version", version, "text", chunk.text, "audio_ms", samples*1000/sink.SampleRate())
		}
	}()

	for {
		var res speechResult
		var ok bool
		select {
		case res, ok = <-chunk.result:
		case <-ctx.Done():
			return
		}
		if !ok {
			return
		}
		if len(res.samples) == 0 {
			continue
		}
		if !released {
			if stale() {
				return
			}
			waitSinkBelow(ctx, sink, maxLead)
		}

		// Admission and enqueue are serialized with interruption + Drain.
		// Otherwise a stale PushPCM can refill the sink just after a drain.
		l.mu.Lock()
		if stale() || l.inFlight.Load() > 0 {
			l.mu.Unlock()
			return
		}
		if !*started {
			*started = true
			l.speaking.Store(true)
			l.setStateLocked(StateSpeaking)
			slog.InfoContext(ctx, "live call: first audio", "identity", l.identity, "turn_version", version, "since_thinking_ms", time.Since(l.turnStarted).Milliseconds())
			if turn := l.current; turn != nil {
				_, turn.endPlayback = telemetry.StartStage(ctx, "playback")
				if !turn.speechEnded.IsZero() {
					turn.SetAttributes(attribute.Int64("turn.first_audio_ms", time.Since(turn.speechEnded).Milliseconds()))
				}
			}
		}
		sink.PushPCM(res.samples)
		samples += len(res.samples)
		if !released {
			released = true
			if replySink != nil {
				replySink.WriteToken(chunk.text)
			}
			spoken.WriteString(chunk.text)
		}
		l.mu.Unlock()
	}
}

// waitSinkBelow blocks until the sink has less than limit of audio left to
// play, or ctx is done. The sink's own writer keeps draining throughout
// (its lifetime is the call's, not any one turn's), so polling once a frame
// period is enough to notice it caught up.
func waitSinkBelow(ctx context.Context, sink AudioSink, limit time.Duration) {
	ticker := time.NewTicker(sinkPollInterval)
	defer ticker.Stop()
	for sink.QueuedDuration() > limit {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return
		}
	}
}

// preRoll keeps the most recent tail of inbound audio so that the frames
// which *caused* VAD to declare speech can be replayed into the STT stream
// opened afterwards.
//
// Without it the recognizer never hears the beginning of anything: an
// energy VAD only reports EventSpeechStart once MinSpeechMS of loud audio
// has already gone by, and the loop used to open the stream at that point
// and start pushing from the next frame — so the first fifth of a second of
// every utterance, which is to say its first syllable, went in the bin
// before transcription began.
type preRoll struct {
	buf []int16
	max int
}

func newPreRoll(ms, sampleRate int) *preRoll {
	return &preRoll{max: ms * sampleRate / 1000}
}

func (p *preRoll) add(frame []int16) {
	if p.max <= 0 {
		return
	}
	p.buf = append(p.buf, frame...)
	if len(p.buf) > p.max {
		p.buf = append(p.buf[:0], p.buf[len(p.buf)-p.max:]...)
	}
}

// take returns the buffered lead-in and empties the buffer.
func (p *preRoll) take() []int16 {
	if len(p.buf) == 0 {
		return nil
	}
	out := make([]int16, len(p.buf))
	copy(out, p.buf)
	p.buf = p.buf[:0]
	return out
}

func (p *preRoll) reset() { p.buf = p.buf[:0] }
