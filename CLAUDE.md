# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This is the **livekit-agent** subproject of the `t10` workspace — an
independent Go module and git repo. A sibling `mac-client/` repo (see its own
`CLAUDE.md`) is a real mic/speaker LiveKit participant that talks to this
agent; the two share no code (separate Go modules can't import each
other's `internal/` packages) but do share the same LiveKit room/server.

## What this is

A small, runnable voice agent built around the STT→LLM→TTS pattern on top of
[LiveKit](https://livekit.io). It's a deliberately simplified Go sibling of a
larger Python project (`livekit-agent`, a sales-CRM voice assistant) — all
business logic is dropped, keeping only the generic, reusable shape:
provider-agnostic STT/LLM/TTS, a turn/conversation model, and a VAD-driven
live-call loop with barge-in.

**Read `docs/DESIGN.md` first.** It documents the architecture, the
DashScope/Qwen realtime wire protocol (reverse-engineered, undocumented at
this level of detail), the live-call turn-loop sequence, and the deliberate
simplifications vs. the Python original in more depth than is worth
duplicating here.

## Commands

Building on the host requires `libopus`, `libopusfile`, and `pkg-config`
(cgo binding for Opus): `brew install opus opusfile pkgconf` on macOS. The
Docker build (below) installs its own Debian equivalents, so this is only
needed for `go build`/`go test` on the host.

```bash
go build ./...                       # build everything
go vet ./...                         # vet
go test ./...                        # run all tests
go test ./internal/engine/...        # test one package
go test ./internal/engine/ -run TestLiveCallController_BargeIn  # single test
```

Running the full stack (LiveKit dev server + agent + observability) via
Docker Compose:

```bash
cp .env.example .env   # fill in DASHSCOPE_API_KEY, OPENAI_*; LiveKit devkey/secret already match docker-compose.yml
docker compose up --build

# in another terminal — testclient always runs on the host, exercising each mode:
go run ./cmd/testclient text  -msg "hello"
go run ./cmd/testclient voice -file sample.wav -out reply.wav
go run ./cmd/testclient live  -file sample.wav -out reply.wav \
    -interrupt-file interrupt.wav -interrupt-after 10
```

This also brings up `otel-lgtm` (Grafana, traces, metrics and logs) — see
"Observability" below. The containerized `agent` always exports to them;
`go run ./cmd/agent` on the host only does if `OTEL_EXPORTER_OTLP_ENDPOINT`
is set in `.env` (see `.env.example`).

To iterate on the agent without a container rebuild per change, run only the
LiveKit server via Compose and the agent on the host:

```bash
docker compose up livekit-server
go run ./cmd/agent -room demo   # separate terminal
```

## Architecture

Primary runtime dependency direction:
`internal/livekit` → `internal/engine` → `internal/provider`.

- **`internal/provider`** — capability interfaces (`STTProvider`,
  `LLMProvider`, `TTSProvider`) plus a name→factory registry
  (`RegisterSTT`/`RegisterLLM`/`RegisterTTS`, `registry.go`). Concrete
  backends live in subpackages (`qwen/`, `openaicompat/`) and self-register
  via `init()` — the `database/sql` driver pattern. `cmd/agent/main.go`
  blank-imports whichever backends should be linked in; the actual choice is
  a `*_PROVIDER` env var read through `internal/config`.
- **`internal/engine`** — the transport-agnostic pipeline: per-participant
  conversation history (`conversation.go`), a one-shot chat turn runner
  (`chat.go`: `RunChatTurn`), a voice-message runner (`voice.go`: `RunVoiceTurn`), and the live-call turn loop (`live_turn.go`:
  `LiveCallController`). **Zero imports of LiveKit, WebRTC, or Opus** — it
  uses provider interfaces, pure-Go audio utilities, telemetry, and its own small ports in `ports.go`
  (`ReplySink`, `AudioSource`, `AudioSink`). This boundary is what lets
  `live_turn_test.go` drive the whole VAD→STT→LLM→TTS→barge-in state machine
  with fakes and no network/cgo.
- **`internal/livekit`** — the server adapter that imports the LiveKit SDK,
  pion/webrtc, or the Opus codec (`internal/audio/opuscodec`, cgo). It joins
  a room, turns LiveKit's text-streams/byte-streams/tracks into calls into
  `engine`, and implements `engine`'s ports so the engine never has to know
  a room exists. `agent.go`'s `RoomCallback.OnTrackSubscribed` is what arms
  live-call mode — any subscribed audio track starts a `LiveCallController`.

- **`internal/roomprotocol`** — SDK-free topics and stream attributes, used by
  the adapter and testclient. Workspace checks compare the client copy.
- **`internal/roomauth`** — development token minting, used by cmd/agent and
  cmd/testclient. A future auth project will own production admission/token APIs.
- **`cmd/testclient`** — server integration harness; imports the contract, auth
  helper and audio utilities, never the server adapter or engine.

All three conversation modes (text, voice-message, live call) run over one
LiveKit room per conversation, sharing a single per-identity
`engine.Conversation` so context carries across modalities.

### Three extensibility axes

- **Model providers**: implement one of the three interfaces in
  `internal/provider/{stt,llm,tts}.go`, register it via `provider.RegisterXxx`
  in an `init()` in a new subpackage, blank-import that subpackage from
  `cmd/agent/main.go`, point the matching `*_PROVIDER` env var at the
  registered name. The LLM side needs even less: `internal/provider/openaicompat`
  works against any OpenAI-compatible endpoint, so swapping models is often
  just an `.env` change (`OPENAI_BASE_URL`/`OPENAI_API_KEY`/`OPENAI_MODEL`).
- **Transport**: implement `engine.ReplySink` (text) and
  `engine.AudioSource`/`engine.AudioSink` (live audio) the way
  `internal/livekit` does, translate the transport's native messages into
  `engine.RunChatTurn`/`engine.LiveCallController.Run` calls.
  `internal/engine` needs no changes at all.
- **Voice Activity Detection (VAD)**: implement `engine.VADProcessor`
  (`internal/engine/ports.go`) and provide it via `engine.LiveConfig.VADOverride`.
  The default is energy-based (`internal/audio/vad.go`), and neural models
  (Silero, WebRTC VAD) or test fakes can be plugged in seamlessly.

### Live-call turn loop essentials

`LiveCallController` (`internal/engine/live_turn.go`) is the most involved
piece — see `docs/DESIGN.md` §6 for the full picture, and
`docs/LIVE-CALL-UX.md` for the experience it's built to produce (including
seven invariants a change here must not break, and how to re-measure any of
this). Most of what's in there is shaped by measurements from real calls, so
before changing any of it, check the trace first (§"Observability" below):
every claim below has a number behind it.

- **Three stages, three goroutines.** `Run` (real-time audio, must never
  block) → commit worker (one utterance's `STT.Commit`, sub-second) → turn
  worker (LLM+TTS+playback, tens of seconds, strictly serial). Don't merge
  the last two back together: when they shared a worker, a transcript sat
  behind the previous reply's whole playback and reached the caller's screen
  up to 45s late.
- **Turn-taking.** `waitForQuiet` keeps a turn from starting while the caller
  is still talking or still being transcribed; `coalesce` then answers
  everything that piled up as one turn. Together they make a mid-thought
  pause survivable instead of producing a reply to half a sentence.
- **`preRoll`** replays the audio from *before* VAD declared speech into the
  STT stream. Without it the recognizer never gets the first syllable.
- **Barge-in via a turn-version counter**: new speech bumps `turnVersion`
  unconditionally (an interruption during the STT commit has no reply to
  cancel yet, but still has to invalidate the turn about to start), cancels
  the in-flight turn's `context.Context`, and calls `sink.Drain()`. Anything
  holding a stale version stops touching the sink. `TopicState`'s
  `interrupted` message does the part `Drain()` can't: tell the client to
  drop the audio already in *its* buffers.
- **Chunking and TTS concurrency** (`sentence.go`'s `speechChunker`, plus
  `LiveConfig.TTSConcurrency`). One TTS call per sentence, all fired at once,
  put 18 concurrent provider WebSockets in flight on one measured reply;
  three-character chunks took 7.7s and one hit the provider's 20s timeout.
  Batch later chunks, cap concurrency, and let the *first* chunk skip the
  minimum so time-to-first-audio doesn't pay for it.
- **`playSpeech` paces the captions to the audio**, not to the model, and
  holds each chunk until the sink's backlog drops under `MaxLeadMS`. Both
  look like inefficiency and are not — see the comments on the function.

### Correlation IDs, tracing, and metrics

`internal/telemetry` gives every client-initiated request or session (one
text/voice message, or one live call) a correlation ID and threads it on
`context.Context`, using OpenTelemetry's trace-ID type/propagation model
rather than a bespoke scheme. `telemetry.StartSession(ctx)` is called once
per request/session in `internal/livekit` (`text_mode.go`, `voice_mode.go`,
`agent.go`'s `OnTrackSubscribed`) and opens a root `"session"` span; every
context derived from the result — including `LiveCallController`'s per-turn
contexts in `internal/engine/live_turn.go` — carries the same ID. Log with
`slog.InfoContext`/`slog.ErrorContext` (not the plain `slog.Info`/`Error`)
so it gets attached automatically: `cmd/agent/main.go` wraps the default
handler in `telemetry.NewLogHandler`, which reads the ID off the context and
adds it as a `correlation_id` attribute. See `docs/DESIGN.md` §7 for the
full rationale.

`telemetry.StartStage(ctx, name, attrs...)` starts a child span for one
STT/LLM/TTS provider call or one live-call turn, timing it; the returned
`end(err)` func closes the span (recording the error, if any) and records
the stage's duration/outcome as OTel metrics in one call — see
`engine/chat.go`, `engine/live_turn.go`, and `engine/voice.go` for the
call sites. `stage="turn"` spans a whole live-call turn *including playback*,
so its duration is how long the agent actually held the floor — the single
most useful number when a call felt like being talked at rather than talked
with. Both the trace ID scheme and these spans/metrics are no-ops
until `telemetry.SetupOTel` is called (see `cmd/agent/main.go`, gated on
`OTEL_EXPORTER_OTLP_ENDPOINT`) — every call site works identically, and
`CorrelationID` keeps returning the same ID either way, whether or not an
OTLP endpoint is configured.

### Observability

`docker-compose.yml` runs `otel-lgtm` (`grafana/otel-lgtm:latest`), an
all-in-one container bundling the OpenTelemetry Collector, Tempo (traces),
Loki (logs), Prometheus/Mimir (metrics), and Grafana (unified UI at
http://localhost:3002) alongside the agent. The containerized agent always
exports to `otel-lgtm:4317` (also aliased as `otel-collector:4317`); the
client (a separate, host-run module — see `../mac-client/CLAUDE.md`) exports to
the same endpoint over its published host port (`localhost:4317`) via
`OTEL_EXPORTER_OTLP_ENDPOINT`.

Grafana at http://localhost:3002 is pre-configured with all data sources:
- **Loki** for Logs (queryable via LogQL, e.g. `{service_name="t10-agent"}`)
- **Tempo** for Traces (searchable by service, operation, or trace ID)
- **Prometheus** for Metrics (`t10_stage_calls_total`, `t10_stage_duration_milliseconds_*`)
- **Bidirectional Correlation**: Clicking a `trace_id` in Loki opens the trace in Tempo, and Tempo spans link back to their corresponding logs in Loki.

`tts.synthesize` spans additionally carry `tts.first_delta_ms` and
`tts.total_ms` (set from `internal/provider/qwen/tts.go` onto the caller's
span). The gap between the two is latency the batch-shaped
`TTSProvider.Synthesize` throws away — nothing can be played until it
returns — and the gap between `tts.total_ms` and the span's own duration is
the per-call WebSocket dial and session setup. Measured on a real reply those
were ~1.1s and ~1.0s on the *first* chunk, i.e. about two seconds of the
caller's wait, which makes streaming synthesis and connection reuse the two
obvious next levers on time-to-first-audio.

A trace ID printed in a log line's `correlation_id` / `trace_id` field is the exact
trace ID for that request/session/call — paste it into Tempo or click the trace link
in Grafana to see the STT/LLM/TTS timing breakdown for whatever went wrong or slow.
The `t10.stage.calls`/`t10.stage.duration` metrics (Prometheus: `t10_stage_calls_total`,
`t10_stage_duration_milliseconds_*`), tagged by `stage` and `error`, are what to graph
for trends across many requests rather than one.

### Provider registry configuration

Provider selection and all tunables are env vars loaded via
`internal/config` (`.env` file first, then process env — real env vars are
never overridden). See `.env.example` for the full list: LiveKit connection,
`{STT,LLM,TTS}_PROVIDER` registry keys, DashScope/Qwen model+voice settings,
OpenAI-compatible endpoint settings, VAD tuning
(`VAD_ENERGY_THRESHOLD`/`VAD_MIN_SPEECH_MS`/`VAD_MIN_SILENCE_MS`/
`VAD_BARGE_IN_MIN_SPEECH_MS`), and the live-call pipeline knobs
(`LIVE_PREROLL_MS`, `LIVE_MAX_LEAD_MS`, `TTS_CONCURRENCY`, `TTS_TIMEOUT_MS`,
`LIVE_MIN_CHUNK_RUNES`, `LIVE_MAX_CHUNK_RUNES` — all mapped onto
`engine.LiveConfig`, which fills in defaults for anything left unset).

The `-system-prompt` flag defaults to a prompt written specifically for the
live-call case (`cmd/agent/main.go`). Keeping replies to a couple of spoken
sentences is a turn-taking property, not a style preference: a model that
answers at length holds the floor for most of a minute, and the caller's only
way out is to interrupt it.
