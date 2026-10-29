# Silence is the failure mode

Live-call mode is a fight over one shared floor in real time — see
[`LIVE-CALL-UX.md`](LIVE-CALL-UX.md). Text and voice-message mode never
have that fight: nobody is on an open line waiting to be interrupted, so
there is no floor to hold or hand back. That looks like it should make the
UX problem easier, and for the parts that are just "stream the tokens as
they arrive" it does. But removing the floor also removes the one thing
that used to prove the agent was still there. On a call, dead air is
instantly obvious — silence on an open mic is unmistakably broken. Over a
text-stream or a byte-stream, dead air looks exactly like "still thinking."
That's the throughline below: every gap in this document is some version of
the caller not being able to tell those two apart.

## Current behavior (September 13, 2026)

The original audit below identified four real gaps. The implementation now
addresses them as follows:

- **Visible progress and completion.** Every admitted message opens an ordered
  JSONL stream on `agent.message.status`: `queued`, then `thinking` for text or
  `transcribing` → `thinking` → `audio_ready` for voice, and finally `done` or
  `error`. Queue/upload/provider work has a 50-second deadline. The client keeps
  its 60-second fallback for a missing agent or broken connection.
- **Visible failures.** Invalid/empty input, STT/LLM/TTS failure, empty model
  output and deadlines produce a user-facing error. Provider details stay in
  logs. A partial text reply is retained in history with `[incomplete]`.
- **One message turn per identity.** The transport reserves a FIFO position on
  handler admission, before reading the upload. Text and voice share this queue;
  another identity remains independent. The engine also serializes direct calls,
  including the full voice transcription/synthesis turn. Canceled queue entries
  cannot let later work bypass an active predecessor. Admission order is the SDK
  callback order, not a guarantee about two simultaneous sends' network order.
- **Incremental voice output.** A bounded speech-chunk queue feeds one TTS worker
  while the model continues generating. The first sentence can reach the caller
  before the LLM finishes. Each chunk is a complete mono PCM16 WAV, sent on
  `agent.voice` with a zero-based `chunk_index`. Text still streams immediately
  for reading; it is not paced to speaker playback. Batch STT and per-chunk batch
  TTS remain, so this does not promise live-call latency or gapless playback.

Every request/reply/status stream carries `request_id` (the incoming stream ID
is the fallback for older senders). The terminal status includes `audio_chunks`.
The interactive client matches replies by ID, orders audio by chunk index, and
waits for terminal success, the text stream, and—for voice—the transcript and
all audio chunks. A late reply cannot complete a newer exchange. The integration
harness assembles the chunks back into one output WAV. **Update agent and client
together:** old receivers that expect one WAV will only receive part of a reply.

Message writers wait for the SDK's write callback before closing. In the pinned
SDK, `Write` only queues data and immediate `Close` can discard it. This also
applies to the interactive client's voice upload. A completed SDK write is not
an application delivery acknowledgment; missing data still hits the client timeout.
The SDK reader has no public cancellation method; a stalled upload stops holding
its turn at the deadline, though its reader goroutine waits until the SDK ends
that stream. Concurrent live calls still use their own floor controller; this
message queue does not arbitrate between an active live call and typed messages.

Validation:

```sh
# From the workspace root:
make check
make race
# Against local LiveKit, with deterministic providers and no audio device:
(cd livekit-agent && T10_MESSAGE_SMOKE=1 go test -race ./internal/livekit -run TestMessageRoomSmoke)
(cd mac-client && T10_MESSAGE_SMOKE=1 go test -race ./internal/room -run TestMessageSessionRoomSmoke)
```

The engine regressions cover first audio before LLM completion, transcript before
LLM, mixed text/voice history, independent identities, canceled queue entries,
empty output and partial failure. Room tests cover actual SDK delivery, malformed
WAV and provider errors, plus terminal status arriving before text/audio and audio
chunks arriving out of order. These checks establish behavior, not real-provider
latency or microphone/speaker quality.

## Original audit (before these fixes)

The remaining sections preserve the original rationale and describe the old
behavior. Voice orchestration now lives in `internal/engine/voice.go`
(`RunVoiceTurn`); the transport owns WAV conversion and room delivery. Historical
line numbers below refer to the earlier layout. Current traces are in Grafana/
Tempo at http://localhost:3002; the Jaeger commands at the end are historical.

For *how* either mode is wired, see `DESIGN.md` §4's mode table; this
document is the why.

---

## The one rule

> **A message sent gets an answer — in words, or in a reason there aren't
> any. Nothing in between is allowed to look the same as broken.**

---

## Text message: two moments

Mechanically the simplest of the three modes: `text_mode.go`'s stream
handler reads one message off `agent.chat` and calls
`engine.RunChatTurn`, which appends it to the caller's `Conversation`,
streams the model's reply back token-by-token on the same topic, and
appends the finished reply to history (`chat.go`).

### 1. You send it

*Should feel like: it left, and something already started on a reply.*

`SendText` is fire-and-forget from the caller's side, so that half is fine
by construction. What happens next is where the moment actually lives: the
handler spins up a goroutine (`text_mode.go:42`) and calls the LLM. Between
that call starting and its first token coming back, the caller sees
*nothing*. Live-call mode publishes `agent.state = thinking` for exactly
this gap; text mode has no equivalent message on any topic, so a
three-second reply and a hung provider call look identical until one of
them ends.

### 2. It answers

*Should feel like: watching it get typed, not handed a finished page.*

This part is already right. `RunChatTurn` passes `sink.WriteToken` as the
`ChatStream` callback (`chat.go:23`), so every delta the model produces goes
out on `agent.chat` the moment it exists — the same shape a human typing a
reply would produce, not a wall of text that appears once generation
finishes. `cmd/testclient text` renders this with a plain `io.Copy` to
stdout and gets a live-typed effect for free.

What's missing is the failure path. If `ChatStream` errors, `RunChatTurn`
returns it, `text_mode.go:48` logs it server-side, and the stream closes
having written nothing at all. The caller's screen shows whatever it showed
before sending — no error, no partial answer, no sign anything happened.
`cmd/testclient text` waits out a 60-second timeout and prints "(timed out
waiting for a reply)"; a real client without that fallback has nothing to
show at all.

---

## Voice message: three moments

`voice_mode.go`'s `handleVoiceMessage` does more work per call: decode the
uploaded WAV, transcribe it, run the same `RunChatTurn` text mode uses, then
synthesize the reply back to speech.

### 1. You hand it over

*Should feel like: it's uploaded, on its way.*

The upload blocks on the client until the byte-stream finishes, the same as
handing someone a recording in person. There's no acknowledgment of receipt
separate from what happens next — the first visible sign of life is the
transcript, whenever `TranscribeBatch` finishes.

### 2. It tells you what it heard

*Should feel like: proof it listened, arriving before it's decided what to
say back.*

This is the best-designed moment either mode has, and it's inherited
straight from live-call mode's own rule that a transcript is not the reply
and shouldn't wait on one (`DESIGN.md` §4's `OnTranscript`). The handler
sends the transcript on `agent.transcript` (`voice_mode.go:73`) *before*
calling `RunChatTurn` at all, so the caller learns whether they were heard
correctly seconds before they'd otherwise learn anything. And when
`TranscribeBatch` comes back empty, the code doesn't drop the turn or send
nothing — it substitutes the literal string `"(unintelligible)"`
(`voice_mode.go:68-70`) and answers *that*, so the caller sees the failure
instead of a reply that quietly doesn't match what they said.

The gap: `TranscribeBatch` is one blocking call over the *entire*
recording — there's no live-call-style interim transcript or per-utterance
commit to shrink the wait. A ten-second voice message pays for something
like ten seconds of silence before anything appears at all, same problem as
moment 1 in text mode.

### 3. It answers — eventually, all at once

*Should feel like: an answer arriving, not a file arriving.*

What actually happens is the opposite of every design decision
`LIVE-CALL-UX.md` and `DESIGN.md` §6 make about live-call audio. There, a
reply is chunked sentence-by-sentence specifically so time-to-first-audio
doesn't have to pay for the whole answer. Here, `handleVoiceMessage` waits
for `RunChatTurn` to finish completely — the full reply text, fully
generated — and only then makes one `TTS.Synthesize` call over the entire
string (`voice_mode.go:85`), encodes one WAV, and writes it as a single
byte-stream message. A caller waits for STT-of-everything, then
LLM-of-everything, then TTS-of-everything, serially, with none of it
pipelined. And because the text reply already streamed out on `agent.chat`
while the audio was still being synthesized, a caller reading along gets
the answer in words before the clip they're waiting for even starts. The
audio isn't just slow; by the time it lands it's telling the caller
something they were already shown.

Errors here fail the same silent way text mode's do: a bad WAV, a failed
transcription, or a failed synthesis is logged (`voice_mode.go:45`) and
nothing else happens. Unlike a failed text reply, which at least leaves an
empty stream that closed, a voice-message failure partway through can leave
the caller having *seen* a transcript and a full text reply with no audio
ever arriving to match either one.

---

## What text and voice message share

Both run through the same `engine.RunChatTurn` and the same `Conversation`
per identity (`conversation.go`), which is what makes "type a question,
then send a voice-message follow-up" work at all — the model sees one
history regardless of which topic produced which line.

That sharing has a cost neither mode currently pays for. `Conversation`'s
`Append`/`Snapshot` are mutex-protected against races on the slice itself,
but nothing serializes *turns* for one identity the way live-call's
`waitForQuiet`/`coalesce` do. Every incoming message — text or voice — spawns
its own goroutine that calls `RunChatTurn` independently. Two messages in
flight for the same caller — a fast second text before the first reply
lands, or a text sent while a voice-message reply is still generating — run
as two unrelated goroutines racing to call the LLM and append to history.
Whichever call returns first appends its `assistant` line first, so history
can end up ordered user-1, user-2, assistant-2, assistant-1: the model's own
memory of "what did I just say" stops matching what the caller actually
saw. It doesn't take a deliberate double-send to trigger — a caller
double-tapping send while waiting out the state indicator that doesn't
exist (moment 1, both modes) is the obvious way to hit this by accident.

---

## Where it falls short

Four things, ranked by how much they change what the caller thinks is
happening:

| Gap | What the caller experiences |
|---|---|
| No progress signal between send and first output, either mode | A three-second wait and a hung provider call are indistinguishable until one of them ends. |
| Errors are logged, never surfaced | A failed reply looks exactly like an agent still working — no error message exists on any topic. |
| Turns for one identity aren't serialized | Two messages sent close together can be answered, and remembered, out of order. |
| Voice-message reply is unchunked, and only synthesized after the full text reply exists | The caller's wait is the sum of three whole-answer stages, and the audio can arrive after the text already gave the answer away. |

None of these is wrong the way a live-call floor violation is wrong — a
slow or silent-on-error text reply doesn't step on anything the way
speaking over a caller does. But the discipline `LIVE-CALL-UX.md` argues
for — that the caller should never have to guess what state the agent is
in — applies just as much to someone staring at an unchanging screen as to
someone on an open mic.

---

## How to check any of this

Every stage named above is already its own span:
`stt.transcribe_batch`/`llm.chat_stream`/`tts.synthesize` in
`voice_mode.go`, `llm.chat_stream` alone in `chat.go`. Run the stack and
drive one message through each mode:

```bash
docker compose up -d
go run ./cmd/testclient text  -msg "hello"
go run ./cmd/testclient voice -file sample.wav -out reply.wav
```

then pull the trace by the `correlation_id` the agent logs for that
request, the same way `LIVE-CALL-UX.md`'s own "How these numbers were
taken" does:

```bash
curl -s "http://localhost:16687/api/traces/<correlation_id>"
```

Unlike the live-call numbers, nothing above is quoted from a real trace —
there's no floor-timing budget to hit here, so the useful thing to look at
isn't a target number but which of the three stages dominates a given
reply, and whether it's the one the caller is actually waiting on.
