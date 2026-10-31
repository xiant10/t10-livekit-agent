# agent-pi LLM provider: design

Status: implemented. Tested through LiveKit with `cmd/testclient`, simulated
microphone audio, and a real model (`qwen-plus`). Not yet tried by a person on a
call. Date: October 30, 2026.

Adds an `agent_pi` LLM provider. Each message is one Streamable HTTP request
to agent-pi, answered by a stream. A message that arrives while the agent is
working **interrupts** it by default, except that running tools finish first.
`AGENT_PI_WHEN_BUSY` can change this to `interrupt`, `steer` or `followUp`.

## Summary

- **The protocol can be reused.** `POST /agent` with `session.interrupt` covers
  both the first and the second message, and agent-pi already streams it as SSE.
  On an idle session it starts a run; on a busy one it aborts the work and answers
  the new input. The provider never has to know which, so there is no busy-check
  race. `session.prompt` with `whenBusy` covers the other two modes the same way.
- **Two small agent-pi changes are needed.** A stream does not name its own
  submission until its final message, and every open stream receives every
  session event, so a new `Agent-Submission-Id` response header closes that gap.
  `session.interrupt` also gains `duringTools: "steer"`, because after a barge-in
  only the server knows whether tools are running.
- **`LLMProvider` stays as it is.** The session and an "admitted" callback travel
  on `ctx`. `openaicompat` behaves exactly as it does today.
- **The turn model changes in three ways:** agent-pi owns the conversation, a
  second message must not wait in the Go FIFO, and an interrupted reply needs an
  explicit correction, because the model never sees an aborted answer.

## What exists today

**Go side.** `LLMProvider.ChatStream(ctx, msgs, onToken)` is stateless: every
call carries the full history, and cancelling `ctx` ends the work. Two things
stand in the way of steering:

- Text and voice messages queue behind each other. `text_mode.go` and
  `voice_mode.go` hold a FIFO position, and `RunChatTurn`/`RunVoiceTurn` hold a
  second one. Both are held for the whole reply, so a second message reaches the
  LLM only after the first has finished, whichever mode is configured.
- A live barge-in cancels the turn at speech start, before the transcript
  exists. The new words arrive about a second later as a new `ChatStream` call.

**agent-pi side.** Sessions are stateful and durable, and the server owns the
transcript, system prompt, tools, and model. The three modes differ when the
agent is already working:

| Agent is... | `interrupt` | `steer` | `followUp` |
|---|---|---|---|
| Generating text | Aborts now, answers the new input | Waits for the answer, then a new run | Same as `steer` |
| In a tool round | Aborts the tools, answers the new input | Placed after the round; one run answers both | Waits for the whole run |

With `steer`, `steeringMode` is `one-at-a-time`, so several queued steers are
placed one per boundary, and each gets its own answer.

## Measured behaviour

The steps below come from the real agent-pi harness, at the versions in its
lockfile (pi-durable 1.0.2), with a scripted model. The claims about ordering
in this document rest on these runs, not on the docs.

**Real model, second message during a long answer.** Time from sending the second
message to its first token, on `qwen-plus`, one run each:

| Mode | First token | First reply |
|---|---:|---|
| `interrupt` | 1.0 s | Stopped at 76 characters |
| `steer` | 18.3 s | Finished, 1,120 characters |
| `followUp` | 17.4 s | Finished, 1,086 characters |

**Interrupt during a text answer.** A's stream ends as `aborted`. B's stream
still carries A's last events before its own, so B must wait to be placed:

```
A/B message.end  "First answer"  stopReason=aborted
A/B run.finished [13]
A/B submission   13 unanswered aborted
A   RESULT       13 unanswered aborted, EOF
  B submission   19 placed                       <- handover point
  B run.started  [19]
  B text.delta   "Second."
  B RESULT       19 done, EOF
```

**What the model then sees.** The aborted answer is not there:

```
[user "one"], [user "two"]
```

pi-durable keeps the aborted entry in storage but excludes it from model context
(`EXCLUDED_STOP_REASONS` in `harness/context.js`). agent-pi's `design.md` says the
partial answer stays "so the model knows what it had already said". That is true
of storage only, and that doc should be corrected.

**Interrupt during a tool round.** The tool is reported as failed. Text from
before the tool survives, because that message did not stop with `aborted`:

```
[user "one"], [assistant "Let me check." + tool call],
[toolResult "Tool crm_slow was aborted"], [user "two"]
```

**Interrupt on an idle session.** Behaves exactly like a prompt.

**Three messages in a row, sent one after another.** Each interrupts the previous
one, nothing is lost, and only the last is answered:

```
[user "one"], [user "two"], [user "three"]
```

**Two interrupts at the same moment.** The second one's abort lands before the
first is placed, so it finds nothing to stop and queues as a follow-up. Both get
answered, so **admissions must be serialized per session**.

**Steer during a text answer.** B receives the tail of A's answer before its own:

```
A/B text.delta   "...r a while."                 <- A's text, also sent to B
A/B message.end  "First answer..."
A/B run.finished [13]
A/B submission   13 done
A/B submission   17 placed                       <- handover point
A/B run.started  [17]
A   RESULT       13 done, EOF
  B text.delta   "Second."
  B RESULT       17 done, EOF
```

**Steer during a tool round.** The run is joined, and no second `run.started`
appears:

```
A   text.delta   "Let me check."
A/B tool.finished
A/B submission   19 placed                       <- handover point
A/B text.delta   "Joined answer."
A/B run.finished [13, 19]
A/B RESULT       same text on both streams
```

**Steer behind a failed run.** B never ends. Durable leaves the queued item in
the inbox until the next submission arrives (the stream was still open when the
probe stopped watching it at 6 s):

```
A/B run.finished [13]
A/B submission   13 unanswered model_error
A   RESULT, EOF
  B ...nothing further
```

`session.cancel` followed by a new `session.prompt` with a fresh `requestId`
recovers it. B's stream ends with `unanswered/aborted`, and the new submission
is answered normally. Tested.

**A client disconnects.** The run continues, and queued steers are unaffected.

## Protocol verdict

| Part | Verdict |
|---|---|
| `POST /agent`, JSON-RPC, SSE framing, bearer auth | Reuse |
| `session.create`, with `metadata` | Reuse |
| `session.list` | Reuse, to find a session again after a Go restart |
| `session.interrupt` | Reuse, for every message in the default mode |
| `session.prompt` with `whenBusy` | Reuse, for every message in `steer` and `followUp` |
| `session.cancel` | Reuse, for failed-run recovery in `steer` and `followUp` |
| `text.delta`, `submission`, the final JSON-RPC result | Reuse |
| `requestId`, `turnId` | Reuse |
| `session.steer` | **Do not use.** It answers JSON only, with no stream |
| `speech` events | **Do not use.** `speechChunker` stays the only chunker, since its thresholds are tuned to measured TTS behaviour |
| `GET /agent` event stream | Not needed |
| Submission id at stream start | **Missing: the one server change** |

### The server change

`streamRun` in `transport/streamable-http.ts` creates the `SseStream` after
admission, so it already knows the id:

```ts
export const SUBMISSION_HEADER = "agent-submission-id";

const stream = new SseStream(res, options.keepAliveMs, {
	[SESSION_HEADER]: sessionId,
	[SUBMISSION_HEADER]: String(ours),
});
```

Also add it to `access-control-expose-headers` in `http.ts`, document it in the
README, and cover it with a test. The change is additive. The plugin-reply path
sends no header, which is how the client recognizes that path.

A client cannot work this out for itself. The first `submission` event on a new
stream can belong to an earlier message that is just being placed, so guessing
would misattribute it.

## Design

### Package and config

New package `internal/provider/agentpi`, registered as `agent_pi` and
blank-imported from `cmd/agent/main.go`.

| Env var | Default | Meaning |
|---|---|---|
| `LLM_PROVIDER` | `openai_compat` | Set to `agent_pi` to enable |
| `AGENT_PI_URL` | `http://localhost:9800/agent` | Endpoint |
| `AGENT_PI_TOKEN` | | Bearer token, one service identity |
| `AGENT_PI_PROFILE` | `voice` | Profile for new sessions |
| `AGENT_PI_WHEN_BUSY` | `auto` | `auto`, `interrupt`, `steer`, or `followUp` |

`AGENT_PI_WHEN_BUSY` applies to live calls and to text and voice messages alike.
An unknown value fails at startup.

| Mode | Request sent for every message |
|---|---|
| `auto` | `session.interrupt` with `input`, `duringTools: "steer"` |
| `interrupt` | `session.interrupt` with `input` |
| `steer` | `session.prompt` with `input`, `whenBusy: "steer"` |
| `followUp` | `session.prompt` with `input`, `whenBusy: "followUp"` |

### Passing turn context

`ChatStream` keeps its signature. A new `internal/provider/turn.go` adds two
`ctx` helpers:

```go
// WithSession names the conversation a stateful provider should continue.
func WithSession(ctx context.Context, id string) context.Context

// WithAdmitted registers fn to run once the provider has durably accepted the
// input. A provider that never calls it leaves today's behaviour in place.
func WithAdmitted(ctx context.Context, fn func()) context.Context

// ErrSuperseded: a newer message took over this turn's output. Not a failure.
var ErrSuperseded = errors.New("provider: superseded by a newer message")
```

Only one interface exists to change, but several test fakes implement it, and
stateless providers have no use for a session id. `ctx` costs them nothing.

The engine sets the session at both `ChatStream` call sites (`chat.go`,
`live_turn.go`) using the participant identity.

### Letting a second message through

Both FIFO layers release **at admission or at the end of the turn, whichever
comes first**. The order messages enter the session stays FIFO. Only the wait
for the previous reply goes away. Every mode needs this, or an interrupt would
arrive after the reply it was meant to stop.

- `openaicompat` never signals admission, so it stays strictly serial and
  nothing changes for it.
- `close(done)` in `ReserveTurn` would panic if called twice, so the release
  function must become idempotent.
- Voice messages keep their place through STT, so a typed message cannot
  overtake a voice message that is still being transcribed.

### Session mapping

`Manager` keeps one session per identity, in memory:

```go
type session struct {
	mu     sync.Mutex // serializes admissions and guards the fields below
	id     string
	sent   int        // how far into the msgs snapshot has been submitted
	active *call      // the call owning the output, if its stream is still open
}
```

`mu` is held until the input is admitted, so the next interrupt always finds the
previous input placed and stops it.

On first use, the provider calls `session.list` and looks for an entry whose
`metadata.identity` and profile match; otherwise it calls `session.create`.
Reuse matters because all identities share one token and `maxSessionsPerUser`
defaults to 20: creating a session on every start would exhaust the limit after
a few restarts. On a 404, it recreates the session once.

This has a visible effect: **conversations survive a Go restart**, because
agent-pi remembers them even though the Go history is gone.

### Building the input

agent-pi owns the transcript, so the provider sends only what it has not sent:
the `user` messages in `msgs[sent:]`, joined by newlines. `system` messages are
ignored, because the prompt comes from the agent-pi profile.

A cursor is needed because "the trailing user messages" is wrong in two common
cases. An interruption before anything was spoken appends no assistant message,
and with steering a second message arrives before the first has any reply. Both
would resend the first message.

`sent` advances only after a successful admission. A refused message (429, 409)
therefore stays pending and goes out with the next turn, so no words are lost.

### Keeping what the caller heard

The model never sees an aborted answer. Left alone, an interrupt therefore breaks
a rule in `LIVE-CALL-UX.md` directly: "Never let the model think it said nothing."
The agent would restart the sentence it was cut off in. So this correction is
required, not optional.

The provider prefixes the next input with what was actually delivered:

```
[Your previous reply was cut off. The caller heard at most: "杭州是浙江省省会，以西湖美景". Do not mention this.]
等一下，我想问的是上海。
```

"At most" because the engine records whole chunks when they are queued for
playback, not what was played. On test calls the caller cut in about 1.3 s into a
4.3 s chunk, and the whole chunk was reported. "Do not mention this" is there
because without it the agent said aloud that it would complete the interrupted
content, which `LIVE-CALL-UX.md` rules out.

The text comes from one of two places:

| Case | Source | Why |
|---|---|---|
| Live call | The `[interrupted]` assistant message in `msgs[sent:]` | Only the engine knows what was played; generated text runs ahead of the audio |
| Text or voice message | `active`'s delivered deltas | The first turn is still open when the second is admitted, so its partial reply is not in the history yet |

If nothing was delivered, no prefix is needed: the model saw nothing and neither
did the caller, so they agree.

In `steer` and `followUp` the model does see the whole answer. Live calls still
get the prefix, since the caller heard only part of it. Text and voice messages
do not, since the first reply is delivered in full.

### The stream state machine

Each `ChatStream` call handles one POST and needs no shared demultiplexer:

```
             submission(mine, placed)
  waiting  ---------------------------->  owner  --- text.delta ---> onToken
                                            |
                                            | submission(newer, placed)
                                            v
                                        return ErrSuperseded

  any state: final JSON-RPC message --> return
                                        (own unanswered/aborted: ErrSuperseded)
```

- **The rule:** a call owns the output from the moment its input is placed until
  a newer input is placed or its own result arrives. This one rule covers all
  three modes, and text from a previous run is never forwarded to the wrong
  reply.
- **Aborted is not a failure.** A reply that a newer message interrupted returns
  `ErrSuperseded` with its partial text. Treating it as an error would show the
  user a failure on their first message whenever they sent a second.
- **Failed-run recovery (`steer` and `followUp` only):** if another submission
  settles `unanswered` while this one is still `waiting`, and it has not been
  placed 500 ms later, call `session.cancel` and resubmit once with a fresh
  `requestId`. `interrupt` cannot get stuck this way, because it clears the inbox
  before submitting. An input aborted while still queued was never seen by the
  model, unlike an interrupted reply, so it is resubmitted the same way, without
  the cancel.
- **Stream resync:** durable can replace missed events with a `snapshot`. If this
  submission is no longer in the snapshot's `inbox`, treat it as placed. The final
  message remains the authority on how the call ended.
- **The return value is the concatenated deltas**, not `result.text`. The result
  holds only the final message, so it would omit "Let me check." and disagree
  with what was spoken.
- **Plugin replies** have no submission header. Ignore the events and emit
  `result.text` as one delta. A plugin reply queued on a busy session sends its
  events elsewhere, so this is the only path that is always correct.

Errors: non-200 at admission becomes an error (including 429 and 409), with the
404 retry above; `unanswered` with a reason becomes an error carrying `detail`.

### Cancellation and transport

Cancelling `ctx` closes this call's stream and nothing else. The run continues
until the next message reaches it. `abortOnDisconnect` stays unset, because a
barge-in cancels at speech start, before anyone knows whether it was words or a
cough. Aborting there would lose the reply to a false trigger. The abort comes
with the transcript instead.

- **Admission does not use the caller's `ctx`.** agent-pi admits a request even
  if the client leaves, so a barge-in during the POST would otherwise leave the
  provider unsure whether the input was accepted, and the next turn would send
  it twice. The POST uses `context.WithoutCancel`, bounded by
  `Transport.ResponseHeaderTimeout` (10 s), and `context.AfterFunc(ctx, ...)`
  closes the body. SSE headers are sent only after admission, so the response
  arriving means the input is durably in.
- **No `http.Client.Timeout`.** The 120 s limit in `openaicompat` covers the whole
  body, and a tool round can legitimately take longer.
- **Idle watchdog.** agent-pi sends a keep-alive every 15 s. Treat 45 s without a
  byte as a dead stream. A live turn has no other deadline, so a dead connection
  would otherwise mean silence until the caller interrupts.
- **Retries.** Retry a failed connection once with the same `requestId`, which is
  safe because durable deduplicates on it, and a retried interrupt does not abort
  a second time. Mid-stream resume with `Last-Event-ID` is left out of this
  version.

### Engine changes

| File | Change |
|---|---|
| `engine/chat.go` | Set session and admission release. Treat `ErrSuperseded` as success, and skip the empty-reply check for it |
| `engine/live_turn.go` | Set session. Treat `ErrSuperseded` as a normal end |
| `engine/voice.go` | Release the engine's FIFO layer at admission |
| `engine/conversation.go` | Make the release idempotent |
| `livekit/text_mode.go`, `voice_mode.go` | Release the transport's FIFO layer at admission |

Everything else in the live-call loop stays the same. Barge-in still cuts audio
locally and immediately, so "it stops, mid-word" does not depend on agent-pi.

### Observability

`llm.chat_stream` spans gain:

- `agentpi.submission_id`
- `agentpi.queued`: whether it had to wait
- `agentpi.wait_ms`: time from admission to placement

`agentpi.wait_ms` shows how long `steer` or `followUp` held a message back, and
stays near zero with `interrupt`. The `turnId` sent to agent-pi is the trace ID, and a
`traceparent` header puts agent-pi's spans in the same trace (`DESIGN.md` §7).

## Why the default is `auto`

A plain interrupt during a tool round aborts the tool, and the model is told it
was aborted even if it had already acted. So it reports things that are not true.
Measured on `qwen-plus` with a mock `send_email` that sends first and then waits
5 s for an acknowledgement, five runs of each:

| Second message | `interrupt` | `auto` |
|---|---|---|
| Unrelated question | 1 email, answered | 1 email, answered |
| "Change the time to four" | 2 emails, the first never mentioned (5 of 5) | 2 emails, says the first went out (4 of 5); asked first, 1 email (1 of 5) |
| "Never mind, don't send it" | 1 email, **says it was cancelled (5 of 5)** | 1 email, says it cannot be recalled (5 of 5) |

The identical email was never sent twice. `auto` was not slower here: this mock
ignores the abort signal, so both waited for it, about 6 s to the first token. A
tool that honours the signal would stop sooner under `interrupt`.

`auto` decides on the server, because a barge-in closes the stream at speech start
and the client can no longer see what is running when the transcript arrives. It
steers while `pi.live.tools` is set and aborts otherwise. Generation is still
aborted when it is about to call a tool, since nothing has acted yet.

What `auto` still costs:

- **A barge-in during a slow tool waits for that tool.** Audio stops at once, but
  the reply comes after the tool finishes. A hung tool holds the reply until the
  next barge-in. Use `interrupt` only if every tool is safe to abandon.
- **A second text message cuts off the first reply.** Two quick messages get one
  answer, with both in context. The first reply stops where it was and finishes
  with status `done`. This also fits a user splitting one thought across two
  messages.
- **An interrupt that is then refused still stopped the old run.** The abort comes
  before `beforePrompt` and the run limits, so a plugin `reject` or a 429 leaves
  the session idle with no answer. The words stay pending and go out with the
  next turn.

## Known limits

- **A barge-in with no words.** A cough that clears the debounce closes the stream
  but creates no new turn, so nothing interrupts the run. It finishes unheard and
  sits in the model's context as if it had been said.
- **The gap before the abort.** Between speech start and the transcript, the old
  run keeps going, and any tool it starts in that window runs. On a test call the
  gap was 3.8 s, long enough for the run to finish, so there was nothing left to
  abort. The whole answer stayed in model context, and only the "heard" prefix
  told the model what the caller actually got. On a longer reply the abort did
  land mid-generation, three times out of three.
- **Barge-in sensitivity.** The VAD used to restart its count on every quiet frame,
  so the 40 ms dips between syllables hid short phrases: a 1.2 s "换个话题吧" missed
  three times out of three. It now bridges gaps up to `audio.MaxSpeechGapMS`. Over
  LiveKit that phrase interrupted three of three, a simulated 0.3 s cough none of
  three. Both were synthetic, so it still needs checking with a real voice and with
  an open speaker, since nothing cancels the agent's own echo.
- **Shared identity.** One token means one agent-pi user. `maxConcurrentRunsPerUser`
  defaults to 3, so a fourth simultaneous caller gets a 429, and `maxSessionsPerUser`
  defaults to 20, so the 21st distinct identity is refused for good, and the
  provider never closes sessions. Raise `MAX_RUNS_PER_USER` and
  `MAX_SESSIONS_PER_USER` for now. Per-user JWTs are the real fix, and they are also
  what `auth: "forward"` MCP servers need.
- **Text-mode deadline.** The 50-second message deadline still applies. A longer
  tool run shows an error while the run carries on, and its answer is not
  delivered.
- **Live call plus typed message.** They now share one session, so a typed message
  can take over the output of a spoken reply.
- **Superseded voice messages.** Audio already synthesized for the first reply is
  still delivered, alongside the second reply's audio.
- **Partial replies after a text deadline.** The "heard" prefix covers a reply
  that is cut off while its stream is open. One that hit the 50-second deadline has
  already closed, so the model is not told how much of it arrived.
- **Stale Go history.** `Conversation` keeps growing and only feeds the cursor and
  the interrupted-text correction. Its contents are never sent.
- **System prompt.** `-system-prompt` has no effect with this provider. Edit
  `apps/assistant/prompts/voice.md` instead.

## Rejected alternatives

- **One `GET /agent` per session plus JSON admissions.** Needs no server change
  and has one ordered event source. But it adds a long-lived per-identity stream
  and connection lifecycle, and it is not the request-and-stream shape asked for.
  Worth revisiting if mid-stream resume becomes a requirement.
- **`steer` as the default.** Durable steer does not cut off a text answer, so
  after a barge-in the reply would wait for the old answer to finish generating,
  on top of a turn already measured at 5.5 seconds against a target under 2.
- **`auto` decided by the client from `tool.started` events.** A barge-in has
  already closed the stream by the time the decision is needed.
- **Stream `session.steer`.** It would need a server change and add nothing over
  `session.prompt` with `whenBusy`, which already works on idle sessions.
- **Tag every event with a submission id.** The cleanest long-term protocol, but
  a larger server change, and the handover rule does not need it.
- **Send the full history each turn.** agent-pi has no such input, and it would
  defeat durable transcripts, compaction, and prompt caching.
- **Extend the `LLMProvider` interface.** Forces changes on every fake and on
  providers that have no sessions.

## Implementation order

1. **agent-pi:** add `Agent-Submission-Id`, with a test, and correct the claim in
   `design.md` and `session.interrupt`'s comment that the model sees the partial
   answer.
2. **Provider core:** `provider/turn.go`, the `agentpi` package, config, and
   registration. Tests replay the event orderings recorded above against an
   `httptest` server.
3. **Engine and transport:** session, early release, `ErrSuperseded`. Tests cover
   a second message overtaking the first's reply, and `openaicompat`-style fakes
   staying serial.
4. **Docs:** update `.env.example`, `CLAUDE.md`, and the "One message turn per
   identity" section of `TEXT-VOICE-UX.md`, which stops being true for this
   provider.
5. **End to end:** run the assistant app with `AUTH_KIND=static` and use
   `cmd/testclient text` and `cmd/testclient live -interrupt-file`, in each mode.

## Test matrix

| Case | Expected |
|---|---|
| Idle first message, each mode | Placed at once, streams, returns the text |
| Unknown `AGENT_PI_WHEN_BUSY` | Startup fails |
| Interrupt during text | A returns `ErrSuperseded` with partial text, B gets only its own answer |
| Interrupt during tools | A returns `ErrSuperseded`, B gets the new answer |
| `auto` during tools | The tool finishes, the model sees its result and the new message |
| `auto` during text | Same as interrupt |
| Three messages in a row | All three reach the model; only the last is answered |
| Text interrupt | Next input carries A's delivered text as the prefix |
| Live interrupt | Prefix uses the spoken text, not the longer generated text |
| Steer during text | A finishes, B gets only its own answer |
| Steer during tools | A returns `ErrSuperseded` with partial text, B gets the joined answer |
| Two queued steers | Each is placed in order and gets its own answer |
| Steer behind a failed run | Recovered once, no hang |
| Barge-in during admission | Input is sent exactly once across the two turns |
| Barge-in before anything was spoken | Next input is not duplicated and has no prefix |
| Barge-in after partial speech | Next input carries the heard-only prefix |
| 429 at admission | Error returned, words carried into the next turn |
| 404 session | Recreated once |
| Plugin reply, idle and busy | `result.text` delivered as one delta |
| `message.end` with `error` | Error carries `detail`, partial text is kept |
| Stream silent for 45 s | Fails instead of hanging |
| Go restart | Existing session found through `session.list` |
| `openai_compat` | No behaviour change |
