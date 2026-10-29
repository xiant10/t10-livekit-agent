# Holding the floor

Observability note: historical measurements and Jaeger commands below predate
our current otel-lgtm stack. For new measurements use Grafana/Tempo at
http://localhost:3002 with the trace ID from agent logs.


A call has one floor and one speaker on it. Everything the agent does
between hearing you and answering you is a negotiation over when that
floor changes hands — and every defect worth fixing turns out to be a
floor defect. It takes the floor when it shouldn't, keeps it when it
shouldn't, or hands it back too slowly to feel like a conversation.

This is the target: the five moments of a turn, the latency budget behind
them, and the parts that don't meet it yet. Every number here is read off
a trace of a real call through the Qwen stack — see "How these numbers
were taken" at the end. For *how* any of it is implemented, see
`DESIGN.md` §6; this document is the why, that one is the what.

---

## The one rule

> **The caller owns the floor. The agent borrows it.**

Everything below is a consequence of that sentence. When a rule here and
a feature want different things, this is the tiebreaker.

---

## Five moments, in the order they happen to you

A turn is a sequence, so these are numbered. Each says what it should
feel like from the caller's side, what has to be true for that, and —
where it's instructive — what it used to do instead.

### 1. You start talking

*Should feel like: the room went quiet and something leaned in.*

Within about a third of a second of your first syllable — before a single
word has been recognized — you can see that you are being heard. Words
then fill in as they're picked up, and they are allowed to be wrong; they
get corrected the moment you stop.

The first syllable has to survive. A voice detector only fires once
speech has *already* been running for a fifth of a second, so the audio
that triggered it must be kept and replayed into the recognizer.
Otherwise every utterance is transcribed from its second word onward, and
nobody can tell you why the agent keeps mishearing them.

> **Before:** the stream opened at detection and pushed from the next
> frame. The opening 200ms of every single utterance went in the bin
> untranscribed.

### 2. You pause mid-thought

*Should feel like: nothing. A pause is a pause.*

Silence is not a turn boundary. And even when it runs long enough to look
like one, the decision stays reversible: if you pick the sentence back up
before the agent has said anything out loud, both halves are one thing
you said, and they are answered together.

This is the whole difference between an agent you can think out loud in
front of and one you have to compose finished sentences for.

> **Before:** from a real call, `heard: 嗯，我今天的计划是。` — "my plan
> today is." — answered on its own, out loud, while the caller was still
> saying what the plan was.

### 3. You stop

*Should feel like: confirmed, then working.*

Your own words land on screen inside a second — and, crucially, at the
same speed whether the agent is idle or three sentences deep into a
previous answer. Reading back what somebody just said is not the same job
as answering them, and it must never queue behind it.

Then something visible has to change. Silence with no signal reads as a
dropped turn, and people repeat themselves into it.

> **Before:** transcription shared a worker with reply playback, so your
> words waited out the previous answer — measured at up to **45 seconds**
> between speaking and seeing.

### 4. It answers

*Should feel like: someone who has an answer, not a system fetching one.*

Speech starts before the answer has finished being written, runs one to
three sentences, and stops. What is on screen is what has been said out
loud — never what has been generated, which is normally a paragraph
further along.

Length is a floor property, not a matter of style. A forty-second answer
holds the floor for forty seconds, and the caller's only exit is to
interrupt it — which means the agent has arranged for its user to be rude
in order to get a word in.

> **Before:** replies of eighteen sentences, `turn` spans of **44.8s**,
> and the full text on screen while a single sentence of it had been
> heard.

### 5. You cut in

*Should feel like: it stops. Mid-word.*

Not at the end of the sentence, not at the end of the buffer. That
includes the audio already sitting in the speaker on your own machine,
which the agent cannot reach and therefore has to ask for.

Then it answers the new thing without narrating what just happened.
"Sorry, go ahead" spends another turn telling you something you already
know.

And it remembers what it actually said — the part you heard, not the part
it wrote. An agent that believes it said nothing will restart the same
sentence from the top.

A cough is not an interruption. Cutting somebody off mid-word should take
real, sustained speech; starting a fresh turn should not.

> **Before:** an interrupted reply was recorded in the conversation as
> nothing at all, and a barge-in that arrived during transcription — when
> there was no reply to cancel yet — was dropped for arriving too early.

---

## Where the silence goes

Between your last syllable and the agent's first is the number that
decides whether a call feels alive. Human conversation turns over in
roughly a fifth of a second; nothing built on network round trips to a
hosted model is going to touch that. So the target is not *natural* — it
is *not awkward*, which lands somewhere under two seconds.

One turn, one trace. The four stages between the caller's last syllable
and the first audio leaving the agent:

| Stage | Measured | Achievable | What it is |
|---|---:|---:|---|
| Endpoint hold | 900 ms | 700 ms | Silence the detector waits out before calling you finished |
| Transcribe | 599 ms | 599 ms | Committing the utterance and getting the final transcript back |
| Think | 1,340 ms | 1,340 ms | Model generating far enough to cut a first speakable chunk |
| Speak · first chunk | 2,627 ms | 522 ms | Synthesizing that chunk and getting audio moving |
| **Total** | **5,466 ms** | **3,161 ms** | |

"Achievable" is the same turn with the two provider-transport fixes
applied (below). Neither column reaches the two-second line.

Three of the four stages sit near their floor. The hold is a config
value, transcription is one round trip, and thinking is the model's own
time. Almost all of the headroom is in the fourth — so it's worth opening
up:

| Inside that 2.6-second synthesis call | Time | Recoverable by |
|---|---:|---|
| Open connection, configure session | 1,023 ms | Reusing connections across chunks |
| Provider produces first audio | 522 ms | — |
| Waiting for audio already produced | 1,082 ms | Streaming synthesis instead of a buffer |
| **Total** | **2,627 ms** | **2,105 ms recoverable** |

Four fifths of that call is not synthesis. About a second opens a fresh
WebSocket to the provider and configures a session — once per chunk,
every chunk, for the life of the call. Another second is spent holding
audio that has *already arrived*, because the interface hands back a
finished buffer instead of a stream. The caller pays for both and
receives neither.

---

## Seven things that must never happen

Each of these was a real failure before it was a rule. If a change makes
one of them possible again, the change is wrong, whatever else it
improves.

| Rule | The failure it prevents |
|---|---|
| Never speak while the caller is speaking. | Answering the first half of a sentence, out loud, over the second half. |
| Never answer a fragment. | "My plan today is." taken as a complete turn and replied to. |
| Never show text that wasn't spoken. | A whole reply on screen while one sentence of it has been heard — and, after an interruption, paragraphs the agent never said. |
| Never drop an interruption. | Cutting in during transcription, before a reply exists to cancel, and being ignored for arriving too early. |
| Never let the transcript wait on the reply. | Your own words reaching the screen 45 seconds after you said them. |
| Never let one slow provider call stall the rest. | Eighteen synthesis requests in flight at once, three-character chunks taking 7.7s, and one hitting a 20-second timeout with the rest of the answer stuck behind it. |
| Never let the model think it said nothing. | Restarting, word for word, the sentence it was just cut off in the middle of. |

---

## What an interruption looks like

A real exchange, recorded through `cmd/testclient live`. The caller asks
about Hangzhou, changes their mind mid-answer, and asks about Shanghai
instead. The right-hand column is the `agent.state` message that arrived
with each line.

```
You: ▌                                              listening
You: 用一句话介绍一下杭州这座城市。                    transcript · 0.6s later
Agent: ▌                                            thinking
Agent: 杭州是浙江省省会，以西湖美景、…                  interrupted
You: 等一下，我想问的是上海。                          transcript
Agent: 上海是直辖市，中国最大的城市和经济中心，          idle
       有外滩、东方明珠等地标。
```

Three things to notice. The agent's line stops where the *voice* stopped,
not where the model stopped — the rest of that sentence was generated and
thrown away unheard. The ellipsis is the screen saying so. And the reply
to the second question arrives without a word about the interruption,
because acknowledging it would cost a turn.

---

## Where it still falls short

**Time to first audio — 5.5s, target under 2s.** The two levers are
connection reuse and streaming synthesis, worth about a second each. Both
live in the TTS client (`internal/provider/qwen/tts.go`); neither touches
the call logic. After them the next question is the 1.3-second think
stage, which swings between 0.7s and 1.4s turn to turn.

**Captions are chunk-granular — up to several seconds of skew.** When a
chunk is released, all of its text appears at once while its audio plays
out over the following seconds. Honest at the chunk, not at the word.
Closing that gap means tracking playback position inside a chunk and
revealing text against it.

**No echo cancellation — mitigated, not fixed.** On an open speaker the
agent hears its own voice and can read it as a barge-in. Requiring
longer, sustained speech to interrupt (`VAD_BARGE_IN_MIN_SPEECH_MS`)
makes that rare. It does not make it wrong, and headphones are still
doing most of the work.

**The interrupted record is chunk-granular too — minor.** History can
credit the caller with hearing slightly more than actually reached their
speaker, because a chunk is marked spoken when it is handed over rather
than when it finishes playing.

**Transport timing is still approximate.** Turn admission and audio enqueue
now serialize with interruption using the controller's existing mutex.
The transport queues PCM promptly and encodes on its writer, so that lock
never waits for a whole chunk to encode or play. A frame already taken by
the writer or sent over the network can still arrive after an interruption;
client-side buffer clearing remains necessary.

### Implementation review · September 12, 2026

The code now closes several gaps in the rules above:

- Failed or empty synthesis produces neither captions nor assistant history.
  Completed turns, like interrupted turns, remember only audio handed to
  playback, rather than everything the model generated.
- Speech claims the floor before interim-caption callbacks and STT setup.
  A turn rechecks the speech version and outstanding utterances after the
  quiet check; failed admission retains its transcript batch for retry.
- Enqueue and interruption/drain share one ordering, so late synthesis
  cannot refill the cleared audio queue. State callbacks are serialized too.
- Synthesis slots are reserved in submission order, before starting workers.
  Later chunks cannot consume all the slots ahead of the first, and a
  cancelled player does not wait for an outstanding synthesis result.
- Pre-roll always covers both configured speech-detection windows, including
  the longer interruption debounce.
- The audio writer pads the last partial frame and plays it. Previously that
  remainder could stay queued forever and prevent the call returning to idle.
  Enqueueing also no longer blocks behind a full encoded-frame channel.

These changes are covered by engine and transport regression tests, including
race-detector runs. They do not establish a new hosted-provider latency figure:
the timing tables above remain historical measurements. Streaming synthesis,
connection reuse, word-timed captions, and echo cancellation remain open.

---

## How these numbers were taken

All of them come from Jaeger traces of live calls against the
containerized agent, driven by `cmd/testclient live` with a WAV standing
in for a microphone:

```bash
docker compose up -d                      # agent + livekit + otel + jaeger
go run ./cmd/testclient live \
    -file question.wav -interrupt-file interrupt.wav -interrupt-after 10 \
    -out reply.wav -final-wait 30
```

Then read the trace at <http://localhost:16687>, or pull it by the
`correlation_id` the agent logs for the call:

```bash
curl -s "http://localhost:16687/api/traces/<correlation_id>"
```

The stage timings are the `stt.commit` / `llm.chat_stream` /
`tts.synthesize` / `turn` spans. The endpoint hold is not a span — it is
`VAD_MIN_SILENCE_MS`, which elapses before `stt.commit` starts, so
"measured" totals add it to the first span's start. The breakdown inside
a synthesis call comes from the `tts.first_delta_ms` and `tts.total_ms`
attributes on the `tts.synthesize` span; the connection-setup figure is
the span's own duration minus `tts.total_ms`.

Two cautions when re-measuring. Stage times vary a lot turn to turn — the
think stage swung 0.7s–1.4s across one call — so quote a single trace
rather than mixing runs, as the tables above do. And `cmd/testclient`
starts streaming the moment it publishes, before the agent has subscribed
to the track, so the first half-second of the WAV never arrives and the
transcript looks clipped at the front; that is the harness, not the
pipeline.

### Terminal and interim-caption corrections

The live client uses a single terminal-width-bounded preview row and prints
each finished reply once in full. Repainting an entire growing reply while
clearing only one row duplicated wrapped Chinese text. Ordinary diagnostic
logs now go to `t10-client.log` so SDK output cannot break that live row.
The status labels are `[listening]` and `[thinking]`; interruptions use an
explicit `[interrupted]` label rather than ambiguous dot placeholders.

Interim captions consume Qwen's `text + stash` snapshots, including tentative
corrections, rather than waiting for a `delta` string that this ASR model does
not normally send. Final transcripts carry the same utterance ID as their
interim stream, and state messages have a strictly increasing event order.
This protects newer speech and active replies from delayed callbacks.
