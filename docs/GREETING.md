# Call greeting: the voice worker's part

Status: built 2026-11-12 on `feat/call-greeting`, not committed. The agent's part, with variables and `session.greet`, is [`agent-ti/docs/greeting.md`](../../agent-ti/docs/greeting.md).

When a caller's microphone track is subscribed, the agent speaks first. The agent server writes the greeting; this worker asks for it, speaks it, and lets the caller interrupt it like any reply.

## Flow

1. **Join.** When the worker joins a room with a caller in it, it sends `session.greet` with `prepare: true` for each caller it can already see, so the greeting is ready before their microphone is. A room used only for text never gets past this step.
2. **Microphone subscribed.** `startLiveCall` gives the controller a `Greeter`. Before the first real turn, the turn worker runs one **greeting turn**, unless the caller has already started speaking.
3. **Greeting turn.** `TTS.Warm` and `Greeter.Greet` run side by side. The text streams through the same chunker, TTS concurrency cap, caption pacing and `playSpeech` as a reply, and goes out on `agent.chat` and `agent.state`, so clients show it like any reply.
4. **Interrupted.** Caller speech bumps the turn version, cancels the greeting and drains the sink, as for any reply. What was heard goes into the history with the `[interrupted]` mark, so the agent-pi provider tells the model what the caller heard on the next turn.

A caller is greeted at most once per stay. A track subscribed again for the same caller isn't greeted again, and agent-ti's `cooldownMs` covers the agent rejoining a room after a drop.

## Engine

```go
// Greeter produces a call's opening line. "" says nothing.
type Greeter interface {
    Greet(ctx context.Context, identity string, onToken func(delta string)) (string, error)
}
```

- `LiveCallController.Greeter` is optional; nil greets no one.
- `streamAndSynthesize` takes its text source as a function, so a reply and the greeting share every step after it.
- The greeting turn is a telemetry turn with mode `greeting`. Its `turn.first_audio_ms` runs from the track subscription, which is the number that says whether the greeting feels instant.

## Providers

| Provider | Greeting |
|---|---|
| `agent_pi` | `session.greet` on the call's session, under the caller's grant, as SSE: `greeting.delta` events, then the result. A harness without the method (`method not implemented`), an error, or no first word within `VOICE_GREETING_TIMEOUT_MS` falls back to `VOICE_GREETING`. |
| any other | `VOICE_GREETING` as fixed text. |

The local fallback is spoken but never written to the agent's transcript.

## Client values

The caller's room token can carry `t10.client.<key>` attributes, set by auth-server at admission from what the client asked for (see `auth-server/docs`). They go to `session.greet` as `client`, with `client.locale` and `client.timeZone` also sent as `locale` and `timeZone`. The client can't change them after admission, but agent-ti still treats them as content written by someone else.

## Settings

| Variable | Default | |
|---|---|---|
| `VOICE_GREETING_MODE` | `auto` | `agent`, `fixed` or `off`. `auto` is `agent` with `LLM_PROVIDER=agent_pi`, else `fixed` when `VOICE_GREETING` is set, else `off`. |
| `VOICE_GREETING` | empty | The local greeting, and the fallback in `agent` mode. |
| `VOICE_GREETING_TIMEOUT_MS` | `4000` | How long to wait for the agent's first word before the fallback. |
| `VOICE_GREETING_PREPARE` | `true` | Send `prepare` when joining a room. |

`cmd/testclient live` would hear the greeting before its own audio. Run the agent with `VOICE_GREETING_MODE=off` for those tests.
