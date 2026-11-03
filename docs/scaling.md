# Scaling voice workers

How voice workers share rooms, survive deploys, and keep a call's text in step with its audio. [`DESIGN.md`](DESIGN.md) is the worker itself, and the workspace's [`ARCHITECTURE.md`](../../ARCHITECTURE.md), "Communication and scaling", places it in the platform.

Status: claims, `VOICE_MAX_CALLS` and draining are built, and two workers run on the local cluster. No load test has run, and clustered LiveKit and TURN are outstanding.

## Voice workers and LiveKit

Each room gets one worker, chosen by a short lease in Redis. Every `t10-livekit-agent` process polls LiveKit, all under one identity, and LiveKit allows one participant per identity, so without a claim two processes would keep evicting each other. Claims are built, and a single worker needs no Redis.

Polling stays, because a restarted worker still finds calls already under way. A claim step comes before the join:

1. **Claim.** A worker that finds an occupied room runs `SET t10:voice:<room> <worker> NX PX 15000`. Only the winner joins.
2. **Hold.** The holder renews the lease every 5 seconds. Both numbers are starting values.
3. **Spread.** A worker at `VOICE_MAX_CALLS` stops claiming, and a busier worker waits slightly longer before it claims, so calls lean towards idle workers.
4. **Fence.** Workers keep one identity per room. If two ever overlap, LiveKit evicts the older one, and that worker stops without retrying.

**Failures.** If a worker crashes, its lease runs out and another worker joins within about 16 seconds. With `LLM_PROVIDER=agent_pi`, the conversation remains in the agent replica, but the active audio turn is interrupted. With the standalone `openai_compat` default, the worker's in-memory conversation is lost. If Redis is down, workers claim nothing new, and existing calls carry on.

**Deploys.** On `SIGTERM` a worker stops claiming, lets calls run up to `VOICE_DRAIN_SECONDS`, then leaves each room and releases its lease. Another worker joins within about a second. Kubernetes's termination grace period must exceed that limit, and the first limit to try is 10 minutes.

**LiveKit.** A second LiveKit node needs Redis, which then carries room data and messages between nodes, and the same instance can hold the claims. A room must fit on one node. That's no limit here, because a call has two participants. `auth-server` needs no change, since `server_url` stays one load-balanced address and it still never calls LiveKit. Each node needs its UDP ports reachable, and networks that block UDP need TURN.

The first version runs in one region. LiveKit has a region-aware node selector, which we can adopt if callers turn out to be far apart.

## During a call

A client in a call has both paths open, so each kind of text needs one source. The room shows what was spoken, and the conversation stream shows everything else.

| Text | Source during a call | Rule |
| --- | --- | --- |
| What the caller said | `agent.transcript` and `agent.transcript.interim` in the room | Arrives before any reply, as today |
| A spoken reply | `agent.chat` in the room | Paced to the audio, so the screen never shows text that wasn't spoken |
| The same reply on the conversation stream | Ignored by a client in the call | Its `text.delta` events run ahead of the audio. A client knows its typed turns, so any other `turnId` is a spoken one |
| A typed message and its reply | The conversation stream | Shown as text and not spoken |

`workbench` holds to this by asking whose turn it is. In a call it shows only text from turns it started, because conversation deltas reach it before the room's stream names the turn. Transcript reads also wait while the agent speaks, or a finished reply would appear early.

**A typed message doesn't cut off speech.** During a call it goes out as `session.prompt` with `whenBusy: "followUp"`, and the reply follows the spoken one. `agent-pi`'s default would let it take over a spoken reply's output, as `AGENT-PI-PROVIDER.md` warns. A later barge-in still interrupts that typed reply, because speech outranks it. This path hasn't been tried on a real call.

A client outside the call sees the voice turn as ordinary conversation text. It isn't listening, so nothing can run ahead of the audio.

## Alternatives considered

| Option | Why not |
| --- | --- |
| LiveKit agent dispatch for voice workers | The better balancer in the long run, since workers report their load. But `server-sdk-go` 2.18.1 has no worker client, so we'd write that protocol ourselves. Dispatch from a token also fires only when a room is created. Revisit once the load tests show claims aren't enough |
