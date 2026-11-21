---
title: "t10-livekit-agent room protocol"
audience: consumer
kind: reference
status: current
summary: "The LiveKit room protocol a client uses to talk to the voice worker: topics, streams, attributes."
---

# t10-livekit-agent room protocol

`t10-livekit-agent` serves no HTTP API. A client talks to it inside a LiveKit room, over text streams, byte streams and an audio track, and this page is that contract. The topics and attributes are [`internal/roomprotocol/topics.go`](../internal/roomprotocol/topics.go), whose copy in each client the workspace checks compare. [`design.md`](design.md) explains the agent, [`text-voice-ux.md`](text-voice-ux.md) and [`live-call-ux.md`](live-call-ux.md) the experience each mode is built to give, and [`greeting.md`](greeting.md) the greeting.

## Joining

1. The client asks `auth-server` for admission: `POST /v1/rooms/admission` with the person's token and `{room, client?}`, or workbench's `POST /api/voice/admission`. A conversation's room is `conv-<session ID>`.
2. It joins LiveKit with the `room_token` it gets back.
3. A worker claims the room and joins it. It reads two attributes from the caller, both inside the LiveKit-signed token so the client can't change them:
   - `t10.grant`: the agent grant admission created, which the worker exchanges for a delegated token when `LLM_PROVIDER=agent_pi`.
   - `t10.client.<key>`: what the client said about the call, such as `screen`, `locale` and `timeZone`, passed to the agent's greeting.

One room is one conversation. Text, voice messages and a live call in the same room share one conversation history per participant.

## Modes

| Mode | Client sends | Agent sends |
| --- | --- | --- |
| Text | A text stream on `agent.chat` (or `lk.chat`) | The reply, streamed as text on the same topic |
| Voice message | A WAV byte stream on `agent.voice` | What it heard on `agent.transcript`, then the reply as WAV byte streams on `agent.voice` |
| Live call | A published audio track. No start signal: subscribing to it starts the call | An audio track, final transcripts, interim captions, reply text, and state |

## Topics

| Topic | Direction | Carries |
| --- | --- | --- |
| `agent.chat` | Both | Text messages in, streamed reply text out. Append-only |
| `agent.voice` | Both | WAV recordings in, synthesized WAV replies out, one stream per chunk |
| `agent.transcript` | Agent → client | The authoritative transcript of one utterance, as plain text |
| `agent.transcript.interim` | Agent → client | Live call only. One stream per utterance, `format=snapshot-jsonl`: lines like `{"text": "…"}`, each replacing the last |
| `agent.state` | Agent → client | Live call only: `listening`, `thinking`, `speaking`, `interrupted`, `idle` |
| `agent.message.status` | Agent → client | Text and voice messages: one JSONL stream per request (below) |
| `lk.chat`, `lk.transcription` | Both | LiveKit's standard topics, mirrored for standard agent clients |

**`interrupted` is load-bearing.** On it, and on the change back to `listening`, the client must drop audio already in its speaker buffer: the caller cut in.

## Stream attributes

| Attribute | On | Means |
| --- | --- | --- |
| `request_id` | Status, transcript, text and audio of a text or voice message | Ties every stream of one request together. The client sets it on what it sends, and without one the agent uses the incoming stream's ID |
| `chunk_index` | Voice reply WAV streams | Zero-based order of the chunk |
| `event_order` | Live-call streams | Increasing within a call. Ignore a state older than one already shown |
| `utterance_id` | Interim and final transcripts | Which segment of speech, so a late final never erases a newer caption |
| `format` | Interim transcripts | `snapshot-jsonl` |
| `turn_id`, `traceparent` | Any stream that belongs to a turn | Whichever end starts a turn sends these, and the other joins its trace. A peer that sends none is answered in a trace of its own |
| `lk.segment_id`, `lk.transcription_final`, `lk.transcribed_track_id` | `lk.transcription` | LiveKit's standard transcription attributes |

The agent also sets its own participant attribute `lk.agent.state` (`initializing`, `listening`, `thinking`, `speaking`, `idle`), which LiveKit keeps across reconnects.

## Message status

Each text or voice message gets one `agent.message.status` stream, a line of JSON per change, ending with `done` or `error`:

```json
{"state": "queued"}
{"state": "transcribing"}
{"state": "thinking"}
{"state": "done", "audio_chunks": 3}
```

| `state` | Means |
| --- | --- |
| `queued` | Waiting behind the participant's earlier message |
| `transcribing` | A voice message is being heard |
| `thinking` | The reply is being made |
| `done` | Finished. `audio_chunks` says how many WAV streams to wait for, since streams can arrive out of order |
| `error` | Ended, with a `message` |

Text and voice messages share one queue per participant, answered in order, and each has 50 seconds. Agent and client must be upgraded together when this contract changes.

## Docs for coding agents

The worker serves this repo's docs for people building on t10 at `POST /docs/mcp`, over MCP and with no token, as the workspace's [docs contract](../../contracts/docs/README.md) describes. It takes no calls over HTTP, so the docs share a small listener at `LIVEKIT_AGENT_LISTEN_ADDR`, off when it's empty, with `GET /healthz`, `200` while the process runs, and `GET /readyz`, `200` until a `SIGTERM` starts the drain and `503` after. `t10-livekit-agent docs <addr>` serves only the docs route, for the contract's suite.
