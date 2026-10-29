# t10-agent

A high-performance, real-time voice agent built in Go on top of [LiveKit](https://livekit.io). It orchestrates a modular **STT → LLM → TTS** pipeline supporting low-latency live calls with full-duplex audio, Voice Activity Detection (VAD), and responsive interruption/barge-in.

---

## Features

- **Three Modalities in One Room**:
  - **Live Calls**: Real-time bidirectional WebRTC audio with streaming STT, LLM generation, sentence chunking, parallel TTS synthesis, and barge-in handling.
  - **Voice Messages**: Single-turn audio message processing (inbound audio file/stream $\rightarrow$ outbound speech reply).
  - **Text Chat**: Fast text messaging with streaming replies and synchronized conversation memory.
- **Transport-Agnostic Engine**:
  - `internal/engine` manages conversation history, turn taking, audio buffering, and barge-in state without any dependency on WebRTC or LiveKit.
- **Pluggable Model Providers**:
  - Clean interfaces for `STTProvider`, `LLMProvider`, and `TTSProvider`.
  - Built-in support for OpenAI-compatible endpoints and Alibaba DashScope / Qwen Realtime APIs.
- **Barge-in / Interruption Support**:
  - Rapid cancellation of in-flight LLM/TTS generation and instant playback drain when caller speech is detected.
- **Built-in Observability**:
  - OpenTelemetry tracing and metrics with correlation IDs propagated across all modalities and turns.
  - Pre-configured Grafana LGTM (Loki, Grafana, Tempo, Mimir) stack via Docker Compose.

---

## Architecture Overview

```
                      ┌────────────────────────┐
                      │  LiveKit Room / WebRTC │
                      └───────────┬────────────┘
                                  │ (Audio tracks, Data packets)
                                  ▼
                      ┌────────────────────────┐
                      │    internal/livekit    │
                      │  (Transport Adapter)   │
                      └───────────┬────────────┘
                                  │ (Ports: AudioSource, Sink)
                                  ▼
                      ┌────────────────────────┐
                      │    internal/engine     │
                      │ (Conversation State &  │
                      │  LiveCallController)   │
                      └───────────┬────────────┘
                                  │
         ┌────────────────────────┼────────────────────────┐
         ▼                        ▼                        ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   STTProvider   │      │   LLMProvider   │      │   TTSProvider   │
│ (Qwen / Custom) │      │ (OpenAI / Qwen) │      │ (Qwen / Custom) │
└─────────────────┘      └─────────────────┘      └─────────────────┘
```

- **`cmd/agent`**: The daemon process connecting to a LiveKit room.
- **`cmd/testclient`**: An interactive CLI test harness to exercise text, voice, and live call modes against a running agent.
- **`internal/engine`**: Turn controller, VAD speech detection, utterance coalescing, sentence chunking, and playback pacing.
- **`internal/livekit`**: Subscribes to participant tracks, translates audio frames to Opus/PCM, and implements engine audio sinks.
- **`internal/provider`**: Provider registry and adapters.
- **`internal/telemetry`**: Context-propagated distributed tracing, latency spans, and log handlers.

---

## Prerequisites

- **Go**: 1.22+ (tested on Go 1.26.1)
- **Native Audio Libraries** (required for cgo Opus bindings on host):
  - **macOS**: `brew install opus opusfile pkgconf`
  - **Debian / Ubuntu**: `apt-get install libopus-dev libopusfile-dev pkg-config`
- **Docker & Docker Compose** (optional, for running LiveKit and observability locally)

---

## Quickstart

### 1. Configuration

Copy the example environment configuration:

```bash
cp .env.example .env
```

Edit `.env` to configure your API keys (e.g., `DASHSCOPE_API_KEY`, `OPENAI_API_KEY`) and LiveKit server credentials.

### 2. Run with Docker Compose

Run the entire stack including LiveKit dev server, agent, and Grafana observability:

```bash
docker compose up --build
```

- **LiveKit Server**: `http://localhost:7880`
- **Grafana Dashboard**: `http://localhost:3002`

### 3. Run Locally (Development)

Alternatively, run only the LiveKit server in Docker and execute the agent directly on your machine:

```bash
# Terminal 1: Start LiveKit dev server
docker compose up livekit-server

# Terminal 2: Run agent on host
go run ./cmd/agent -room voice-agent-demo
```

---

## Using the Test Client

The repository includes a test client (`cmd/testclient`) to test the agent across all three modes:

```bash
# Text message test
go run ./cmd/testclient text -room voice-agent-demo -msg "Hello! Tell me about yourself."

# Voice message test (WAV file in, WAV file reply out)
go run ./cmd/testclient voice -room voice-agent-demo -file sample.wav -out reply.wav

# Simulated live call (streams audio with simulated interruption)
go run ./cmd/testclient live -room voice-agent-demo -file sample.wav -out reply.wav \
    -interrupt-file interrupt.wav -interrupt-after 5
```

---

## Running Tests

Run the full test suite:

```bash
go test ./...
```

Run race detector on the engine state machine:

```bash
go test -race ./internal/engine -run TestLiveCallController_BargeIn
```

---

## Project Structure

```
t10-agent/
├── cmd/
│   ├── agent/             # Voice agent main entrypoint
│   └── testclient/        # CLI client for testing text, voice, and live calls
├── deploy/                # Deployment configs and Prometheus/Grafana provisions
├── docs/                  # Detailed design and UX specifications
│   ├── DESIGN.md          # Wire protocol and architecture deep dive
│   ├── LIVE-CALL-UX.md    # Real-time audio performance and barge-in invariants
│   └── TEXT-VOICE-UX.md   # Text and voice message UX guidelines
├── internal/
│   ├── audio/             # PCM/WAV processing, Opus codec bindings, and VAD
│   ├── config/            # Environment and runtime configuration
│   ├── engine/            # Transport-agnostic conversation & turn pipeline
│   ├── livekit/           # LiveKit room adapter and track subscriber
│   ├── provider/          # STT, LLM, and TTS provider registry & backends
│   ├── roomauth/          # LiveKit token generation
│   ├── roomprotocol/      # Shared topic and attribute definitions
│   └── telemetry/         # OpenTelemetry tracing, metrics, and correlation IDs
├── docker-compose.yml     # Local orchestration for LiveKit, Agent, and LGTM
├── Dockerfile             # Multi-stage production container build
└── go.mod
```

---

## License

This project is licensed under the [MIT License](LICENSE).
