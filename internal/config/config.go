// Package config loads voice-agent configuration from a .env file and the
// process environment.
package config

import (
	"bufio"
	"os"
	"strconv"
	"strings"
)

// LoadDotEnv reads KEY=VALUE pairs from path into the process environment,
// without overriding variables that are already set. Missing files are not
// an error (real env vars alone are a valid configuration).
func LoadDotEnv(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		value = strings.Trim(value, `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			os.Setenv(key, value)
		}
	}
	return scanner.Err()
}

// Config holds every knob the voice agent needs, all sourced from env vars.
type Config struct {
	// LiveKit
	LiveKitURL       string
	LiveKitAPIKey    string
	LiveKitAPISecret string

	// Provider selection (registry keys)
	STTProvider string
	LLMProvider string
	TTSProvider string

	// DashScope / Qwen realtime STT+TTS
	DashScopeAPIKey string
	DashScopeWSURL  string
	QwenSTTModel    string
	QwenTTSModel    string
	QwenTTSVoice    string

	// OpenAI speech (STT_PROVIDER / TTS_PROVIDER = openai). The key falls back
	// to OpenAIAPIKey, whose base URL is the LLM's and so is not shared.
	OpenAISpeechBaseURL string
	OpenAISpeechAPIKey  string
	OpenAISTTModel      string
	OpenAITTSModel      string
	OpenAITTSVoice      string

	// OpenAI-compatible LLM
	OpenAIBaseURL string
	OpenAIAPIKey  string
	OpenAIModel   string

	// agent-pi LLM. AgentPiWhenBusy is what a message does when the agent is
	// already working: "auto", "interrupt", "steer" or "followUp".
	AgentPiURL      string
	AgentURLs       string
	AgentRoutesFile string
	// AgentControlURL is t10-agent-control. When set, AgentRoutesFile and AgentURLs
	// aren't read.
	AgentControlURL string
	// AgentControlCache keeps the last routes from control, so a worker that
	// restarts while control is down still finds sessions.
	AgentControlCache string
	AgentPiProfile    string
	AgentPiWhenBusy   string
	// AgentPiSpeakProgress says the agent's acknowledgement before its answer.
	AgentPiSpeakProgress bool

	// Auth service credentials, which turn each caller's agent grant into a
	// token for agent-pi.
	AuthURL          string
	AuthClientID     string
	AuthClientSecret string

	// SpeechURL sends speech through ai-gateway-llm's realtime proxy, such as
	// ws://ai-gateway-llm:18080/v1/audio/realtime, in place of DashScope, so a
	// tenant's own speech key can pay for its calls. SpeechAPIKey is this
	// worker's client key there. Each socket carries the call's run token.
	SpeechURL    string
	SpeechAPIKey string

	RedisURL          string
	VoiceMaxCalls     int
	VoiceDrainSeconds int

	// The call greeting (docs/GREETING.md). VoiceGreetingMode is auto, agent,
	// fixed or off; VoiceGreeting is the local text, and agent mode's fallback.
	VoiceGreetingMode      string
	VoiceGreeting          string
	VoiceGreetingTimeoutMS int
	VoiceGreetingPrepare   bool

	// Live-call VAD tuning. VADBargeInMinSpeechMS is the speech-start
	// debounce used while the agent is already speaking — longer than
	// VADMinSpeechMS, because cutting a reply off mid-word should take real
	// speech rather than a cough or (there is no echo cancellation in this
	// pipeline) the agent's own voice coming back through an open speaker.
	VADEnergyThreshold    float64
	VADMinSpeechMS        int
	VADMinSilenceMS       int
	VADBargeInMinSpeechMS int

	// Live-call pipeline tuning — see engine.LiveConfig for what each one
	// actually controls.
	LivePreRollMS     int
	LiveMaxLeadMS     int
	TTSConcurrency    int
	TTSTimeoutMS      int
	LiveMinChunkRunes int
	LiveMaxChunkRunes int

	LogLevel string

	// OTELExporterEndpoint is an OTLP/gRPC host:port (e.g.
	// "otel-collector:4317", no scheme) to export traces and metrics to.
	// Empty disables tracing/metrics export entirely — see
	// internal/telemetry.SetupOTel and docker-compose.yml.
	OTELExporterEndpoint string
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getenvFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// Load builds a Config from the current process environment (call
// LoadDotEnv first to populate it from a .env file).
func Load() Config {
	return Config{
		LiveKitURL:       getenv("LIVEKIT_URL", "ws://localhost:7880"),
		LiveKitAPIKey:    getenv("LIVEKIT_API_KEY", "devkey"),
		LiveKitAPISecret: getenv("LIVEKIT_API_SECRET", ""),

		STTProvider: getenv("STT_PROVIDER", "qwen"),
		LLMProvider: getenv("LLM_PROVIDER", "openai_compat"),
		TTSProvider: getenv("TTS_PROVIDER", "qwen"),

		SpeechURL:       getenv("SPEECH_URL", ""),
		SpeechAPIKey:    getenv("SPEECH_API_KEY", ""),
		DashScopeAPIKey: getenv("DASHSCOPE_API_KEY", ""),
		DashScopeWSURL:  getenv("DASHSCOPE_WS_URL", "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"),
		QwenSTTModel:    getenv("QWEN_STT_MODEL", "qwen3-asr-flash-realtime"),
		QwenTTSModel:    getenv("QWEN_TTS_MODEL", "qwen3-tts-flash-realtime"),
		QwenTTSVoice:    getenv("QWEN_TTS_VOICE", "Cherry"),

		OpenAISpeechBaseURL: getenv("OPENAI_SPEECH_BASE_URL", "https://api.openai.com/v1"),
		OpenAISpeechAPIKey:  getenv("OPENAI_SPEECH_API_KEY", getenv("OPENAI_API_KEY", "")),
		OpenAISTTModel:      getenv("OPENAI_STT_MODEL", "gpt-4o-mini-transcribe"),
		OpenAITTSModel:      getenv("OPENAI_TTS_MODEL", "gpt-4o-mini-tts"),
		OpenAITTSVoice:      getenv("OPENAI_TTS_VOICE", "alloy"),

		OpenAIBaseURL: getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
		OpenAIAPIKey:  getenv("OPENAI_API_KEY", ""),
		OpenAIModel:   getenv("OPENAI_MODEL", "qwen-plus"),

		AgentPiURL:           getenv("AGENT_PI_URL", "http://localhost:9800/agent"),
		AgentURLs:            getenv("AGENT_URLS", ""),
		AgentRoutesFile:      getenv("AGENT_ROUTES_FILE", ""),
		AgentControlURL:      getenv("AGENT_CONTROL_URL", ""),
		AgentControlCache:    getenv("AGENT_CONTROL_CACHE", ""),
		AgentPiProfile:       getenv("AGENT_PI_PROFILE", "voice"),
		AgentPiWhenBusy:      getenv("AGENT_PI_WHEN_BUSY", "auto"),
		AgentPiSpeakProgress: getenv("AGENT_PI_SPEAK_PROGRESS", "true") != "false",

		AuthURL:          getenv("AUTH_URL", "http://localhost:8081"),
		AuthClientID:     getenv("AUTH_CLIENT_ID", "t10-livekit-agent"),
		AuthClientSecret: getenv("AUTH_CLIENT_SECRET", ""),

		RedisURL:          getenv("REDIS_URL", ""),
		VoiceMaxCalls:     getenvInt("VOICE_MAX_CALLS", 0),
		VoiceDrainSeconds: getenvInt("VOICE_DRAIN_SECONDS", 600),

		VoiceGreetingMode:      getenv("VOICE_GREETING_MODE", "auto"),
		VoiceGreeting:          getenv("VOICE_GREETING", ""),
		VoiceGreetingTimeoutMS: getenvInt("VOICE_GREETING_TIMEOUT_MS", 4000),
		VoiceGreetingPrepare:   getenv("VOICE_GREETING_PREPARE", "true") != "false",

		VADEnergyThreshold:    getenvFloat("VAD_ENERGY_THRESHOLD", 500.0),
		VADMinSpeechMS:        getenvInt("VAD_MIN_SPEECH_MS", 200),
		VADMinSilenceMS:       getenvInt("VAD_MIN_SILENCE_MS", 700),
		VADBargeInMinSpeechMS: getenvInt("VAD_BARGE_IN_MIN_SPEECH_MS", 400),

		LivePreRollMS:     getenvInt("LIVE_PREROLL_MS", 400),
		LiveMaxLeadMS:     getenvInt("LIVE_MAX_LEAD_MS", 700),
		TTSConcurrency:    getenvInt("TTS_CONCURRENCY", 2),
		TTSTimeoutMS:      getenvInt("TTS_TIMEOUT_MS", 8000),
		LiveMinChunkRunes: getenvInt("LIVE_MIN_CHUNK_RUNES", 40),
		LiveMaxChunkRunes: getenvInt("LIVE_MAX_CHUNK_RUNES", 180),

		LogLevel: getenv("LOG_LEVEL", "info"),

		OTELExporterEndpoint: getenv("OTEL_EXPORTER_OTLP_ENDPOINT", ""),
	}
}
