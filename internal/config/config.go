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

	// OpenAI-compatible LLM
	OpenAIBaseURL string
	OpenAIAPIKey  string
	OpenAIModel   string

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

		DashScopeAPIKey: getenv("DASHSCOPE_API_KEY", ""),
		DashScopeWSURL:  getenv("DASHSCOPE_WS_URL", "wss://dashscope.aliyuncs.com/api-ws/v1/realtime"),
		QwenSTTModel:    getenv("QWEN_STT_MODEL", "qwen3-asr-flash-realtime"),
		QwenTTSModel:    getenv("QWEN_TTS_MODEL", "qwen3-tts-flash-realtime"),
		QwenTTSVoice:    getenv("QWEN_TTS_VOICE", "Cherry"),

		OpenAIBaseURL: getenv("OPENAI_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
		OpenAIAPIKey:  getenv("OPENAI_API_KEY", ""),
		OpenAIModel:   getenv("OPENAI_MODEL", "qwen-plus"),

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
