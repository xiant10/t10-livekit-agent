// Command agent is the voice agent process: it joins one LiveKit room and
// serves text, voice-message, and live-call mode to whoever connects.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/livekit"
	"t10-livekit-agent/internal/provider"
	_ "t10-livekit-agent/internal/provider/openaicompat"
	_ "t10-livekit-agent/internal/provider/qwen"
	"t10-livekit-agent/internal/roomauth"
	"t10-livekit-agent/internal/telemetry"
)

// defaultSystemPrompt is written for the live-call case specifically,
// because that's the mode where a wordy model stops being a style problem
// and starts being a usability one. "Concise, helpful voice assistant" — the
// previous default — still produced eighteen-sentence answers that took
// three quarters of a minute to speak, and a caller who has to sit through
// that (or fight their way out of it with a barge-in) has effectively lost
// the turn. Everything here is aimed at replies short enough that the caller
// keeps control of the conversation.
const defaultSystemPrompt = "You are a voice assistant on a live phone call. " +
	"Your replies are spoken aloud, so keep them to one to three short sentences and then stop. " +
	"Never use markdown, bullet points, headings, emoji, or code — write numbers, dates, and symbols the way they are said out loud. " +
	"Always answer in the language the caller is speaking. " +
	"If a complete answer would be long, give the short version first and offer to go deeper. " +
	"Being interrupted is normal: when it happens, just answer the new question without remarking on it."

func main() {
	envPath := flag.String("env", ".env", "path to .env file")
	roomName := flag.String("room", "voice-agent-demo", "LiveKit room to join")
	identity := flag.String("identity", "agent", "agent participant identity")
	systemPrompt := flag.String("system-prompt", defaultSystemPrompt, "system prompt for the LLM")
	logPath := flag.String("log-file", "t10-agent.log", "diagnostic log file (empty disables file logging)")
	flag.Parse()

	if err := config.LoadDotEnv(*envPath); err != nil {
		slog.Error("failed to load .env", "path", *envPath, "err", err)
		os.Exit(1)
	}
	cfg := config.Load()

	logOutput := io.Writer(os.Stderr)
	if *logPath != "" {
		file, err := os.OpenFile(*logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "open diagnostic log: %v\n", err)
			os.Exit(1)
		}
		defer file.Close()
		logOutput = io.MultiWriter(os.Stderr, file)
		log.SetOutput(file) // LiveKit/Pion SDK use the standard logger.
	}

	// Wrap the base handler so any log call made with a context that
	// telemetry.StartSession stamped (one per text/voice message or live
	// call) automatically carries that request/session's correlation ID.
	baseHandler := slog.NewTextHandler(logOutput, &slog.HandlerOptions{Level: parseLogLevel(cfg.LogLevel)})
	slog.SetDefault(slog.New(telemetry.NewOTelLogHandler(telemetry.NewLogHandler(baseHandler))))

	// Wires every telemetry.StartSession/StartStage span and metric this
	// process produces to a real OTLP backend (see docker-compose.yml's
	// otel-collector service). Nothing to do if unset — every telemetry.*
	// call already degrades to a correctly-propagating no-op without it.
	if cfg.OTELExporterEndpoint != "" {
		shutdown, err := telemetry.SetupOTel(context.Background(), "t10-agent", cfg.OTELExporterEndpoint)
		if err != nil {
			slog.Error("otel setup failed; continuing without trace/metric export", "endpoint", cfg.OTELExporterEndpoint, "err", err)
		} else {
			slog.Info("otel export enabled", "endpoint", cfg.OTELExporterEndpoint)
			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := shutdown(shutdownCtx); err != nil {
					slog.Error("otel shutdown failed", "err", err)
				}
			}()
		}
	}

	sttProvider, err := provider.BuildSTT(cfg.STTProvider, cfg)
	if err != nil {
		fatal("stt provider", err)
	}
	llmProvider, err := provider.BuildLLM(cfg.LLMProvider, cfg)
	if err != nil {
		fatal("llm provider", err)
	}
	ttsProvider, err := provider.BuildTTS(cfg.TTSProvider, cfg)
	if err != nil {
		fatal("tts provider", err)
	}

	token, err := roomauth.MintToken(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, *identity, *roomName, 24*time.Hour)
	if err != nil {
		fatal("mint token", err)
	}

	deps := &engine.Deps{
		STT:           sttProvider,
		LLM:           llmProvider,
		TTS:           ttsProvider,
		Conversations: engine.NewManager(*systemPrompt),
		Voice:         cfg.QwenTTSVoice,
	}
	liveCfg := engine.LiveConfig{
		VAD: engine.VADConfig{
			EnergyThreshold:    cfg.VADEnergyThreshold,
			MinSpeechMS:        cfg.VADMinSpeechMS,
			MinSilenceMS:       cfg.VADMinSilenceMS,
			BargeInMinSpeechMS: cfg.VADBargeInMinSpeechMS,
		},
		PreRollMS:      cfg.LivePreRollMS,
		MaxLeadMS:      cfg.LiveMaxLeadMS,
		TTSConcurrency: cfg.TTSConcurrency,
		TTSTimeout:     time.Duration(cfg.TTSTimeoutMS) * time.Millisecond,
		MinChunkRunes:  cfg.LiveMinChunkRunes,
		MaxChunkRunes:  cfg.LiveMaxChunkRunes,
	}

	r, err := livekit.Join(cfg.LiveKitURL, token, deps, liveCfg)
	if err != nil {
		fatal("join room", err)
	}
	defer r.Disconnect()

	slog.Info("agent connected", "room", *roomName, "identity", *identity, "livekit_url", cfg.LiveKitURL,
		"stt", cfg.STTProvider, "llm", cfg.LLMProvider, "tts", cfg.TTSProvider, "model", cfg.OpenAIModel)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh
	slog.Info("shutting down")
}

func fatal(what string, err error) {
	slog.Error("fatal", "what", what, "err", err)
	os.Exit(1)
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
