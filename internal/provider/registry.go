package provider

import (
	"fmt"

	"t10-livekit-agent/internal/config"
)

// Factory constructs a provider instance from the process configuration.
// Concrete provider packages register themselves via RegisterSTT/RegisterLLM/
// RegisterTTS in an init() func, mirroring a plugin registry: swapping a
// model provider is then a one-line config change plus a blank import,
// never a change to session/pipeline code.
type (
	STTFactory func(cfg config.Config) (STTProvider, error)
	LLMFactory func(cfg config.Config) (LLMProvider, error)
	TTSFactory func(cfg config.Config) (TTSProvider, error)
)

var (
	sttFactories = map[string]STTFactory{}
	llmFactories = map[string]LLMFactory{}
	ttsFactories = map[string]TTSFactory{}
)

func RegisterSTT(name string, f STTFactory) { sttFactories[name] = f }
func RegisterLLM(name string, f LLMFactory) { llmFactories[name] = f }
func RegisterTTS(name string, f TTSFactory) { ttsFactories[name] = f }

func BuildSTT(name string, cfg config.Config) (STTProvider, error) {
	f, ok := sttFactories[name]
	if !ok {
		return nil, fmt.Errorf("provider: unknown stt provider %q", name)
	}
	return f(cfg)
}

func BuildLLM(name string, cfg config.Config) (LLMProvider, error) {
	f, ok := llmFactories[name]
	if !ok {
		return nil, fmt.Errorf("provider: unknown llm provider %q", name)
	}
	return f(cfg)
}

func BuildTTS(name string, cfg config.Config) (TTSProvider, error) {
	f, ok := ttsFactories[name]
	if !ok {
		return nil, fmt.Errorf("provider: unknown tts provider %q", name)
	}
	return f(cfg)
}
