package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"t10-livekit-agent/internal/provider"
)

type batchSTT struct {
	fakeSTTProvider
	text string
	err  error
}

func (s *batchSTT) TranscribeBatch(context.Context, []byte, string) (string, error) {
	return s.text, s.err
}

type voiceEvents struct{ events []string }

func (s *voiceEvents) WriteTranscript(text string) { s.events = append(s.events, "transcript:"+text) }
func (s *voiceEvents) WriteToken(text string)      { s.events = append(s.events, "reply:"+text) }
func (s *voiceEvents) Close()                      { s.events = append(s.events, "close") }

type voiceTTS struct {
	sink *voiceEvents
	err  error
}

func (s *voiceTTS) Synthesize(_ context.Context, text, voice string) ([]byte, int, error) {
	s.sink.events = append(s.sink.events, "tts:"+text+":"+voice)
	return []byte{1, 0}, 24000, s.err
}

type failingVoiceLLM struct{ err error }

func (l failingVoiceLLM) ChatStream(context.Context, []provider.Message, func(string)) (string, error) {
	return "", l.err
}

func TestRunVoiceTurn(t *testing.T) {
	failure := errors.New("provider unavailable")
	for _, tc := range []struct {
		name, transcript       string
		sttErr, llmErr, ttsErr error
		want                   []string
	}{
		{"success", " hello ", nil, nil, nil, []string{"transcript:hello", "reply:Hi", "tts:Hi:demo", "close"}},
		{"empty transcript", "  ", nil, nil, nil, []string{"transcript:(unintelligible)", "reply:Hi", "tts:Hi:demo", "close"}},
		{"stt failure", "", failure, nil, nil, []string{"close"}},
		{"llm failure", "hello", nil, failure, nil, []string{"transcript:hello", "close"}},
		{"tts failure", "hello", nil, nil, failure, []string{"transcript:hello", "reply:Hi", "tts:Hi:demo", "close"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &voiceEvents{}
			deps := &Deps{
				STT:           &batchSTT{text: tc.transcript, err: tc.sttErr},
				LLM:           &fakeLLM{tokens: []string{"Hi"}},
				TTS:           &voiceTTS{sink: sink, err: tc.ttsErr},
				Conversations: NewManager(""), Voice: "demo",
			}
			if tc.llmErr != nil {
				deps.LLM = failingVoiceLLM{tc.llmErr}
			}
			pcm, rate, err := RunVoiceTurn(context.Background(), deps, "alice", []byte{0, 0}, sink)
			if tc.sttErr != nil || tc.llmErr != nil || tc.ttsErr != nil {
				if !errors.Is(err, failure) || pcm != nil || rate != 0 {
					t.Fatalf("failure result: %v %d %v", pcm, rate, err)
				}
			} else if err != nil || rate != 24000 || !reflect.DeepEqual(pcm, []byte{1, 0}) {
				t.Fatalf("audio result: %v %d %v", pcm, rate, err)
			}
			if !reflect.DeepEqual(sink.events, tc.want) {
				t.Fatalf("events = %v, want %v", sink.events, tc.want)
			}
		})
	}
}
