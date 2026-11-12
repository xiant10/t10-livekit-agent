// Package openaispeech implements provider.STTProvider and
// provider.TTSProvider against OpenAI's audio endpoints (or any server that
// speaks the same API): /audio/transcriptions and /audio/speech.
//
// Batch STT uses the endpoint; live STT streams audio over the Realtime
// transcription socket (stream.go) for interim captions, and falls back to the
// endpoint if the socket is unavailable. TTS streams: the raw PCM response
// body is handed to the caller as it arrives.
package openaispeech

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strings"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/provider"
)

func init() {
	provider.RegisterSTT("openai", func(cfg config.Config) (provider.STTProvider, error) {
		c, err := newClient(cfg)
		if err != nil {
			return nil, err
		}
		return &STT{client: c, model: cfg.OpenAISTTModel}, nil
	})
	provider.RegisterTTS("openai", func(cfg config.Config) (provider.TTSProvider, error) {
		c, err := newClient(cfg)
		if err != nil {
			return nil, err
		}
		return &TTS{client: c, model: cfg.OpenAITTSModel, defaultVoice: cfg.OpenAITTSVoice}, nil
	})
}

const (
	sttSampleRate = 16000
	// ttsSampleRate is what response_format "pcm" always returns: 24kHz mono
	// signed 16-bit little-endian.
	ttsSampleRate = 24000
)

type client struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func newClient(cfg config.Config) (*client, error) {
	if cfg.OpenAISpeechAPIKey == "" {
		return nil, fmt.Errorf("openaispeech: OPENAI_SPEECH_API_KEY or OPENAI_API_KEY is required")
	}
	return &client{
		baseURL: strings.TrimRight(cfg.OpenAISpeechBaseURL, "/"),
		apiKey:  cfg.OpenAISpeechAPIKey,
		// Each call carries its own context deadline.
		http: &http.Client{},
	}, nil
}

// do sends req and returns the response, turning a non-2xx status into an
// error that carries the server's message.
func (c *client) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode/100 != 2 {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		msg := strings.TrimSpace(string(b))
		var e struct {
			Error struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(b, &e) == nil && e.Error.Message != "" {
			msg = e.Error.Message
		}
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, msg)
	}
	return resp, nil
}

// STT is a provider.STTProvider backed by /audio/transcriptions.
type STT struct {
	client *client
	model  string
}

func (p *STT) TranscribeBatch(ctx context.Context, pcm16Mono16k []byte, lang string) (string, error) {
	if len(pcm16Mono16k) == 0 {
		return "", nil
	}
	wav := audio.EncodeWAV(audio.BytesToInt16(pcm16Mono16k), sttSampleRate, 1)

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	mw.WriteField("model", p.model)
	mw.WriteField("response_format", "json")
	if lang != "" {
		mw.WriteField("language", lang)
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", `form-data; name="file"; filename="speech.wav"`)
	h.Set("Content-Type", "audio/wav")
	fw, err := mw.CreatePart(h)
	if err != nil {
		return "", err
	}
	fw.Write(wav)
	if err := mw.Close(); err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.client.baseURL+"/audio/transcriptions", &body)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := p.client.do(req)
	if err != nil {
		return "", fmt.Errorf("openaispeech stt: %w", err)
	}
	defer resp.Body.Close()
	var out struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("openaispeech stt: decode: %w", err)
	}
	return strings.TrimSpace(out.Text), nil
}

// NewStream opens a Realtime transcription socket so onInterim can show the
// words while the user is still talking (see rtStream). It returns at once;
// the socket opens in the background while audio is already being buffered.
// Without one, Commit transcribes the whole utterance in a single request.
func (p *STT) NewStream(ctx context.Context, lang string, onInterim func(snapshot string)) (provider.STTStream, error) {
	s := &rtStream{
		stt:        p,
		lang:       lang,
		onInterim:  onInterim,
		notify:     make(chan struct{}, 1),
		stop:       make(chan struct{}),
		ready:      make(chan struct{}),
		senderDone: make(chan struct{}),
		items:      map[string]*item{},
		changed:    make(chan struct{}, 1),
	}
	go s.dial(ctx)
	return s, nil
}

// TTS is a provider.StreamingTTS backed by /audio/speech.
type TTS struct {
	client       *client
	model        string
	defaultVoice string
}

func (p *TTS) Synthesize(ctx context.Context, text, voice string) ([]byte, int, error) {
	var pcm []byte
	err := p.SynthesizeStream(ctx, text, voice, func(b []byte, _ int) { pcm = append(pcm, b...) })
	if err != nil {
		return nil, 0, err
	}
	return pcm, ttsSampleRate, nil
}

func (p *TTS) SynthesizeStream(ctx context.Context, text, voice string, onAudio func(pcm []byte, sampleRate int)) error {
	if voice == "" {
		voice = p.defaultVoice
	}
	body, err := json.Marshal(map[string]any{
		"model":           p.model,
		"input":           text,
		"voice":           voice,
		"response_format": "pcm",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.client.baseURL+"/audio/speech", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := p.client.do(req)
	if err != nil {
		return fmt.Errorf("openaispeech tts: %w", err)
	}
	defer resp.Body.Close()

	// Samples are two bytes: carry an odd trailing byte into the next read.
	buf := make([]byte, 8192)
	var carry []byte
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			chunk := append(carry, buf[:n]...)
			even := len(chunk) &^ 1
			if even > 0 {
				onAudio(append([]byte(nil), chunk[:even]...), ttsSampleRate)
			}
			carry = append([]byte(nil), chunk[even:]...)
		}
		if rerr == io.EOF {
			return nil
		}
		if rerr != nil {
			return fmt.Errorf("openaispeech tts: read: %w", rerr)
		}
	}
}

// Warm does nothing: each synthesis is one plain HTTP request.
func (p *TTS) Warm(ctx context.Context, voice string) {}

var _ provider.StreamingTTS = (*TTS)(nil)
