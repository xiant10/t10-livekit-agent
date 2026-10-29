package livekit

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"os"
	"strings"
	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/roomauth"
	"t10-livekit-agent/internal/roomprotocol"
	"testing"
	"time"
)

type smokeProviders struct{}

func (smokeProviders) ChatStream(_ context.Context, h []provider.Message, emit func(string)) (string, error) {
	if h[len(h)-1].Content == "fail" {
		return "", errors.New("private provider error")
	}
	emit("First sentence.")
	emit(" Second sentence.")
	return "First sentence. Second sentence.", nil
}
func (smokeProviders) TranscribeBatch(context.Context, []byte, string) (string, error) {
	return "voice question", nil
}
func (smokeProviders) NewStream(context.Context, string, func(string)) (provider.STTStream, error) {
	return nil, errors.New("unused")
}
func (smokeProviders) Synthesize(context.Context, string, string) ([]byte, int, error) {
	return []byte{1, 0, 2, 0}, 24000, nil
}

// Exercises the actual SDK's asynchronous writers and room routing. Providers
// are deterministic; no paid model calls, microphone or speaker is required.
func TestMessageRoomSmoke(t *testing.T) {
	if os.Getenv("T10_MESSAGE_SMOKE") != "1" {
		t.Skip("set T10_MESSAGE_SMOKE=1 with local LiveKit running")
	}
	if err := config.LoadDotEnv("../../.env"); err != nil {
		t.Fatal(err)
	}
	cfg := config.Load()
	roomName := fmt.Sprintf("message-smoke-%d", time.Now().UnixNano())
	token := func(identity string) string {
		v, err := roomauth.MintToken(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, identity, roomName, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	p := smokeProviders{}
	deps := &engine.Deps{STT: p, LLM: p, TTS: p, Conversations: engine.NewManager("")}
	agent, err := Join(cfg.LiveKitURL, token("agent"), deps, engine.LiveConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Disconnect()
	caller, err := lksdk.ConnectToRoomWithToken(cfg.LiveKitURL, token("caller"), lksdk.NewRoomCallback())
	if err != nil {
		t.Fatal(err)
	}
	defer caller.Disconnect()
	type event struct {
		id, kind, text string
		status         roomprotocol.MessageStatus
	}
	events := make(chan event, 100)
	if err := caller.RegisterTextStreamHandler(roomprotocol.TopicMessageStatus, func(r *lksdk.TextStreamReader, _ string) {
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			var status roomprotocol.MessageStatus
			if json.Unmarshal(scanner.Bytes(), &status) == nil {
				events <- event{id: r.Info.Attributes[roomprotocol.AttrRequestID], kind: "status", status: status}
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	for _, topic := range []string{roomprotocol.TopicChat, roomprotocol.TopicTranscript} {
		if err := caller.RegisterTextStreamHandler(topic, func(r *lksdk.TextStreamReader, _ string) {
			events <- event{id: r.Info.Attributes[roomprotocol.AttrRequestID], kind: r.Info.Topic, text: r.ReadAll()}
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := caller.RegisterByteStreamHandler(roomprotocol.TopicVoice, func(r *lksdk.ByteStreamReader, _ string) {
		pcm, rate, ch, err := audio.DecodeWAV(r.ReadAll())
		value := "valid"
		if err != nil || len(pcm) != 2 || rate != 24000 || ch != 1 {
			value = "invalid"
		}
		events <- event{id: r.Info.Attributes[roomprotocol.AttrRequestID], kind: "audio", text: value}
	}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id, text    string
		voice, fail bool
	}{{"text", "hello", false, false}, {"voice", "", true, false}, {"bad-wav", "", true, true}, {"failed-llm", "fail", false, true}} {
		t.Run(tc.id, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			attrs := map[string]string{roomprotocol.AttrRequestID: tc.id}
			if tc.voice {
				data := audio.EncodeWAV([]int16{1, 2, 3}, 16000, 1)
				if tc.fail {
					data = []byte("bad WAV")
				}
				w := caller.LocalParticipant.StreamBytes(lksdk.StreamBytesOptions{Topic: roomprotocol.TopicVoice, Attributes: attrs, MimeType: "audio/wav"})
				if err := writeMessage(ctx, w.Write, data); err != nil {
					t.Fatal(err)
				}
				w.Close()
			} else {
				caller.LocalParticipant.SendText(tc.text, lksdk.StreamTextOptions{Topic: roomprotocol.TopicChat, Attributes: attrs})
			}
			queued, terminal, text, heard := false, false, false, false
			audioCount, wantAudio := 0, 0
			for {
				select {
				case e := <-events:
					if e.id != tc.id {
						t.Fatalf("wrong request: %+v", e)
					}
					switch e.kind {
					case "status":
						if e.status.State == "queued" {
							queued = true
						}
						if e.status.State == "error" {
							if !tc.fail {
								t.Fatalf("unexpected error: %+v", e)
							}
							if !queued {
								t.Fatal("missing acknowledgment")
							}
							if strings.Contains(e.status.Message, "private") {
								t.Fatal("provider detail leaked")
							}
							return
						}
						if e.status.State == "done" {
							if tc.fail {
								t.Fatal("failure reported success")
							}
							terminal = true
							wantAudio = e.status.AudioChunks
						}
					case roomprotocol.TopicChat:
						if e.text != "First sentence. Second sentence." && !tc.fail {
							t.Fatalf("truncated text: %q", e.text)
						}
						text = true
					case roomprotocol.TopicTranscript:
						if e.text != "voice question" {
							t.Fatal(e.text)
						}
						heard = true
					case "audio":
						if e.text != "valid" {
							t.Fatal("bad audio")
						}
						audioCount++
					}
					if terminal && text && (!tc.voice || (heard && audioCount == wantAudio)) {
						if !queued {
							t.Fatal("missing acknowledgment")
						}
						if tc.voice && audioCount != 2 {
							t.Fatalf("audio chunks: %d", audioCount)
						}
						return
					}
				case <-ctx.Done():
					t.Fatalf("reply incomplete: terminal=%v text=%v heard=%v audio=%d/%d", terminal, text, heard, audioCount, wantAudio)
				}
			}
		})
	}
}
