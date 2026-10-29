package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"t10-livekit-agent/internal/audio"
	"t10-livekit-agent/internal/audio/opuscodec"
	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/roomprotocol"
)

// runLive simulates a live call: it publishes -file as if it were the
// tester's live microphone (paced at real playback speed, Opus-encoded),
// then records whatever audio track the agent publishes back and saves it
// to -out. Trailing silence is appended after the input so the agent's VAD
// actually observes end-of-speech and replies.
func runLive(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("live", flag.ExitOnError)
	roomName := fs.String("room", "voice-agent-demo", "LiveKit room to join")
	inFile := fs.String("file", "", "input WAV file to stream as simulated microphone audio")
	outFile := fs.String("out", "reply.wav", "where to save the agent's recorded spoken reply")
	trailingSilenceS := fs.Float64("trailing-silence", 2.0, "seconds of silence to send after the input finishes")
	interruptFile := fs.String("interrupt-file", "", "optional second WAV to stream mid-reply, to exercise barge-in")
	interruptAfterS := fs.Float64("interrupt-after", 0, "seconds after streaming starts to send -interrupt-file (requires -interrupt-file)")
	finalWaitS := fs.Float64("final-wait", 6.0, "seconds of trailing silence to keep streaming while waiting for the agent's reply")
	agentIdentity := fs.String("agent-identity", "agent", "identity of the agent participant, whose audio track is the one recorded")
	_ = fs.Parse(args)
	if *inFile == "" {
		fatal("live", fmt.Errorf("-file is required"))
	}

	samples48 := loadWAV48(*inFile)
	samples48 = append(samples48, make([]int16, int(*trailingSilenceS*opuscodec.SampleRate))...)

	var interruptSamples48 []int16
	if *interruptFile != "" {
		interruptSamples48 = loadWAV48(*interruptFile)
		interruptSamples48 = append(interruptSamples48, make([]int16, int(*trailingSilenceS*opuscodec.SampleRate))...)
	}

	encoder, err := opuscodec.NewEncoder()
	if err != nil {
		fatal("new opus encoder", err)
	}

	var (
		recMu    sync.Mutex
		recorded []int16
	)

	cb := lksdk.NewRoomCallback()
	cb.OnTrackSubscribed = func(track *webrtc.TrackRemote, _ *lksdk.RemoteTrackPublication, rp *lksdk.RemoteParticipant) {
		if track.Kind() != webrtc.RTPCodecTypeAudio {
			return
		}
		// Anyone else in the room (a real mic client running alongside this
		// one) publishes a track too, and recording it would mix their audio
		// into -out. Skip everything but the agent's.
		if rp.Identity() != *agentIdentity {
			fmt.Println("ignoring audio from", rp.Identity())
			return
		}
		fmt.Println("receiving agent audio from", rp.Identity())
		// One decoder per track, created here rather than shared: libopus
		// decoder state is not safe to use from two goroutines at once, and
		// this callback fires once per subscribed track.
		decoder, err := opuscodec.NewDecoder()
		if err != nil {
			fatal("new opus decoder", err)
		}
		go func() {
			for {
				pkt, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				pcm, err := decoder.DecodeFrame(pkt.Payload)
				if err != nil {
					continue
				}
				recMu.Lock()
				recorded = append(recorded, pcm...)
				recMu.Unlock()
			}
		}()
	}

	r, err := connect(cfg, *roomName, "tester", cb)
	if err != nil {
		fatal("connect", err)
	}
	defer r.Disconnect()

	// Live-call mode streams the STT transcript and the LLM reply text back
	// on the same two topics text/voice mode use, alongside the audio track
	// recorded above -- print both so a WAV-driven run can be checked for
	// this without needing a real mic/speaker client.
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicTranscript, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Println("[heard]", reader.ReadAll())
	}); err != nil {
		fatal("register transcript handler", err)
	}
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicState, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Printf("[state] %s\n", reader.ReadAll())
	}); err != nil {
		fatal("register state handler", err)
	}
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicChat, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Print("[reply] ")
		for {
			ch, _, err := reader.ReadRune()
			if err != nil {
				break
			}
			fmt.Print(string(ch))
		}
		fmt.Println()
	}); err != nil {
		fatal("register chat handler", err)
	}

	localTrack, err := lksdk.NewLocalSampleTrack(webrtc.RTPCodecCapability{
		MimeType:  webrtc.MimeTypeOpus,
		ClockRate: opuscodec.SampleRate,
		Channels:  opuscodec.RTPChannels,
	})
	if err != nil {
		fatal("create local track", err)
	}
	if _, err := r.LocalParticipant.PublishTrack(localTrack, &lksdk.TrackPublicationOptions{
		Name:   "tester-mic",
		Source: livekit.TrackSource_MICROPHONE,
	}); err != nil {
		fatal("publish local track", err)
	}

	ticker := time.NewTicker(opuscodec.FrameMS * time.Millisecond)
	defer ticker.Stop()

	start := time.Now()
	fmt.Printf("streaming %.1fs of simulated microphone audio...\n", float64(len(samples48))/opuscodec.SampleRate)
	streamFrames(localTrack, encoder, ticker, samples48)

	if len(interruptSamples48) > 0 {
		if wait := time.Until(start.Add(time.Duration(*interruptAfterS * float64(time.Second)))); wait > 0 {
			time.Sleep(wait)
		}
		fmt.Println("sending interrupt utterance (testing barge-in)...")
		streamFrames(localTrack, encoder, ticker, interruptSamples48)
	}

	fmt.Println("input finished, waiting for the agent's reply to finish...")
	streamFrames(localTrack, encoder, ticker, make([]int16, int(*finalWaitS*opuscodec.SampleRate)))

	recMu.Lock()
	out := audio.EncodeWAV(recorded, opuscodec.SampleRate, 1)
	recMu.Unlock()
	if err := os.WriteFile(*outFile, out, 0o644); err != nil {
		fatal("save reply wav", err)
	}
	fmt.Println("saved recorded reply to", *outFile)
}

// loadWAV48 decodes a WAV file to mono PCM16 at Opus's 48kHz RTP clock rate.
func loadWAV48(path string) []int16 {
	raw, err := os.ReadFile(path)
	if err != nil {
		fatal("read wav "+path, err)
	}
	samples, sampleRate, channels, err := audio.DecodeWAV(raw)
	if err != nil {
		fatal("decode wav "+path, err)
	}
	return audio.Resample(audio.DownmixToMono(samples, channels), sampleRate, opuscodec.SampleRate)
}

// streamFrames writes samples (48kHz mono) to track as real-time-paced
// 20ms Opus frames, using ticker to hold the wall-clock pace.
func streamFrames(track *lksdk.LocalTrack, encoder *opuscodec.Encoder, ticker *time.Ticker, samples []int16) {
	for off := 0; off < len(samples); off += opuscodec.FrameSamples {
		<-ticker.C
		end := min(off+opuscodec.FrameSamples, len(samples))
		frame, err := encoder.EncodeFrame(samples[off:end])
		if err != nil {
			continue
		}
		_ = track.WriteSample(media.Sample{Data: frame, Duration: opuscodec.FrameMS * time.Millisecond}, nil)
	}
}
