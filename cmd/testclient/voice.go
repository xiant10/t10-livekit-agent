package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"t10-livekit-agent/internal/audio"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/roomprotocol"
)

func runVoice(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("voice", flag.ExitOnError)
	roomName := fs.String("room", "voice-agent-demo", "LiveKit room to join")
	inFile := fs.String("file", "", "input WAV file (16-bit PCM) to upload as a voice message")
	outFile := fs.String("out", "reply.wav", "where to save the agent's synthesized reply")
	_ = fs.Parse(args)
	if *inFile == "" {
		fatal("voice", fmt.Errorf("-file is required"))
	}

	result := &messageResult{done: make(chan struct{})}
	done := result.done
	cb := lksdk.NewRoomCallback()

	r, err := connect(cfg, *roomName, "tester", cb)
	if err != nil {
		fatal("connect", err)
	}
	defer r.Disconnect()
	if err := r.RegisterTextStreamHandler(roomprotocol.TopicMessageStatus, result.status); err != nil {
		fatal("register status", err)
	}

	err = r.RegisterTextStreamHandler(roomprotocol.TopicTranscript, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Println("heard:", reader.ReadAll())
	})
	if err != nil {
		fatal("register transcript handler", err)
	}

	err = r.RegisterTextStreamHandler(roomprotocol.TopicChat, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Print("agent: ")
		_, _ = io.Copy(os.Stdout, reader)
		fmt.Println()
		result.textDone()
	})
	if err != nil {
		fatal("register chat handler", err)
	}

	var audioMu sync.Mutex
	chunks := map[int][]int16{}
	sampleRate := 0
	err = r.RegisterByteStreamHandler(roomprotocol.TopicVoice, func(reader *lksdk.ByteStreamReader, _ string) {
		samples, rate, channels, err := audio.DecodeWAV(reader.ReadAll())
		if err != nil {
			fmt.Println("invalid reply audio:", err)
			return
		}
		index, err := strconv.Atoi(reader.Info.Attributes[roomprotocol.AttrChunkIndex])
		if err != nil {
			fmt.Println("invalid chunk index:", err)
			return
		}
		audioMu.Lock()
		if sampleRate == 0 {
			sampleRate = rate
		}
		chunks[index] = audio.ResampleIf(audio.DownmixToMono(samples, channels), rate, sampleRate)
		audioMu.Unlock()
		result.audioDone()
	})
	if err != nil {
		fatal("register voice handler", err)
	}

	fmt.Println("uploading", *inFile)
	if _, err := r.LocalParticipant.SendFile(*inFile, lksdk.StreamBytesOptions{
		Topic:    roomprotocol.TopicVoice,
		MimeType: "audio/wav",
	}); err != nil {
		fatal("upload voice message", err)
	}

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		fmt.Println("(timed out waiting for a reply)")
	}
	audioMu.Lock()
	defer audioMu.Unlock()
	var samples []int16
	for i := 0; i < len(chunks); i++ {
		samples = append(samples, chunks[i]...)
	}
	if len(samples) > 0 {
		if err := os.WriteFile(*outFile, audio.EncodeWAV(samples, sampleRate, 1), 0o644); err != nil {
			fmt.Println("failed to save audio:", err)
		} else {
			fmt.Println("saved reply audio to", *outFile)
		}
	}

}
