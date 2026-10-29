package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/roomprotocol"
)

func runText(cfg config.Config, args []string) {
	fs := flag.NewFlagSet("text", flag.ExitOnError)
	roomName := fs.String("room", "voice-agent-demo", "LiveKit room to join")
	msg := fs.String("msg", "Hello!", "message to send")
	_ = fs.Parse(args)

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

	err = r.RegisterTextStreamHandler(roomprotocol.TopicChat, func(reader *lksdk.TextStreamReader, _ string) {
		fmt.Print("agent: ")
		_, _ = io.Copy(os.Stdout, reader)
		fmt.Println()
		result.textDone()
	})
	if err != nil {
		fatal("register text handler", err)
	}

	fmt.Println("you:", *msg)
	r.LocalParticipant.SendText(*msg, lksdk.StreamTextOptions{Topic: roomprotocol.TopicChat})

	select {
	case <-done:
	case <-time.After(60 * time.Second):
		fmt.Println("(timed out waiting for a reply)")
	}
}
