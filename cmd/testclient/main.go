// Command testclient exercises the voice agent's three modes against a
// running agent process: text chat, voice-message upload, and a file-driven
// simulated live call.
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/roomauth"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	if err := config.LoadDotEnv(".env"); err != nil {
		fatal("load .env", err)
	}
	cfg := config.Load()

	switch os.Args[1] {
	case "text":
		runText(cfg, os.Args[2:])
	case "voice":
		runVoice(cfg, os.Args[2:])
	case "live":
		runLive(cfg, os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Println("usage: testclient <text|voice|live> [flags]")
	fmt.Println("  text  -room NAME -msg TEXT")
	fmt.Println("  voice -room NAME -file input.wav -out reply.wav")
	fmt.Println("  live  -room NAME -file input.wav -out reply.wav")
}

func fatal(what string, err error) {
	slog.Error("fatal", "what", what, "err", err)
	os.Exit(1)
}

// connect mints a fresh join token for identity and connects to room.
func connect(cfg config.Config, roomName, identity string, cb *lksdk.RoomCallback) (*lksdk.Room, error) {
	token, err := roomauth.MintToken(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, identity, roomName, time.Hour)
	if err != nil {
		return nil, fmt.Errorf("mint token: %w", err)
	}
	return lksdk.ConnectToRoomWithToken(cfg.LiveKitURL, token, cb, lksdk.WithAutoSubscribe(true))
}
