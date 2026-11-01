// Command testclient exercises the voice agent's three modes against a
// running agent process: text chat, voice-message upload, and a file-driven
// simulated live call.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/config"
	"t10-livekit-agent/internal/roomauth"
)

// agentWait covers the agent's poll for new rooms, with room for a slow join.
const agentWait = 10 * time.Second

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

// connect mints a fresh join token for identity and connects to room. With
// TESTCLIENT_USERNAME set it joins as that user instead, through the auth
// service's dev password grant and room admission, which is the only way to get
// the agent grant LLM_PROVIDER=agent_pi needs. roomName is then the short name:
// the room is then the tenant's namespaced one. The agent joins after the
// caller, so connect waits for it: a message sent before then reaches nobody.
func connect(cfg config.Config, roomName, identity string, cb *lksdk.RoomCallback) (*lksdk.Room, error) {
	var token string
	var err error
	if username := os.Getenv("TESTCLIENT_USERNAME"); username != "" {
		token, err = admit(cfg.AuthURL, username, os.Getenv("TESTCLIENT_PASSWORD"), roomName)
	} else {
		token, err = roomauth.MintToken(cfg.LiveKitAPIKey, cfg.LiveKitAPISecret, identity, roomName, time.Hour)
	}
	if err != nil {
		return nil, err
	}
	room, err := lksdk.ConnectToRoomWithToken(cfg.LiveKitURL, token, cb, lksdk.WithAutoSubscribe(true))
	if err != nil {
		return nil, err
	}
	for deadline := time.Now().Add(agentWait); room.GetParticipantByIdentity("agent") == nil; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			room.Disconnect()
			return nil, fmt.Errorf("the agent did not join %s within %s", roomName, agentWait)
		}
	}
	return room, nil
}

// admit signs in and returns a room token from the auth service.
func admit(authURL, username, password, room string) (string, error) {
	var login struct {
		AccessToken string `json:"access_token"`
	}
	err := postJSON(authURL+"/v1/token", "", map[string]string{
		"grant_type": "password", "username": username, "password": password,
	}, &login)
	if err != nil {
		return "", fmt.Errorf("sign in: %w", err)
	}
	var admission struct {
		RoomToken string `json:"room_token"`
	}
	if err := postJSON(authURL+"/v1/rooms/admission", login.AccessToken, map[string]string{"room": room}, &admission); err != nil {
		return "", fmt.Errorf("room admission: %w", err)
	}
	return admission.RoomToken, nil
}

func postJSON(url, bearer string, body, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
