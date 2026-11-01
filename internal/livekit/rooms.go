package livekit

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	lkproto "github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"

	"t10-livekit-agent/internal/engine"
	"t10-livekit-agent/internal/provider"
	"t10-livekit-agent/internal/roomauth"
)

const (
	// roomPoll bounds how long a caller waits for the agent to start joining.
	roomPoll = time.Second
	// roomGrace is how long the agent stays in a room with nobody else in it. It
	// also covers arriving after the only caller left.
	roomGrace = 5 * time.Second
	// roomTokenTTL only has to outlast joining: LiveKit refreshes a connected
	// participant's token.
	roomTokenTTL = time.Hour
)

// A workbench call is in the room conv-<session ID>, which admission prefixes
// with the tenant and the user. The name proves nothing by itself: agent-pi
// still decides, from the caller's own grant, whether the session is theirs.
var conversationRoom = regexp.MustCompile(`(?:^|__)conv-([A-Za-z0-9._-]+)$`)

// ConversationOf returns the conversation a room belongs to, or "".
func ConversationOf(room string) string {
	match := conversationRoom.FindStringSubmatch(room)
	if match == nil {
		return ""
	}
	return match[1]
}

// ServeOptions configures Serve.
type ServeOptions struct {
	URL          string
	APIKey       string
	APISecret    string
	Identity     string
	SystemPrompt string
	Deps         *engine.Deps
	Live         engine.LiveConfig
}

// stay is the agent's time in one room.
type stay interface {
	Callers() int
	Leave()
}

// roomServer keeps the agent in every room that has someone in it. Polling
// rather than LiveKit webhooks means no server config, and a restarted agent
// finds the calls already under way.
type roomServer struct {
	list func(context.Context) ([]*lkproto.Room, error)
	// waiting says whether anyone but the agent is in a room. The room list can't:
	// its count lags by seconds, which would leave a new caller waiting and bring
	// the agent back to a room only it just left.
	waiting func(ctx context.Context, name string) (bool, error)
	join    func(name string) (stay, error)

	mu     sync.Mutex
	rooms  map[string]*visit
	failed bool
}

// visit's conn is nil while joining.
type visit struct {
	conn     stay
	occupied time.Time
}

// Serve runs until ctx ends, then leaves every room.
func Serve(ctx context.Context, opts ServeOptions) {
	client := lksdk.NewRoomServiceClient(opts.URL, opts.APIKey, opts.APISecret)
	var joins atomic.Uint64
	server := &roomServer{
		list: func(ctx context.Context) ([]*lkproto.Room, error) {
			listed, err := client.ListRooms(ctx, &lkproto.ListRoomsRequest{})
			if err != nil {
				return nil, err
			}
			return listed.Rooms, nil
		},
		waiting: func(ctx context.Context, name string) (bool, error) {
			listed, err := client.ListParticipants(ctx, &lkproto.ListParticipantsRequest{Room: name})
			if err != nil {
				return false, err
			}
			for _, participant := range listed.Participants {
				if participant.GetIdentity() != opts.Identity {
					return true, nil
				}
			}
			return false, nil
		},
		join: func(name string) (stay, error) {
			return joinRoom(opts, name, strconv.FormatUint(joins.Add(1), 10))
		},
		rooms: map[string]*visit{},
	}
	ticker := time.NewTicker(roomPoll)
	defer ticker.Stop()
	for {
		server.reconcile(ctx, time.Now())
		select {
		case <-ticker.C:
		case <-ctx.Done():
			server.leaveAll()
			return
		}
	}
}

func (s *roomServer) reconcile(ctx context.Context, now time.Time) {
	listed, err := s.list(ctx)

	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		if !s.failed && ctx.Err() == nil {
			slog.ErrorContext(ctx, "rooms: can't list rooms", "err", err)
		}
		s.failed = true
	} else {
		s.failed = false
	}
	for name, room := range s.rooms {
		switch {
		case room.conn == nil:
		case room.conn.Callers() > 0:
			room.occupied = now
		case now.Sub(room.occupied) >= roomGrace:
			delete(s.rooms, name)
			go room.conn.Leave()
			slog.InfoContext(ctx, "rooms: left", "room", name)
		}
	}
	for _, room := range listed {
		name := room.GetName()
		if s.rooms[name] != nil {
			continue
		}
		s.rooms[name] = &visit{occupied: now}
		// One room that's slow to join mustn't hold up the rest.
		go s.enter(ctx, name)
	}
}

func (s *roomServer) enter(ctx context.Context, name string) {
	waiting, err := s.waiting(ctx, name)
	var conn stay
	if err == nil && waiting {
		conn, err = s.join(name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil && conn == nil {
		delete(s.rooms, name)
		return
	}
	if err != nil {
		// Forgotten, so the next poll tries again if the room is still there.
		delete(s.rooms, name)
		slog.ErrorContext(ctx, "rooms: can't join", "room", name, "err", err)
		return
	}
	room, ok := s.rooms[name]
	if !ok {
		// Shut down during the join, so nothing else will leave this room.
		conn.Leave()
		return
	}
	room.conn = conn
	slog.InfoContext(ctx, "rooms: joined", "room", name, "conversation", ConversationOf(name))
}

func (s *roomServer) leaveAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, room := range s.rooms {
		if room.conn != nil {
			room.conn.Leave()
		}
		delete(s.rooms, name)
	}
}

// roomStay is a joined room, and what the room's turns kept for it.
type roomStay struct {
	room    *lksdk.Room
	release func()
}

// Callers is 0 once the connection is gone: the SDK keeps its participants after
// a disconnect, and the agent has to leave so it can join again. A connection
// that's reconnecting isn't gone.
func (r *roomStay) Callers() int {
	if r.room.ConnectionState() == lksdk.ConnectionStateDisconnected {
		return 0
	}
	return len(r.room.GetRemoteParticipants())
}

func (r *roomStay) Leave() {
	r.room.Disconnect()
	r.release()
}

// joinRoom gives each stay its own history, because a person's conversations in
// two rooms are different conversations.
func joinRoom(opts ServeOptions, name, joinID string) (stay, error) {
	token, err := roomauth.MintToken(opts.APIKey, opts.APISecret, opts.Identity, name, roomTokenTTL)
	if err != nil {
		return nil, fmt.Errorf("mint token: %w", err)
	}
	llm := provider.ForRoom(opts.Deps.LLM, joinID, name, ConversationOf(name))
	deps := *opts.Deps
	deps.LLM = llm
	deps.Conversations = engine.NewManager(opts.SystemPrompt)
	room, err := Join(opts.URL, token, &deps, opts.Live)
	if err != nil {
		return nil, err
	}
	return &roomStay{room: room, release: llm.Release}, nil
}
