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
	// Claims says which rooms are this worker's.
	Claims Claims
	// MaxCalls is how many rooms this worker serves at once, or 0 for no limit.
	MaxCalls int
	// Drain is how long calls may run on after shutdown starts.
	Drain time.Duration
}

// stay is the agent's time in one room.
type stay interface {
	Callers() int
	Leave()
}

// roomServer keeps the agent in every room it can claim that has someone in it.
// Polling rather than LiveKit webhooks means no server config, and a restarted
// agent finds the calls already under way.
type roomServer struct {
	list func(context.Context) ([]*lkproto.Room, error)
	// waiting says whether anyone but the agent is in a room. The room list can't:
	// its count lags by seconds, which would leave a new caller waiting and bring
	// the agent back to a room only it just left.
	waiting func(ctx context.Context, name string) (bool, error)
	join    func(name string) (stay, error)
	claims  Claims
	// maxCalls is 0 for no limit.
	maxCalls int

	mu          sync.Mutex
	rooms       map[string]*visit
	failed      bool
	claimFailed bool
	draining    bool
	// departing lets shutdown wait for rooms already on their way out, so a claim
	// isn't left to lapse.
	departing sync.WaitGroup
}

// visit's conn is nil while joining.
type visit struct {
	conn     stay
	occupied time.Time
	// counted means it takes up one of maxCalls, and claimed that it's renewed.
	counted bool
	claimed bool
}

// Serve runs until ctx ends, then stops claiming, lets calls finish for up to
// opts.Drain, and leaves. Leaving releases the room, so another worker picks up
// any call left within a poll. ctx ending doesn't end the work, which needs its
// LiveKit and Redis calls to keep working during the drain.
func Serve(ctx context.Context, opts ServeOptions) {
	work, stopWork := context.WithCancel(context.WithoutCancel(ctx))
	defer stopWork()
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
		claims:   opts.Claims,
		maxCalls: opts.MaxCalls,
		rooms:    map[string]*visit{},
	}
	go server.holdClaims(work)
	ticker := time.NewTicker(roomPoll)
	defer ticker.Stop()
	for {
		server.reconcile(work, time.Now())
		select {
		case <-ticker.C:
		case <-ctx.Done():
			server.drain(work, ticker.C, time.After(opts.Drain))
			server.leaveAll(work)
			return
		}
	}
}

// drain runs the rooms until they empty or limit fires.
func (s *roomServer) drain(ctx context.Context, tick <-chan time.Time, limit <-chan time.Time) {
	s.mu.Lock()
	s.draining = true
	calls := len(s.rooms)
	s.mu.Unlock()
	slog.InfoContext(ctx, "rooms: draining", "rooms", calls)
	for calls > 0 {
		select {
		case now := <-tick:
			s.reconcile(ctx, now)
		case <-limit:
			slog.WarnContext(ctx, "rooms: drain limit reached", "rooms", calls)
			return
		}
		s.mu.Lock()
		calls = len(s.rooms)
		s.mu.Unlock()
	}
}

// holdClaims renews until ctx ends. Redis being down costs nothing here: a call
// under way keeps going, and if its claim lapses, LiveKit keeps the newer of two
// workers.
func (s *roomServer) holdClaims(ctx context.Context) {
	ticker := time.NewTicker(claimRenew)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.renew(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *roomServer) renew(ctx context.Context) {
	s.mu.Lock()
	var names []string
	for name, room := range s.rooms {
		if room.claimed {
			names = append(names, name)
		}
	}
	s.mu.Unlock()
	for _, name := range names {
		held, err := s.claims.Renew(ctx, name)
		if err != nil {
			s.noteClaimError(ctx, err)
			continue
		}
		s.noteClaimError(ctx, nil)
		if !held {
			s.lost(ctx, name)
		}
	}
}

// lost leaves a room another worker has taken. Unjoined, the join's end does it.
func (s *roomServer) lost(ctx context.Context, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	room := s.rooms[name]
	if room == nil {
		return
	}
	delete(s.rooms, name)
	if room.conn != nil {
		go room.conn.Leave()
	}
	slog.WarnContext(ctx, "rooms: claim lost", "room", name)
}

// noteClaimError logs a Redis failure once, not once per room per poll.
func (s *roomServer) noteClaimError(ctx context.Context, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil && !s.claimFailed {
		slog.ErrorContext(ctx, "rooms: can't reach claims", "err", err)
	}
	s.claimFailed = err != nil
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
			s.departing.Go(func() { s.depart(ctx, name, room.conn) })
			slog.InfoContext(ctx, "rooms: left", "room", name)
		}
	}
	if s.draining {
		return
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
	if err == nil && waiting && s.takeClaim(ctx, name) {
		conn, err = s.join(name)
		if err != nil {
			s.release(ctx, name)
		}
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
		// Shut down, or lost the claim, during the join, so nothing else will
		// leave this room.
		s.departing.Go(func() { s.depart(ctx, name, conn) })
		return
	}
	room.conn = conn
	slog.InfoContext(ctx, "rooms: joined", "room", name, "conversation", ConversationOf(name))
}

// takeClaim reports whether this worker should join. A busier worker waits a
// little first, so calls lean towards idle workers.
func (s *roomServer) takeClaim(ctx context.Context, name string) bool {
	wait, ok := s.reserve(name)
	if !ok {
		return false
	}
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		return false
	}
	claimed, err := s.claims.Claim(ctx, name)
	s.noteClaimError(ctx, err)
	if !claimed {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if room := s.rooms[name]; room != nil {
		room.claimed = true
	}
	return true
}

// reserve holds one of maxCalls, and says how long to wait before claiming. Only
// rooms with a caller count: most listed rooms stop here or sooner.
func (s *roomServer) reserve(name string) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	calls := 0
	for _, room := range s.rooms {
		if room.counted {
			calls++
		}
	}
	room := s.rooms[name]
	if s.draining || room == nil || (s.maxCalls > 0 && calls >= s.maxCalls) {
		return 0, false
	}
	room.counted = true
	if s.maxCalls == 0 {
		return 0, true
	}
	return claimSpread * time.Duration(calls) / time.Duration(s.maxCalls), true
}

// depart leaves first, so the next worker never arrives to an evicting overlap.
func (s *roomServer) depart(ctx context.Context, name string, conn stay) {
	conn.Leave()
	s.release(ctx, name)
}

func (s *roomServer) release(ctx context.Context, name string) {
	if err := s.claims.Release(ctx, name); err != nil {
		// The claim lapses by itself.
		slog.WarnContext(ctx, "rooms: can't release claim", "room", name, "err", err)
	}
}

func (s *roomServer) leaveAll(ctx context.Context) {
	s.mu.Lock()
	for name, room := range s.rooms {
		if room.conn != nil {
			s.departing.Go(func() { s.depart(ctx, name, room.conn) })
		}
		delete(s.rooms, name)
	}
	s.mu.Unlock()
	s.departing.Wait()
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
