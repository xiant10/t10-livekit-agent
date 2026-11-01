package livekit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	lkproto "github.com/livekit/protocol/livekit"
)

type fakeStay struct {
	callers atomic.Int32
	left    atomic.Bool
}

func (f *fakeStay) Callers() int { return int(f.callers.Load()) }
func (f *fakeStay) Leave()       { f.left.Store(true) }

type fakeRooms struct {
	mu     sync.Mutex
	listed []*lkproto.Room
	stays  map[string]*fakeStay
	joins  map[string]int
	refuse error
	wait   chan struct{}
	alone  map[string]bool
}

func newFakeRooms() (*fakeRooms, *roomServer) {
	f := &fakeRooms{stays: map[string]*fakeStay{}, joins: map[string]int{}, alone: map[string]bool{}}
	return f, &roomServer{
		rooms: map[string]*visit{},
		list: func(context.Context) ([]*lkproto.Room, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return f.listed, nil
		},
		waiting: func(_ context.Context, name string) (bool, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			return !f.alone[name], nil
		},
		join: func(name string) (stay, error) {
			f.mu.Lock()
			wait, refuse := f.wait, f.refuse
			f.joins[name]++
			f.mu.Unlock()
			if wait != nil {
				<-wait
			}
			if refuse != nil {
				return nil, refuse
			}
			conn := &fakeStay{}
			conn.callers.Store(1)
			f.mu.Lock()
			f.stays[name] = conn
			f.mu.Unlock()
			return conn, nil
		},
	}
}

func (f *fakeRooms) set(rooms ...*lkproto.Room) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listed = rooms
}

func (f *fakeRooms) joined(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joins[name]
}

func settled(t *testing.T, s *roomServer, name string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		room := s.rooms[name]
		done := room == nil || room.conn != nil
		s.mu.Unlock()
		if done {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("%s still joining", name)
}

func occupied(name string) *lkproto.Room {
	return &lkproto.Room{Name: name, NumParticipants: 1}
}

func TestConversationOf(t *testing.T) {
	cases := map[string]string{
		"ten_acme__conv-abc-123":      "abc-123",
		"ten_acme__usr_a__conv-a.b.3": "a.b.3",
		"conv-abc":                    "abc",
		"ten_acme__voice-agent-demo":  "",
		"voice-agent-demo":            "",
		"ten_acme__my-conv-abc":       "",
		"ten_acme__conv-":             "",
	}
	for room, want := range cases {
		if got := ConversationOf(room); got != want {
			t.Errorf("ConversationOf(%q) = %q, want %q", room, got, want)
		}
	}
}

func TestJoinsARoomWithSomeoneInItOnce(t *testing.T) {
	f, s := newFakeRooms()
	now := time.Now()
	f.alone["empty"] = true
	f.set(occupied("a"), &lkproto.Room{Name: "empty"})
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	settled(t, s, "empty")
	s.reconcile(context.Background(), now.Add(time.Second))
	settled(t, s, "a")
	settled(t, s, "empty")
	if f.joined("a") != 1 || f.joined("empty") != 0 {
		t.Fatalf("joins = %v", f.joins)
	}
}

func TestRoomCountsAreNotTrusted(t *testing.T) {
	f, s := newFakeRooms()
	f.set(&lkproto.Room{Name: "new", NumParticipants: 0}) // a caller, not counted yet
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "new")
	if f.joined("new") != 1 {
		t.Fatalf("left a new caller waiting: %d joins", f.joined("new"))
	}

	f.alone["a"] = true
	f.set(occupied("a")) // still counts someone who has left
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	if f.joined("a") != 0 {
		t.Fatalf("joined %d times, with nobody there", f.joined("a"))
	}
	f.alone["a"] = false
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	if f.joined("a") != 1 {
		t.Fatalf("a real caller got %d joins", f.joined("a"))
	}
}

func TestAPollWhileJoiningDoesNotJoinAgain(t *testing.T) {
	f, s := newFakeRooms()
	f.wait = make(chan struct{})
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	s.reconcile(context.Background(), time.Now())
	close(f.wait)
	settled(t, s, "a")
	if f.joined("a") != 1 {
		t.Fatalf("joined %d times", f.joined("a"))
	}
}

func TestLeavesOnlyAfterTheRoomStaysEmpty(t *testing.T) {
	f, s := newFakeRooms()
	now := time.Now()
	f.set(occupied("a"))
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	s.reconcile(context.Background(), now.Add(time.Second)) // seen occupied
	f.set()
	conn := f.stays["a"]
	conn.callers.Store(0)

	s.reconcile(context.Background(), now.Add(time.Second+roomGrace-time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	if conn.left.Load() {
		t.Fatal("left a room a caller could be coming back to")
	}
	s.reconcile(context.Background(), now.Add(time.Second+roomGrace))
	deadline := time.Now().Add(time.Second)
	for !conn.left.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !conn.left.Load() {
		t.Fatal("stayed in an empty room")
	}
}

func TestACallerBackInTimeKeepsTheAgent(t *testing.T) {
	f, s := newFakeRooms()
	now := time.Now()
	f.set(occupied("a"))
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	conn := f.stays["a"]
	conn.callers.Store(0)
	s.reconcile(context.Background(), now.Add(roomGrace-time.Millisecond))
	conn.callers.Store(1)
	s.reconcile(context.Background(), now.Add(roomGrace))
	conn.callers.Store(0)
	s.reconcile(context.Background(), now.Add(2*roomGrace-time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	if conn.left.Load() || f.joined("a") != 1 {
		t.Fatalf("left = %v, joins = %d", conn.left.Load(), f.joined("a"))
	}
}

func TestAFailedJoinIsTriedAgain(t *testing.T) {
	f, s := newFakeRooms()
	f.refuse = errors.New("refused")
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	f.mu.Lock()
	f.refuse = nil
	f.mu.Unlock()
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	if f.joined("a") != 2 || f.stays["a"] == nil {
		t.Fatalf("joins = %d", f.joined("a"))
	}
}

func TestAFailedListLeavesCurrentRoomsAlone(t *testing.T) {
	f, s := newFakeRooms()
	now := time.Now()
	f.set(occupied("a"))
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	s.list = func(context.Context) ([]*lkproto.Room, error) { return nil, errors.New("down") }
	s.reconcile(context.Background(), now.Add(time.Minute))
	time.Sleep(10 * time.Millisecond)
	if f.stays["a"].left.Load() {
		t.Fatal("left a live call because LiveKit's API hiccuped")
	}
}

func TestAJoinFinishingAfterShutdownLeaves(t *testing.T) {
	f, s := newFakeRooms()
	f.wait = make(chan struct{})
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	for f.joined("a") == 0 {
		time.Sleep(time.Millisecond)
	}
	s.leaveAll()
	close(f.wait)
	deadline := time.Now().Add(time.Second)
	for {
		f.mu.Lock()
		conn := f.stays["a"]
		f.mu.Unlock()
		if conn != nil && conn.left.Load() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the late join was never left")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestShutdownLeavesEveryRoom(t *testing.T) {
	f, s := newFakeRooms()
	f.set(occupied("a"), occupied("b"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	settled(t, s, "b")
	s.leaveAll()
	if !f.stays["a"].left.Load() || !f.stays["b"].left.Load() {
		t.Fatal("still in a room after shutting down")
	}
}
