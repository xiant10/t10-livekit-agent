package livekit

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func twoWorkers(t *testing.T) (*miniredis.Miniredis, Claims, Claims) {
	t.Helper()
	server := miniredis.RunT(t)
	first, err := NewClaims("redis://" + server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewClaims("redis://" + server.Addr())
	if err != nil {
		t.Fatal(err)
	}
	return server, first, second
}

func TestOneWorkerHoldsARoom(t *testing.T) {
	ctx := context.Background()
	_, first, second := twoWorkers(t)
	if held, err := first.Claim(ctx, "a"); err != nil || !held {
		t.Fatalf("first claim = %v, %v", held, err)
	}
	if held, err := second.Claim(ctx, "a"); err != nil || held {
		t.Fatalf("second claim = %v, %v", held, err)
	}
	if held, err := second.Renew(ctx, "a"); err != nil || held {
		t.Fatalf("second renew = %v, %v", held, err)
	}
	if err := second.Release(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if held, _ := second.Claim(ctx, "a"); held {
		t.Fatal("a worker released another's claim")
	}
	if err := first.Release(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if held, _ := second.Claim(ctx, "a"); !held {
		t.Fatal("a released room can't be claimed")
	}
}

func TestACrashedWorkersClaimLapses(t *testing.T) {
	ctx := context.Background()
	server, first, second := twoWorkers(t)
	if held, _ := first.Claim(ctx, "a"); !held {
		t.Fatal("no claim")
	}
	server.FastForward(claimTTL + time.Second)
	if held, _ := second.Claim(ctx, "a"); !held {
		t.Fatal("the claim outlived its worker")
	}
}

func TestRenewKeepsAClaimAndSurvivesLosingRedisData(t *testing.T) {
	ctx := context.Background()
	server, first, second := twoWorkers(t)
	if held, _ := first.Claim(ctx, "a"); !held {
		t.Fatal("no claim")
	}
	server.FastForward(claimTTL - time.Second)
	if held, err := first.Renew(ctx, "a"); err != nil || !held {
		t.Fatalf("renew = %v, %v", held, err)
	}
	server.FastForward(claimTTL - time.Second)
	if held, _ := second.Claim(ctx, "a"); held {
		t.Fatal("a renewed claim lapsed")
	}
	server.FlushAll()
	if held, err := first.Renew(ctx, "a"); err != nil || !held {
		t.Fatalf("renew after data loss = %v, %v", held, err)
	}
}

// fakeClaims refuses what's in taken, and records what it's asked.
type fakeClaims struct {
	soloClaims
	mu       sync.Mutex
	taken    map[string]bool
	err      error
	released []string
}

func (f *fakeClaims) refuse(room string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if room != "" {
		f.taken[room] = true
	}
	f.err = err
}

func (f *fakeClaims) releases() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.released)
}

func (f *fakeClaims) Claim(_ context.Context, room string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.taken[room] && f.err == nil, f.err
}

func (f *fakeClaims) Renew(_ context.Context, room string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return !f.taken[room], f.err
}

func (f *fakeClaims) Release(_ context.Context, room string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released = append(f.released, room)
	return nil
}

func TestJoinsOnlyWhatItClaims(t *testing.T) {
	f, s := newFakeRooms()
	s.claims = &fakeClaims{taken: map[string]bool{"theirs": true}}
	f.set(occupied("theirs"), occupied("ours"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "theirs")
	settled(t, s, "ours")
	if f.joined("theirs") != 0 || f.joined("ours") != 1 {
		t.Fatalf("joins = %v", f.joins)
	}
}

func TestClaimsNothingNewWithoutRedis(t *testing.T) {
	f, s := newFakeRooms()
	s.claims = &fakeClaims{err: errors.New("redis down")}
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	if f.joined("a") != 0 {
		t.Fatal("joined without a claim")
	}
	if len(s.rooms) != 0 {
		t.Fatal("kept a room it can't claim")
	}
}

func TestStopsClaimingAtMaxCalls(t *testing.T) {
	f, s := newFakeRooms()
	s.maxCalls = 1
	f.set(occupied("a"))
	now := time.Now()
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	f.set(occupied("a"), occupied("b"))
	s.reconcile(context.Background(), now.Add(time.Second))
	settled(t, s, "b")
	if f.joined("a") != 1 || f.joined("b") != 0 {
		t.Fatalf("joins = %v", f.joins)
	}
}

func TestABusierWorkerWaitsLonger(t *testing.T) {
	_, s := newFakeRooms()
	s.maxCalls = 4
	s.rooms["a"], s.rooms["b"], s.rooms["c"] = &visit{counted: true}, &visit{counted: true}, &visit{}
	wait, ok := s.reserve("c")
	if !ok || wait != claimSpread/2 {
		t.Fatalf("wait = %v, %v", wait, ok)
	}
}

func TestLeavesAndReleasesWhenTheCallEnds(t *testing.T) {
	f, s := newFakeRooms()
	claims := &fakeClaims{}
	s.claims = claims
	f.set(occupied("a"))
	now := time.Now()
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	f.mu.Lock()
	f.stays["a"].callers.Store(0)
	f.mu.Unlock()
	s.reconcile(context.Background(), now.Add(roomGrace))
	deadline := time.Now().Add(time.Second)
	for claims.releases() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !f.stays["a"].left.Load() || claims.releases() != 1 {
		t.Fatalf("left = %v, releases = %d", f.stays["a"].left.Load(), claims.releases())
	}
}

func TestLeavesARoomAnotherWorkerTook(t *testing.T) {
	f, s := newFakeRooms()
	claims := &fakeClaims{taken: map[string]bool{}}
	s.claims = claims
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	claims.refuse("a", nil)
	s.renew(context.Background())
	deadline := time.Now().Add(time.Second)
	for !f.stays["a"].left.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !f.stays["a"].left.Load() {
		t.Fatal("stayed in a room it lost")
	}
}

func TestRedisFailureLeavesCallsAlone(t *testing.T) {
	f, s := newFakeRooms()
	claims := &fakeClaims{}
	s.claims = claims
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	claims.refuse("", errors.New("redis down"))
	s.renew(context.Background())
	if f.stays["a"].left.Load() {
		t.Fatal("left a call because Redis failed")
	}
}

func TestDrainTakesNoNewRoomAndWaitsForCalls(t *testing.T) {
	f, s := newFakeRooms()
	f.set(occupied("a"))
	now := time.Now()
	s.reconcile(context.Background(), now)
	settled(t, s, "a")
	f.set(occupied("a"), occupied("late"))
	tick := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		s.drain(context.Background(), tick, nil)
		close(done)
	}()
	tick <- now.Add(time.Second)
	if f.joined("late") != 0 {
		t.Fatal("took a room while draining")
	}
	f.mu.Lock()
	f.stays["a"].callers.Store(0)
	f.mu.Unlock()
	tick <- now.Add(roomGrace + 2*time.Second)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("drain outlived its last call")
	}
}

func TestDrainStopsAtItsLimit(t *testing.T) {
	f, s := newFakeRooms()
	f.set(occupied("a"))
	s.reconcile(context.Background(), time.Now())
	settled(t, s, "a")
	limit := make(chan time.Time, 1)
	limit <- time.Now()
	s.drain(context.Background(), nil, limit)
	if !s.draining || len(s.rooms) != 1 {
		t.Fatalf("draining = %v, rooms = %d", s.draining, len(s.rooms))
	}
}
