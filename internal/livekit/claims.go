package livekit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// claimTTL and claimRenew are starting values, from the design. A crashed
	// worker's calls wait for claimTTL, plus a poll, for another worker.
	claimTTL   = 15 * time.Second
	claimRenew = 5 * time.Second
	// claimTimeout bounds one Redis call, so that a slow Redis can't stall a join.
	claimTimeout = 2 * time.Second
	// claimSpread is the longest a nearly full worker waits before it claims. It
	// stays under roomPoll, so a full fleet's idle worker still gets there first.
	claimSpread = 500 * time.Millisecond
	claimPrefix = "t10:voice:"
)

// Claims decides which worker serves a room. Polling finds rooms; a claim stops
// every worker joining them, because all workers share one identity and LiveKit
// would have them evict each other.
type Claims interface {
	// Claim reports whether this worker now holds the room.
	Claim(ctx context.Context, room string) (bool, error)
	// Renew extends a held claim, and reports false if another worker took it.
	// An error says nothing about who holds it.
	Renew(ctx context.Context, room string) (bool, error)
	// Release gives up a claim, if this worker still holds it.
	Release(ctx context.Context, room string) error
}

// soloClaims is one worker alone, which needs no Redis.
type soloClaims struct{}

func (soloClaims) Claim(context.Context, string) (bool, error) { return true, nil }
func (soloClaims) Renew(context.Context, string) (bool, error) { return true, nil }
func (soloClaims) Release(context.Context, string) error       { return nil }

// A key that's gone was lost with Redis, not taken, so renewing puts it back.
// Otherwise a Redis restart would end every call.
var renewClaim = redis.NewScript(`
local holder = redis.call("GET", KEYS[1])
if holder == false or holder == ARGV[1] then
	redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
	return 1
end
return 0`)

var releaseClaim = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
	return redis.call("DEL", KEYS[1])
end
return 0`)

type redisClaims struct {
	client *redis.Client
	worker string
}

// NewClaims returns claims held in Redis, or a single worker's when redisURL is
// empty.
func NewClaims(redisURL string) (Claims, error) {
	if redisURL == "" {
		return soloClaims{}, nil
	}
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("REDIS_URL: %w", err)
	}
	worker, err := workerID()
	if err != nil {
		return nil, err
	}
	return &redisClaims{client: redis.NewClient(options), worker: worker}, nil
}

// workerID is unique per process, since a restart must not inherit claims.
func workerID() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		return "", err
	}
	return host + "-" + hex.EncodeToString(suffix), nil
}

func (c *redisClaims) Claim(ctx context.Context, room string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	err := c.client.SetArgs(ctx, claimPrefix+room, c.worker, redis.SetArgs{Mode: "NX", TTL: claimTTL}).Err()
	if err == redis.Nil {
		return false, nil
	}
	return err == nil, err
}

func (c *redisClaims) Renew(ctx context.Context, room string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	held, err := renewClaim.Run(ctx, c.client, []string{claimPrefix + room}, c.worker, claimTTL.Milliseconds()).Int()
	return held == 1, err
}

func (c *redisClaims) Release(ctx context.Context, room string) error {
	ctx, cancel := context.WithTimeout(ctx, claimTimeout)
	defer cancel()
	return releaseClaim.Run(ctx, c.client, []string{claimPrefix + room}, c.worker).Err()
}
