// Package client is the leader-chasing KV client shared by parallaxctl and
// parallax-bench. It holds one gRPC connection per configured address, tries
// the last-known leader first, and follows not_leader redirects (by address
// when hinted, else round-robin) until an operation is served or attempts
// run out. Every mutation carries a per-client (ClientID, Seq): retries
// reuse the same Seq so the server's session table dedups them.
package client

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iwang-1/parallax-kv/proto/kvpb"
)

// clientIDCounter hands out client session IDs. It is seeded from a random
// 48-bit base at process start so that two independent processes — e.g. two
// one-shot parallaxctl invocations — do not collide on the same (ClientID,
// Seq) pair, which the server's exactly-once session table would otherwise
// mistake for a retry and answer from cache without executing.
var clientIDCounter = randomIDBase()

// randomIDBase returns a random session-ID base in the high 48 bits, leaving
// the low 16 bits for the per-process counter.
func randomIDBase() uint64 {
	var b [8]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return uint64(time.Now().UnixNano()) << 16
	}
	return (binary.BigEndian.Uint64(b[:]) & 0xFFFFFFFFFFFF) << 16
}

// Client is a leader-chasing KV client. It is safe for use by one goroutine
// at a time (closed-loop); create one Client per concurrent caller.
type Client struct {
	clientID uint64
	seq      uint64

	conns  []*grpc.ClientConn
	stubs  []kvpb.KVServiceClient
	byAddr map[string]int
	leader int // index of the last address that served us
	maxTry int
}

// Dial connects to every address in addrs (lazily; gRPC dials on first use)
// and returns a ready Client.
func Dial(addrs []string) (*Client, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("client: no addresses")
	}
	c := &Client{
		clientID: atomic.AddUint64(&clientIDCounter, 1),
		byAddr:   make(map[string]int, len(addrs)),
		maxTry:   len(addrs) * 4,
	}
	for i, a := range addrs {
		conn, err := grpc.NewClient(a, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("client: dial %s: %w", a, err)
		}
		c.conns = append(c.conns, conn)
		c.stubs = append(c.stubs, kvpb.NewKVServiceClient(conn))
		c.byAddr[a] = i
	}
	return c, nil
}

// Close releases all connections.
func (c *Client) Close() {
	for _, conn := range c.conns {
		if conn != nil {
			_ = conn.Close()
		}
	}
}

// header returns the next request header, advancing the client's sequence.
func (c *Client) header() *kvpb.RequestHeader {
	c.seq++
	return &kvpb.RequestHeader{ClientId: c.clientID, Seq: c.seq}
}

// advanceLeader moves the leader pointer toward a redirect hint (by address
// if provided) or round-robins to the next node.
func (c *Client) advanceLeader(addr string) {
	if addr != "" {
		if idx, ok := c.byAddr[addr]; ok {
			c.leader = idx
			return
		}
	}
	c.leader = (c.leader + 1) % len(c.stubs)
}

// PutResult is the outcome of a Put.
type PutResult struct{ Version uint64 }

// Put sets key to value, chasing the leader and retrying on redirect.
func (c *Client) Put(ctx context.Context, key string, value []byte) (PutResult, error) {
	h := c.header()
	var last error
	for try := 0; try < c.maxTry; try++ {
		resp, err := c.stubs[c.leader].Put(ctx, &kvpb.PutRequest{Header: h, Key: key, Value: value})
		if err != nil {
			last = err
			c.advanceLeader("")
			if !sleepBackoff(ctx, try) {
				return PutResult{}, ctx.Err()
			}
			continue
		}
		if resp.GetHeader().GetNotLeader() {
			c.advanceLeader(resp.GetHeader().GetLeaderAddr())
			continue
		}
		return PutResult{Version: resp.GetVersion()}, nil
	}
	if last == nil {
		last = fmt.Errorf("client: no leader after %d attempts", c.maxTry)
	}
	return PutResult{}, last
}

// GetResult is the outcome of a Get.
type GetResult struct {
	Found   bool
	Value   []byte
	Version uint64
}

// Get reads key via the linearizable ReadIndex path.
func (c *Client) Get(ctx context.Context, key string) (GetResult, error) {
	// Reads are idempotent and take the ReadIndex path; the header's seq is
	// ignored by the server for Get but sent for uniformity.
	h := c.header()
	var last error
	for try := 0; try < c.maxTry; try++ {
		resp, err := c.stubs[c.leader].Get(ctx, &kvpb.GetRequest{Header: h, Key: key})
		if err != nil {
			last = err
			c.advanceLeader("")
			if !sleepBackoff(ctx, try) {
				return GetResult{}, ctx.Err()
			}
			continue
		}
		if resp.GetHeader().GetNotLeader() {
			c.advanceLeader(resp.GetHeader().GetLeaderAddr())
			continue
		}
		return GetResult{Found: resp.GetFound(), Value: resp.GetValue(), Version: resp.GetVersion()}, nil
	}
	if last == nil {
		last = fmt.Errorf("client: no leader after %d attempts", c.maxTry)
	}
	return GetResult{}, last
}

// Delete removes key, returning whether it existed.
func (c *Client) Delete(ctx context.Context, key string) (bool, error) {
	h := c.header()
	var last error
	for try := 0; try < c.maxTry; try++ {
		resp, err := c.stubs[c.leader].Delete(ctx, &kvpb.DeleteRequest{Header: h, Key: key})
		if err != nil {
			last = err
			c.advanceLeader("")
			if !sleepBackoff(ctx, try) {
				return false, ctx.Err()
			}
			continue
		}
		if resp.GetHeader().GetNotLeader() {
			c.advanceLeader(resp.GetHeader().GetLeaderAddr())
			continue
		}
		return resp.GetFound(), nil
	}
	if last == nil {
		last = fmt.Errorf("client: no leader after %d attempts", c.maxTry)
	}
	return false, last
}

// CasResult is the outcome of a CAS.
type CasResult struct {
	Swapped bool
	Current []byte
	Version uint64
}

// Cas sets key to value iff its current value equals expect. A nil expect
// with expectAbsent means "iff the key does not exist".
func (c *Client) Cas(ctx context.Context, key string, expect, value []byte, expectAbsent bool) (CasResult, error) {
	h := c.header()
	var last error
	for try := 0; try < c.maxTry; try++ {
		resp, err := c.stubs[c.leader].Cas(ctx, &kvpb.CasRequest{
			Header:       h,
			Key:          key,
			Expect:       expect,
			ExpectAbsent: expectAbsent,
			Value:        value,
		})
		if err != nil {
			last = err
			c.advanceLeader("")
			if !sleepBackoff(ctx, try) {
				return CasResult{}, ctx.Err()
			}
			continue
		}
		if resp.GetHeader().GetNotLeader() {
			c.advanceLeader(resp.GetHeader().GetLeaderAddr())
			continue
		}
		return CasResult{Swapped: resp.GetSwapped(), Current: resp.GetCurrent(), Version: resp.GetVersion()}, nil
	}
	if last == nil {
		last = fmt.Errorf("client: no leader after %d attempts", c.maxTry)
	}
	return CasResult{}, last
}

// sleepBackoff waits a short, capped interval between retries (used only on
// hard RPC errors, e.g. a node down mid-failover). It returns false if ctx
// expired during the wait.
func sleepBackoff(ctx context.Context, try int) bool {
	d := time.Duration(10*(try+1)) * time.Millisecond
	if d > 200*time.Millisecond {
		d = 200 * time.Millisecond
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
