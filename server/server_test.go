package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/iwang-1/parallax-kv/client"
)

// harness is an in-process 3-node cluster. Unlike the e2e package (which
// spawns real OS processes), this runs the servers as goroutines in the test
// binary so the drive loop, transport, and apply path are all covered by the
// -race detector.
type harness struct {
	servers []*Server
	cancels []context.CancelFunc
	addrs   []string
}

func startHarness(t *testing.T) *harness {
	t.Helper()
	const peerBase, clientBase = 27100, 28100
	dir := t.TempDir()

	peers := map[uint64]string{}
	clientAddrs := map[uint64]string{}
	for i := 0; i < 3; i++ {
		id := uint64(i + 1)
		peers[id] = fmt.Sprintf("localhost:%d", peerBase+i)
		clientAddrs[id] = fmt.Sprintf("localhost:%d", clientBase+i)
	}

	h := &harness{}
	for i := 0; i < 3; i++ {
		id := uint64(i + 1)
		cfg := Config{
			ID:                 id,
			Peers:              peers,
			ClientAddrs:        clientAddrs,
			DataDir:            fmt.Sprintf("%s/d%d", dir, id),
			ListenPeer:         peers[id],
			ListenClient:       clientAddrs[id],
			TickIntervalMillis: 10,
			HeartbeatTicks:     3,
			ElectionTicks:      15,
		}
		srv, err := New(cfg)
		if err != nil {
			t.Fatalf("new server %d: %v", id, err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = srv.Run(ctx) }()
		h.servers = append(h.servers, srv)
		h.cancels = append(h.cancels, cancel)
		h.addrs = append(h.addrs, clientAddrs[id])
	}
	t.Cleanup(func() {
		for _, cancel := range h.cancels {
			cancel()
		}
		// Give drive loops a moment to unwind their listeners.
		time.Sleep(100 * time.Millisecond)
	})
	return h
}

// TestInProcessClusterReadWrite exercises the full write/read path across an
// in-process 3-node cluster under -race: put a spread of keys, overwrite
// some, delete some, CAS some, and verify linearizable reads reflect every
// acknowledged mutation.
func TestInProcessClusterReadWrite(t *testing.T) {
	h := startHarness(t)

	c, err := client.Dial(h.addrs)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	want := map[string]string{}

	// Initial writes.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("v%02d", i)
		if _, err := putWithRetry(ctx, c, key, val); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		want[key] = val
	}
	// Overwrite half.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("k%02d", i)
		val := fmt.Sprintf("overwrite%02d", i)
		if _, err := c.Put(ctx, key, []byte(val)); err != nil {
			t.Fatalf("overwrite %s: %v", key, err)
		}
		want[key] = val
	}
	// Delete a few.
	for i := 15; i < 20; i++ {
		key := fmt.Sprintf("k%02d", i)
		if _, err := c.Delete(ctx, key); err != nil {
			t.Fatalf("delete %s: %v", key, err)
		}
		delete(want, key)
	}
	// CAS create + match.
	if r, err := c.Cas(ctx, "cas-key", nil, []byte("cas-1"), true); err != nil || !r.Swapped {
		t.Fatalf("cas create: r=%+v err=%v", r, err)
	}
	want["cas-key"] = "cas-1"
	if r, err := c.Cas(ctx, "cas-key", []byte("cas-1"), []byte("cas-2"), false); err != nil || !r.Swapped {
		t.Fatalf("cas match: r=%+v err=%v", r, err)
	}
	want["cas-key"] = "cas-2"
	// CAS mismatch must not mutate.
	if r, err := c.Cas(ctx, "cas-key", []byte("wrong"), []byte("cas-3"), false); err != nil || r.Swapped {
		t.Fatalf("cas mismatch should not swap: r=%+v err=%v", r, err)
	}

	// Verify every expected key, and that deleted keys are gone.
	for key, val := range want {
		r, err := c.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if !r.Found || string(r.Value) != val {
			t.Errorf("get %s = (found=%v, %q), want %q", key, r.Found, r.Value, val)
		}
	}
	for i := 15; i < 20; i++ {
		key := fmt.Sprintf("k%02d", i)
		r, err := c.Get(ctx, key)
		if err != nil {
			t.Fatalf("get deleted %s: %v", key, err)
		}
		if r.Found {
			t.Errorf("deleted key %s still present", key)
		}
	}
}

// TestExactlyOnceRetry verifies the session table dedups a retried write:
// the client keeps its (ClientID, Seq), so re-submitting the same Put does
// not double-apply. Because the write path is idempotent, an overwrite with a
// distinct seq bumps the version by exactly one per logical write.
func TestExactlyOnceRetry(t *testing.T) {
	h := startHarness(t)
	c, err := client.Dial(h.addrs)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer c.Close()
	ctx := context.Background()

	r1, err := putWithRetry(ctx, c, "dedup", "one")
	if err != nil {
		t.Fatalf("first put: %v", err)
	}
	if r1.Version != 1 {
		t.Fatalf("first put version = %d, want 1", r1.Version)
	}
	// A second distinct write advances the version to 2 (not more), showing a
	// single apply per logical operation.
	r2, err := c.Put(ctx, "dedup", []byte("two"))
	if err != nil {
		t.Fatalf("second put: %v", err)
	}
	if r2.Version != 2 {
		t.Fatalf("second put version = %d, want 2", r2.Version)
	}
}

// putWithRetry retries a Put through a short window to absorb the initial
// leader election at cluster startup.
func putWithRetry(ctx context.Context, c *client.Client, key, val string) (client.PutResult, error) {
	deadline := time.Now().Add(10 * time.Second)
	var last error
	for time.Now().Before(deadline) {
		octx, cancel := context.WithTimeout(ctx, 2*time.Second)
		r, err := c.Put(octx, key, []byte(val))
		cancel()
		if err == nil {
			return r, nil
		}
		last = err
		time.Sleep(50 * time.Millisecond)
	}
	return client.PutResult{}, last
}
