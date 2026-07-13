// Package e2e drives a REAL parallax-kv cluster: it builds the parallaxd
// binary, starts three OS processes on localhost, exercises them through the
// public client, then SIGKILLs the leader and verifies the cluster fails over
// without losing a single acknowledged write.
//
// This is deliberately a separate binary-level test (not in-process): it is
// the closest reproducible stand-in for a real deployment, catching wiring
// that in-process tests cannot — process flags, listener binding, transport
// dial/redial, and durable recovery. Race coverage of the server internals
// lives in the in-process test (server package), since child processes escape
// the -race instrumentation of the test binary.
package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/iwang-1/parallax-kv/client"
	"github.com/iwang-1/parallax-kv/proto/kvpb"
)

// node is one parallaxd process under test.
type node struct {
	id         uint64
	peerAddr   string
	clientAddr string
	dataDir    string
	cmd        *exec.Cmd
	logPath    string
}

// buildParallaxd compiles the parallaxd binary into a temp dir and returns
// its path. The build uses only already-fetched modules (no network).
func buildParallaxd(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "parallaxd")
	cmd := exec.Command("go", "build", "-o", bin, "github.com/iwang-1/parallax-kv/cmd/parallaxd")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build parallaxd: %v\n%s", err, out)
	}
	return bin
}

// startCluster launches a 3-node cluster and returns the nodes. Ports are
// fixed offsets from a base chosen to avoid the common ephemeral range.
func startCluster(t *testing.T, bin string) []*node {
	t.Helper()
	const peerBase, clientBase = 17100, 18100
	dir := t.TempDir()

	peerMap := ""
	clientMap := ""
	nodes := make([]*node, 3)
	for i := 0; i < 3; i++ {
		id := uint64(i + 1)
		n := &node{
			id:         id,
			peerAddr:   fmt.Sprintf("localhost:%d", peerBase+i),
			clientAddr: fmt.Sprintf("localhost:%d", clientBase+i),
			dataDir:    filepath.Join(dir, fmt.Sprintf("d%d", id)),
		}
		nodes[i] = n
		if i > 0 {
			peerMap += ","
			clientMap += ","
		}
		peerMap += fmt.Sprintf("%d=%s", id, n.peerAddr)
		clientMap += fmt.Sprintf("%d=%s", id, n.clientAddr)
	}
	for _, n := range nodes {
		startNode(t, bin, n, peerMap, clientMap)
	}
	t.Cleanup(func() {
		for _, n := range nodes {
			n.kill()
		}
	})
	return nodes
}

func startNode(t *testing.T, bin string, n *node, peerMap, clientMap string) {
	t.Helper()
	n.logPath = n.dataDir + ".log"
	logf, err := os.Create(n.logPath)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	defer logf.Close()
	// Fast timers keep the test short while staying well above the ~1ms
	// loopback round trip: 10ms tick, heartbeat every 3 ticks (30ms),
	// election timeout 15-30 ticks (150-300ms).
	n.cmd = exec.Command(bin,
		"--id", fmt.Sprint(n.id),
		"--peers", peerMap,
		"--client-peers", clientMap,
		"--data-dir", n.dataDir,
		"--listen", n.clientAddr,
		"--tick-ms", "10",
		"--heartbeat-ticks", "3",
		"--election-ticks", "15",
	)
	n.cmd.Stdout = logf
	n.cmd.Stderr = logf
	if err := n.cmd.Start(); err != nil {
		t.Fatalf("start node %d: %v", n.id, err)
	}
}

func (n *node) kill() {
	if n.cmd != nil && n.cmd.Process != nil {
		_ = n.cmd.Process.Signal(syscall.SIGKILL)
		_, _ = n.cmd.Process.Wait()
	}
}

// findLeader probes every node's client address directly and returns the node
// whose response does not carry not_leader. It retries until deadline because
// an election may still be in progress.
func findLeader(t *testing.T, nodes []*node, deadline time.Time) *node {
	t.Helper()
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.cmd == nil || n.cmd.Process == nil {
				continue
			}
			if probeIsLeader(n.clientAddr) {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	dumpLogs(t, nodes)
	t.Fatal("no leader elected before deadline")
	return nil
}

// probeIsLeader dials addr and issues a one-shot Get; a response without
// not_leader means addr is the current leader.
func probeIsLeader(addr string) bool {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return false
	}
	defer conn.Close()
	stub := kvpb.NewKVServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	resp, err := stub.Get(ctx, &kvpb.GetRequest{Header: &kvpb.RequestHeader{ClientId: 1}, Key: "__probe__"})
	if err != nil {
		return false
	}
	return !resp.GetHeader().GetNotLeader()
}

func dumpLogs(t *testing.T, nodes []*node) {
	t.Helper()
	for _, n := range nodes {
		if b, err := os.ReadFile(n.logPath); err == nil {
			t.Logf("=== node %d log ===\n%s", n.id, b)
		}
	}
}

// TestClusterFailoverNoAckedWriteLoss is the S4 definition-of-done test: a
// real 3-process cluster, write set A, SIGKILL the leader, write set B against
// the survivors, then verify every acknowledged write from BOTH sets is still
// readable and a single new leader is serving. No acked write may be lost.
func TestClusterFailoverNoAckedWriteLoss(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping real-process cluster test in -short mode")
	}
	bin := buildParallaxd(t)
	nodes := startCluster(t, bin)

	addrs := make([]string, len(nodes))
	for i, n := range nodes {
		addrs[i] = n.clientAddr
	}

	leader := findLeader(t, nodes, time.Now().Add(10*time.Second))
	t.Logf("initial leader: node %d (%s)", leader.id, leader.clientAddr)

	c, err := client.Dial(addrs)
	if err != nil {
		t.Fatalf("dial cluster: %v", err)
	}
	defer c.Close()

	// Set A: writes acknowledged BEFORE the leader is killed.
	acked := make(map[string]string)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("A-%02d", i)
		val := fmt.Sprintf("valueA-%02d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Put(ctx, key, []byte(val))
		cancel()
		if err != nil {
			t.Fatalf("set A put %s: %v", key, err)
		}
		acked[key] = val
	}
	t.Logf("set A: %d writes acknowledged", len(acked))

	// Kill the leader.
	t.Logf("killing leader node %d", leader.id)
	leader.kill()
	leader.cmd = nil

	// Failover: a new leader must emerge among the survivors.
	survivors := make([]*node, 0, 2)
	for _, n := range nodes {
		if n.id != leader.id {
			survivors = append(survivors, n)
		}
	}
	newLeader := findLeader(t, survivors, time.Now().Add(10*time.Second))
	if newLeader.id == leader.id {
		t.Fatalf("killed leader %d still reported as leader", leader.id)
	}
	t.Logf("new leader after failover: node %d", newLeader.id)

	// Set B: writes acknowledged AFTER failover, against the survivors.
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("B-%02d", i)
		val := fmt.Sprintf("valueB-%02d", i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := c.Put(ctx, key, []byte(val))
		cancel()
		if err != nil {
			t.Fatalf("set B put %s: %v", key, err)
		}
		acked[key] = val
	}
	t.Logf("set B: writes acknowledged, %d total acked", len(acked))

	// Verify: every acknowledged write from BOTH sets is still readable with
	// its exact acknowledged value. This is the durability guarantee — a
	// committed (acked) write survives a leader crash.
	for key, want := range acked {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		r, err := c.Get(ctx, key)
		cancel()
		if err != nil {
			t.Fatalf("verify get %s: %v", key, err)
		}
		if !r.Found {
			t.Errorf("acked write %s lost after failover", key)
			continue
		}
		if string(r.Value) != want {
			t.Errorf("key %s = %q, want %q", key, r.Value, want)
		}
	}
	if t.Failed() {
		dumpLogs(t, nodes)
	}
}
