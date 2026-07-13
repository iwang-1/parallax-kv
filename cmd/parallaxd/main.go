// Command parallaxd runs one parallax-kv node: the raft core driven by the
// production runtime (real clock, disk WAL, gRPC transport).
//
// Usage:
//
//	parallaxd --id 1 \
//	          --peers 1=localhost:7101,2=localhost:7102,3=localhost:7103 \
//	          --data-dir /var/lib/parallax/1 \
//	          --listen localhost:8101 \
//	          --client-peers 1=localhost:8101,2=localhost:8102,3=localhost:8103
//
// --peers is the peer-RPC address map (must include this node's --id).
// --listen is this node's client-facing KVService address. --client-peers is
// optional: it lets leader-redirect responses carry the leader's client
// address so clients can jump straight to it (without it, clients round-robin).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/iwang-1/parallax-kv/server"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "parallaxd:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("parallaxd", flag.ExitOnError)
	id := fs.Uint64("id", 0, "node ID (positive, unique in the cluster)")
	peers := fs.String("peers", "", "comma-separated id=host:port peer-RPC map, including this node")
	clientPeers := fs.String("client-peers", "", "optional id=host:port client-address map for redirect hints")
	dataDir := fs.String("data-dir", "", "WAL and snapshot directory")
	listen := fs.String("listen", "", "client-facing listen address")
	tickMillis := fs.Int("tick-ms", 20, "real-time duration of one raft tick in milliseconds")
	heartbeatTicks := fs.Int("heartbeat-ticks", 5, "leader heartbeat interval in ticks")
	electionTicks := fs.Int("election-ticks", 50, "base election timeout in ticks")
	_ = fs.Parse(os.Args[1:])

	if *id == 0 {
		return fmt.Errorf("--id is required and must be positive")
	}
	if *dataDir == "" {
		return fmt.Errorf("--data-dir is required")
	}
	if *listen == "" {
		return fmt.Errorf("--listen is required")
	}
	peerMap, err := parsePeerMap(*peers)
	if err != nil {
		return fmt.Errorf("--peers: %w", err)
	}
	if _, ok := peerMap[*id]; !ok {
		return fmt.Errorf("--peers must include this node's --id %d", *id)
	}
	var clientMap map[uint64]string
	if *clientPeers != "" {
		if clientMap, err = parsePeerMap(*clientPeers); err != nil {
			return fmt.Errorf("--client-peers: %w", err)
		}
	}

	cfg := server.Config{
		ID:                 *id,
		Peers:              peerMap,
		ClientAddrs:        clientMap,
		DataDir:            *dataDir,
		ListenPeer:         peerMap[*id],
		ListenClient:       *listen,
		TickIntervalMillis: *tickMillis,
		ElectionTicks:      *electionTicks,
		HeartbeatTicks:     *heartbeatTicks,
	}
	srv, err := server.New(cfg)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Fprintf(os.Stderr, "parallaxd: node %d peer=%s client=%s data-dir=%s\n",
		*id, peerMap[*id], *listen, *dataDir)
	return srv.Run(ctx)
}

// parsePeerMap parses "id=host:port,id=host:port,..." into a map. IDs must be
// positive and unique.
func parsePeerMap(s string) (map[uint64]string, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("empty map")
	}
	m := make(map[uint64]string)
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		eq := strings.IndexByte(part, '=')
		if eq < 0 {
			return nil, fmt.Errorf("entry %q missing '='", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(part[:eq]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("entry %q: bad id: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("entry %q: id must be positive", part)
		}
		addr := strings.TrimSpace(part[eq+1:])
		if addr == "" {
			return nil, fmt.Errorf("entry %q: empty address", part)
		}
		if _, dup := m[id]; dup {
			return nil, fmt.Errorf("duplicate id %d", id)
		}
		m[id] = addr
	}
	if len(m) == 0 {
		return nil, fmt.Errorf("no entries")
	}
	return m, nil
}
