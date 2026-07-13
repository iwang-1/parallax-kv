// Command parallaxd runs one parallax-kv node.
//
// Usage (stage S4 wires this to server.Server):
//
//	parallaxd --id 1 --peers 1=localhost:7101,2=localhost:7102,3=localhost:7103 \
//	          --data-dir /var/lib/parallax/1 --listen localhost:8101
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	fs := flag.NewFlagSet("parallaxd", flag.ExitOnError)
	id := fs.Uint64("id", 0, "node ID (positive, unique in the cluster)")
	peers := fs.String("peers", "", "comma-separated id=host:port peer map, including this node")
	dataDir := fs.String("data-dir", "", "WAL and snapshot directory")
	listen := fs.String("listen", "", "client-facing listen address")
	_ = fs.Parse(os.Args[1:])
	_, _, _, _ = id, peers, dataDir, listen

	// TODO(S4): build server.Config and run.
	fmt.Fprintln(os.Stderr, "parallaxd: not implemented yet (stage S4)")
	os.Exit(1)
}
