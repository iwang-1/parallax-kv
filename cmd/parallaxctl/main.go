// Command parallaxctl is the CLI client for a parallax-kv cluster.
//
// Usage (stage S4 wires this to the KVService client):
//
//	parallaxctl --cluster localhost:8101,localhost:8102 get  KEY
//	parallaxctl --cluster ...                            put  KEY VALUE
//	parallaxctl --cluster ...                            del  KEY
//	parallaxctl --cluster ...                            cas  KEY EXPECT VALUE
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	fs := flag.NewFlagSet("parallaxctl", flag.ExitOnError)
	cluster := fs.String("cluster", "localhost:8101", "comma-separated client addresses")
	_ = fs.Parse(os.Args[1:])
	_ = cluster

	// TODO(S4): session establishment, op dispatch, leader chasing.
	fmt.Fprintln(os.Stderr, "parallaxctl: not implemented yet (stage S4)")
	os.Exit(1)
}
