// Command parallaxctl is the CLI client for a parallax-kv cluster. It dials
// every node in --cluster and follows leader redirects automatically, so any
// address may be given.
//
// Usage:
//
//	parallaxctl --cluster localhost:8101,localhost:8102,localhost:8103 get KEY
//	parallaxctl --cluster ...                                          put KEY VALUE
//	parallaxctl --cluster ...                                          del KEY
//	parallaxctl --cluster ...                                          cas KEY EXPECT VALUE
//	parallaxctl --cluster ...                                          cas --create KEY VALUE
//
// cas without --create requires the key's current value to equal EXPECT;
// cas --create requires the key to be absent.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iwang-1/parallax-kv/client"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "parallaxctl:", err)
		os.Exit(1)
	}
}

func run() error {
	fs := flag.NewFlagSet("parallaxctl", flag.ExitOnError)
	cluster := fs.String("cluster", "localhost:8101", "comma-separated client addresses")
	timeout := fs.Duration("timeout", 5*time.Second, "overall operation timeout")
	// Global flags must precede the subcommand (standard Go flag parsing);
	// subcommand-specific flags (e.g. cas --create) are parsed separately below.
	_ = fs.Parse(os.Args[1:])

	args := fs.Args()
	if len(args) == 0 {
		return fmt.Errorf("usage: parallaxctl [--cluster ...] <get|put|del|cas> ARGS")
	}
	addrs := splitAddrs(*cluster)
	if len(addrs) == 0 {
		return fmt.Errorf("--cluster is empty")
	}

	c, err := client.Dial(addrs)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	cmd := args[0]
	rest := args[1:]
	switch cmd {
	case "get":
		if len(rest) != 1 {
			return fmt.Errorf("usage: get KEY")
		}
		r, err := c.Get(ctx, rest[0])
		if err != nil {
			return err
		}
		if !r.Found {
			fmt.Println("(not found)")
			return nil
		}
		fmt.Printf("%s\t(version %d)\n", string(r.Value), r.Version)
	case "put":
		if len(rest) != 2 {
			return fmt.Errorf("usage: put KEY VALUE")
		}
		r, err := c.Put(ctx, rest[0], []byte(rest[1]))
		if err != nil {
			return err
		}
		fmt.Printf("OK\t(version %d)\n", r.Version)
	case "del":
		if len(rest) != 1 {
			return fmt.Errorf("usage: del KEY")
		}
		found, err := c.Delete(ctx, rest[0])
		if err != nil {
			return err
		}
		if found {
			fmt.Println("OK")
		} else {
			fmt.Println("(not found)")
		}
	case "cas":
		return runCas(ctx, c, rest)
	default:
		return fmt.Errorf("unknown command %q (want get|put|del|cas)", cmd)
	}
	return nil
}

// runCas parses the cas subcommand, which owns its own --create flag so it
// may appear after the "cas" word (unlike the global flags).
func runCas(ctx context.Context, c *client.Client, args []string) error {
	cfs := flag.NewFlagSet("cas", flag.ExitOnError)
	create := cfs.Bool("create", false, "require the key to be absent (create-if-absent)")
	_ = cfs.Parse(args)
	rest := cfs.Args()

	var key string
	var expect, value []byte
	if *create {
		if len(rest) != 2 {
			return fmt.Errorf("usage: cas --create KEY VALUE")
		}
		key, value = rest[0], []byte(rest[1])
	} else {
		if len(rest) != 3 {
			return fmt.Errorf("usage: cas KEY EXPECT VALUE")
		}
		key, expect, value = rest[0], []byte(rest[1]), []byte(rest[2])
	}
	r, err := c.Cas(ctx, key, expect, value, *create)
	if err != nil {
		return err
	}
	if r.Swapped {
		fmt.Printf("SWAPPED\t(version %d)\n", r.Version)
		return nil
	}
	fmt.Printf("MISMATCH\t(current %q, version %d)\n", string(r.Current), r.Version)
	return nil
}

func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}
