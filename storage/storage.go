// Package storage groups the LogStorage implementations. The interface
// itself is defined in package raft (Go convention: the consumer defines
// the interface); LogStorage here is an alias for discoverability.
//
//   - storage/disk: segmented, CRC32-framed write-ahead log with
//     group-commit fsync per Ready batch, plus atomic snapshot files
//     (write-tmp, fsync, rename, dir-fsync). The production backend.
//   - storage/mem: in-memory implementation for the deterministic
//     simulator, modeling persistence across simulated crash-restarts.
package storage

import "github.com/iwang-1/parallax-kv/raft"

// LogStorage = raft.LogStorage. See that type for the contract.
type LogStorage = raft.LogStorage
