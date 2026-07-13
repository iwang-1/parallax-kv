package sim

import (
	"strconv"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/raft"
)

// This file renders sim payloads into canonical, deterministic detail
// strings for the trace. Nothing here reads a clock, iterates a map, or
// dereferences a pointer whose identity could vary between runs — only the
// stable, value-typed contents of messages and commands are rendered.

func msgTypeName(t raft.MessageType) string {
	switch t {
	case raft.MsgPropose:
		return "Propose"
	case raft.MsgReadIndex:
		return "ReadIndex"
	case raft.MsgPreVote:
		return "PreVote"
	case raft.MsgPreVoteResp:
		return "PreVoteResp"
	case raft.MsgVote:
		return "Vote"
	case raft.MsgVoteResp:
		return "VoteResp"
	case raft.MsgAppend:
		return "Append"
	case raft.MsgAppendResp:
		return "AppendResp"
	case raft.MsgHeartbeat:
		return "Heartbeat"
	case raft.MsgHeartbeatResp:
		return "HeartbeatResp"
	case raft.MsgInstallSnapshot:
		return "InstallSnapshot"
	case raft.MsgInstallSnapshotResp:
		return "InstallSnapshotResp"
	default:
		return "Unknown"
	}
}

// renderMessage produces a canonical one-line rendering of a message,
// including only fields that are deterministic given the run. Entry payload
// bytes are summarized by count and a content digest so large values do not
// bloat the trace while still detecting divergence.
func renderMessage(m raft.Message) string {
	b := make([]byte, 0, 96)
	b = append(b, msgTypeName(m.Type)...)
	b = appendKV(b, " t", m.Term)
	b = appendKV(b, " li", m.LogIndex)
	b = appendKV(b, " lt", m.LogTerm)
	b = appendKV(b, " c", m.Commit)
	if m.Reject {
		b = append(b, " rej"...)
		b = appendKV(b, " rh", m.RejectHint)
		b = appendKV(b, " ht", m.HintTerm)
	}
	if len(m.Entries) > 0 {
		b = appendKV(b, " ne", uint64(len(m.Entries)))
		b = appendKV(b, " ed", digestEntries(m.Entries))
	}
	if m.Snapshot != nil {
		b = appendKV(b, " si", m.Snapshot.Metadata.Index)
		b = appendKV(b, " st", m.Snapshot.Metadata.Term)
		b = appendKV(b, " sd", fnv64(m.Snapshot.Data))
	}
	if len(m.Context) > 0 {
		b = appendKV(b, " ctx", fnv64(m.Context))
	}
	return string(b)
}

// digestEntries folds the term/index/type/data of a slice of entries into
// one digest, so that two nodes carrying "the same" entries render alike
// and divergent entries render differently.
func digestEntries(es []raft.Entry) uint64 {
	var acc uint64 = 1469598103934665603
	const prime uint64 = 1099511628211
	mix := func(v uint64) {
		acc ^= v
		acc *= prime
	}
	for _, e := range es {
		mix(e.Term)
		mix(e.Index)
		mix(uint64(e.Type))
		mix(fnv64(e.Data))
	}
	return acc
}

func appendKV(buf []byte, key string, v uint64) []byte {
	buf = append(buf, key...)
	buf = append(buf, '=')
	return strconv.AppendUint(buf, v, 10)
}

// renderCommand produces a canonical rendering of a client command for the
// trace (client invoke events).
func renderCommand(c kv.Command) string {
	b := make([]byte, 0, 64)
	b = append(b, opName(c.Op)...)
	b = appendKV(b, " cid", c.ClientID)
	b = appendKV(b, " seq", c.Seq)
	b = append(b, " k="...)
	b = append(b, c.Key...)
	if c.Value != nil {
		b = appendKV(b, " v", fnv64(c.Value))
	}
	if c.Expect != nil {
		b = appendKV(b, " x", fnv64(c.Expect))
	}
	return string(b)
}

// renderGroups renders a partition grouping canonically (groups in given
// order, node ids within a group sorted) for the trace.
func renderGroups(groups [][]uint64) string {
	b := make([]byte, 0, 32)
	for gi, g := range groups {
		if gi > 0 {
			b = append(b, '|')
		}
		sorted := append([]uint64(nil), g...)
		for i := 1; i < len(sorted); i++ {
			for j := i; j > 0 && sorted[j-1] > sorted[j]; j-- {
				sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
			}
		}
		for i, id := range sorted {
			if i > 0 {
				b = append(b, ',')
			}
			b = strconv.AppendUint(b, id, 10)
		}
	}
	return string(b)
}

func opName(op kv.OpType) string {
	switch op {
	case kv.OpGet:
		return "Get"
	case kv.OpPut:
		return "Put"
	case kv.OpDelete:
		return "Delete"
	case kv.OpCAS:
		return "CAS"
	default:
		return "Invalid"
	}
}

// renderResult produces a canonical rendering of a command result (client
// return events).
func renderResult(r kv.Result) string {
	b := make([]byte, 0, 32)
	b = append(b, "st="...)
	b = strconv.AppendUint(b, uint64(r.Status), 10)
	b = appendKV(b, " ver", r.Version)
	if r.Value != nil {
		b = appendKV(b, " v", fnv64(r.Value))
	}
	return string(b)
}
