package grpc

import (
	"github.com/iwang-1/parallax-kv/proto/raftpb"
	"github.com/iwang-1/parallax-kv/raft"
)

// toProto converts a native raft.Message to its wire form. The wire enum
// values are defined to track the raft.MessageType constants one-for-one, so
// the conversion is a direct cast.
func toProto(m raft.Message) *raftpb.Message {
	pm := &raftpb.Message{
		Type:       raftpb.MessageType(m.Type),
		From:       m.From,
		To:         m.To,
		Term:       m.Term,
		LogIndex:   m.LogIndex,
		LogTerm:    m.LogTerm,
		Commit:     m.Commit,
		Reject:     m.Reject,
		RejectHint: m.RejectHint,
		HintTerm:   m.HintTerm,
		Context:    m.Context,
	}
	if len(m.Entries) > 0 {
		pm.Entries = make([]*raftpb.Entry, len(m.Entries))
		for i := range m.Entries {
			pm.Entries[i] = toProtoEntry(m.Entries[i])
		}
	}
	if m.Snapshot != nil {
		pm.Snapshot = &raftpb.Snapshot{
			Index: m.Snapshot.Metadata.Index,
			Term:  m.Snapshot.Metadata.Term,
			Data:  m.Snapshot.Data,
		}
	}
	return pm
}

func toProtoEntry(e raft.Entry) *raftpb.Entry {
	return &raftpb.Entry{
		Term:  e.Term,
		Index: e.Index,
		Type:  raftpb.EntryType(e.Type),
		Data:  e.Data,
	}
}

// fromProto converts a wire message back to the native raft.Message.
func fromProto(pm *raftpb.Message) raft.Message {
	m := raft.Message{
		Type:       raft.MessageType(pm.GetType()),
		From:       pm.GetFrom(),
		To:         pm.GetTo(),
		Term:       pm.GetTerm(),
		LogIndex:   pm.GetLogIndex(),
		LogTerm:    pm.GetLogTerm(),
		Commit:     pm.GetCommit(),
		Reject:     pm.GetReject(),
		RejectHint: pm.GetRejectHint(),
		HintTerm:   pm.GetHintTerm(),
		Context:    pm.GetContext(),
	}
	if ents := pm.GetEntries(); len(ents) > 0 {
		m.Entries = make([]raft.Entry, len(ents))
		for i, pe := range ents {
			m.Entries[i] = raft.Entry{
				Term:  pe.GetTerm(),
				Index: pe.GetIndex(),
				Type:  raft.EntryType(pe.GetType()),
				Data:  pe.GetData(),
			}
		}
	}
	if ps := pm.GetSnapshot(); ps != nil {
		m.Snapshot = &raft.Snapshot{
			Metadata: raft.SnapshotMetadata{Index: ps.GetIndex(), Term: ps.GetTerm()},
			Data:     ps.GetData(),
		}
	}
	return m
}
