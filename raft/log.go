package raft

import "fmt"

// unstable holds log entries (and an optional incoming snapshot) that the
// core has produced or received but that the driver has not yet persisted to
// LogStorage. The core serves reads (term lookups, replication) from unstable
// first and falls back to storage, so replication never blocks on the driver's
// fsync. Advance shrinks unstable as the driver confirms persistence.
//
// entries[i] has log index offset+i. When snapshot != nil it covers the log
// through snapshot.Metadata.Index and offset == snapshot.Metadata.Index+1.
type unstable struct {
	snapshot *Snapshot
	entries  []Entry
	offset   uint64
}

// maybeFirstIndex returns the first index represented by the unstable
// snapshot, if any.
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index held in unstable (from entries, or
// from a lone snapshot), if anything is buffered.
func (u *unstable) maybeLastIndex() (uint64, bool) {
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i if it is held in
// unstable (or is the snapshot's covered index).
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}
	last, ok := u.maybeLastIndex()
	if !ok || i > last {
		return 0, false
	}
	if u.snapshot != nil && i == u.snapshot.Metadata.Index {
		return u.snapshot.Metadata.Term, true
	}
	return u.entries[i-u.offset].Term, true
}

// stableTo drops entries through index i (with matching term t) now that the
// driver has persisted them. A term mismatch means the entries were replaced
// by a later truncation, so nothing is dropped.
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok || gt != t || i < u.offset {
		return
	}
	u.entries = append([]Entry(nil), u.entries[i+1-u.offset:]...)
	u.offset = i + 1
}

// stableSnapTo drops the buffered snapshot once the driver has persisted it.
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

// restore resets unstable to sit atop a freshly received snapshot.
func (u *unstable) restore(s Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	snap := s
	u.snapshot = &snap
}

// truncateAppend splices ents onto the unstable log, truncating any
// conflicting suffix. ents must be index-contiguous.
func (u *unstable) truncateAppend(ents []Entry) {
	if len(ents) == 0 {
		return
	}
	first := ents[0].Index
	switch {
	case first == u.offset+uint64(len(u.entries)):
		// Append directly after the last unstable entry.
		u.entries = append(u.entries, ents...)
	case first <= u.offset:
		// Incoming entries replace the whole unstable region.
		u.offset = first
		u.entries = append([]Entry(nil), ents...)
	default:
		// Truncate the conflicting suffix, then append.
		keep := append([]Entry(nil), u.entries[:first-u.offset]...)
		u.entries = append(keep, ents...)
	}
}

// raftLog is the core's logical view of the replicated log: the durable
// prefix in storage, plus the unstable tail, plus the committed and applied
// watermarks. It performs no I/O; all durable writes happen in the driver.
type raftLog struct {
	storage   LogStorage
	unstable  unstable
	committed uint64
	applied   uint64
}

// newLog builds a raftLog over storage, recovering the committed/applied
// watermarks from the persisted first index.
func newLog(storage LogStorage) (*raftLog, error) {
	first, err := storage.FirstIndex()
	if err != nil {
		return nil, err
	}
	last, err := storage.LastIndex()
	if err != nil {
		return nil, err
	}
	l := &raftLog{storage: storage}
	l.unstable.offset = last + 1
	// Everything through the compacted prefix is, by definition, applied and
	// committed; the raft struct raises committed further from HardState.
	l.committed = first - 1
	l.applied = first - 1
	return l, nil
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	i, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		panic(fmt.Sprintf("raft: unexpected error getting last term: %v", err))
	}
	return t
}

// term returns the term of the entry at index i, or ErrCompacted /
// ErrUnavailable when i is outside the available range.
func (l *raftLog) term(i uint64) (uint64, error) {
	dummy := l.firstIndex() - 1
	if i < dummy || i > l.lastIndex() {
		return 0, nil
	}
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// matchTerm reports whether the entry at index i has term t.
func (l *raftLog) matchTerm(i, t uint64) bool {
	gt, err := l.term(i)
	if err != nil {
		return false
	}
	return gt == t
}

// append adds new entries to the unstable log and returns the new last index.
// Entries are stamped by the caller with contiguous indices.
func (l *raftLog) append(ents ...Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	l.unstable.truncateAppend(ents)
	return l.lastIndex()
}

// findConflict returns the index of the first entry in ents whose term does
// not match this log (or that extends past it), or 0 if there is no conflict
// and no new entries.
func (l *raftLog) findConflict(ents []Entry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.Index, e.Term) {
			return e.Index
		}
	}
	return 0
}

// maybeAppend attempts to splice an AppendEntries payload. index/logTerm are
// the entry preceding ents. It returns the index of the last new entry and
// true on success, or 0/false if the preceding entry does not match.
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents []Entry) (uint64, bool) {
	if !l.matchTerm(index, logTerm) {
		return 0, false
	}
	lastnewi := index + uint64(len(ents))
	conflict := l.findConflict(ents)
	switch {
	case conflict == 0:
		// All entries already present; nothing to append.
	case conflict <= l.committed:
		panic(fmt.Sprintf("raft: conflict at committed index %d", conflict))
	default:
		off := index + 1
		l.append(ents[conflict-off:]...)
	}
	l.commitTo(min(committed, lastnewi))
	return lastnewi, true
}

func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if tocommit > l.lastIndex() {
			panic(fmt.Sprintf("raft: commit %d out of range [lastIndex %d]", tocommit, l.lastIndex()))
		}
		l.committed = tocommit
	}
}

func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		panic(fmt.Sprintf("raft: applied %d out of range [prevApplied %d, committed %d]", i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *raftLog) stableTo(i, t uint64)  { l.unstable.stableTo(i, t) }
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

// restore resets the log to sit atop a received snapshot, buffering it in
// unstable for the driver to persist and advancing the watermarks.
func (l *raftLog) restore(s Snapshot) {
	l.committed = s.Metadata.Index
	if l.applied < s.Metadata.Index {
		l.applied = s.Metadata.Index
	}
	l.unstable.restore(s)
}

// slice returns entries in [lo, hi), drawing from unstable and storage as
// needed. It panics on out-of-bounds requests (a core bug), returning
// ErrCompacted only when the range dips below the first available index.
func (l *raftLog) slice(lo, hi uint64) ([]Entry, error) {
	if lo >= hi {
		return nil, nil
	}
	fi := l.firstIndex()
	if lo < fi {
		return nil, ErrCompacted
	}
	last := l.lastIndex()
	if hi > last+1 {
		hi = last + 1
	}
	var ents []Entry
	uoff := l.unstable.offset
	// Storage portion: [lo, min(hi, uoff)).
	if lo < uoff {
		shi := min(hi, uoff)
		se, err := l.storage.Entries(lo, shi)
		if err != nil {
			return nil, err
		}
		ents = append(ents, se...)
	}
	// Unstable portion: [max(lo, uoff), hi).
	if hi > uoff {
		ulo := max(lo, uoff)
		ents = append(ents, l.unstable.entries[ulo-uoff:hi-uoff]...)
	}
	return ents, nil
}

// nextCommittedEnts returns committed-but-unapplied entries in
// (applied, committed], for the driver to apply.
func (l *raftLog) nextCommittedEnts() []Entry {
	lo := l.applied + 1
	hi := l.committed + 1
	if lo >= hi {
		return nil
	}
	// Do not surface entries still buffered as an unstable snapshot boundary.
	if lo < l.firstIndex() {
		lo = l.firstIndex()
	}
	if lo >= hi {
		return nil
	}
	ents, err := l.slice(lo, hi)
	if err != nil {
		panic(err)
	}
	return ents
}

// hasNextCommittedEnts reports whether there are entries to apply.
func (l *raftLog) hasNextCommittedEnts() bool {
	lo := max(l.applied+1, l.firstIndex())
	return l.committed+1 > lo
}

// isUpToDate reports whether a log ending at (lasti, term) is at least as
// up-to-date as this log — the RequestVote up-to-date test.
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// maybeCommit advances the commit index to maxIndex if that entry is from
// term (the Figure-8 current-term restriction is enforced by the caller
// passing its current term). It returns whether commit advanced.
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed {
		if t, _ := l.term(maxIndex); t == term {
			l.commitTo(maxIndex)
			return true
		}
	}
	return false
}
