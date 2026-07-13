package raft

// progress tracks, on the leader, how far a single follower's log has been
// replicated. Match is the highest index known to be replicated on the
// follower; Next is the index of the next entry to send. The leader lowers
// Next on rejection (conflict backtracking) and raises Match/Next on success.
type progress struct {
	Match uint64
	Next  uint64
}

// maybeUpdate raises Match/Next to reflect a successful append acknowledging
// index n. It reports whether Match advanced.
func (pr *progress) maybeUpdate(n uint64) bool {
	updated := false
	if pr.Match < n {
		pr.Match = n
		updated = true
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

// maybeDecrTo lowers Next in response to a rejection. rejected is the index
// the follower rejected; hint is its conflict hint (the highest index it
// believes could match). It returns false for a stale rejection that no
// longer applies.
func (pr *progress) maybeDecrTo(rejected, hint uint64) bool {
	if pr.Next-1 != rejected {
		// Stale out-of-order rejection; ignore.
		return false
	}
	pr.Next = hint + 1
	if pr.Next < 1 {
		pr.Next = 1
	}
	return true
}
