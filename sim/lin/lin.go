// Package lin records client operation histories and checks them for
// linearizability with Porcupine (github.com/anishathalye/porcupine).
//
// Credit where due: the checker itself — an implementation of the
// Wing & Gong / Lowe algorithm — is Porcupine, used as a library. The
// original work here is everything that produces and consumes the
// histories: the deterministic simulator, the fault scenarios, the KV
// model, and the consistency report. docs/DESIGN_NOTES.md sketches how
// the WGL algorithm searches for a valid linearization.
package lin

import (
	"github.com/anishathalye/porcupine"

	"github.com/iwang-1/parallax-kv/kv"
)

// Operation is one completed client operation: what was asked, what came
// back, and the virtual-time window [Call, Return] it was in flight.
// Timestamps are sim.VirtualTime values, typed int64 here so that sim can
// depend on lin and not vice versa.
type Operation struct {
	ClientID uint64
	Input    kv.Command
	Output   kv.Result
	Call     int64
	Return   int64
}

// History accumulates operations as clients invoke and complete them. Not
// safe for concurrent use (the simulator is single-goroutine; real-process
// tests must serialize externally).
type History struct {
	ops     []Operation
	pending map[int]Operation
	nextID  int
}

// NewHistory returns an empty History.
func NewHistory() *History {
	return &History{pending: make(map[int]Operation)}
}

// Invoke records the start of an operation at virtual time `at` and
// returns an ID to pass to Complete.
func (h *History) Invoke(clientID uint64, cmd kv.Command, at int64) int {
	// TODO(S1)
	panic("lin: Invoke not implemented (stage S1)")
}

// Complete records the response for a previously invoked operation.
// Operations never completed (client crashed, request lost) remain
// pending and are handed to Porcupine as possibly-taking-effect, per the
// standard treatment of indeterminate operations.
func (h *History) Complete(id int, res kv.Result, at int64) {
	// TODO(S1)
	panic("lin: Complete not implemented (stage S1)")
}

// Operations returns all completed operations.
func (h *History) Operations() []Operation {
	// TODO(S1)
	panic("lin: Operations not implemented (stage S1)")
}

// KVModel returns the Porcupine sequential model of the kv state machine:
// Get/Put/Delete/CAS over a single key (histories are checked per key —
// linearizability is a local property, so per-key checking is sound and
// exponentially cheaper).
func KVModel() porcupine.Model {
	// TODO(S2)
	panic("lin: KVModel not implemented (stage S2)")
}

// Check runs Porcupine over the history and reports the result plus
// visualization info for failures.
func Check(h *History) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	// TODO(S2)
	panic("lin: Check not implemented (stage S2)")
}
