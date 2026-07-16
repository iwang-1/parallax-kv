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
	"fmt"
	"math"
	"sort"
	"time"

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
	id := h.nextID
	h.nextID++
	h.pending[id] = Operation{ClientID: clientID, Input: cmd, Call: at, Return: -1}
	return id
}

// Complete records the response for a previously invoked operation.
// Operations never completed (client crashed, request lost) remain
// pending and are handed to Porcupine as possibly-taking-effect, per the
// standard treatment of indeterminate operations.
func (h *History) Complete(id int, res kv.Result, at int64) {
	op, ok := h.pending[id]
	if !ok {
		return
	}
	op.Output = res
	op.Return = at
	delete(h.pending, id)
	h.ops = append(h.ops, op)
}

// Operations returns all completed operations, in the order they were
// completed. Operations still pending (never completed) are not included.
func (h *History) Operations() []Operation {
	out := make([]Operation, len(h.ops))
	copy(out, h.ops)
	return out
}

// Pending returns the operations that were invoked but never completed
// (client crashed, request or response lost). Callers that hand histories
// to Porcupine treat these as possibly-taking-effect.
func (h *History) Pending() []Operation {
	ids := make([]int, 0, len(h.pending))
	for id := range h.pending {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	out := make([]Operation, 0, len(ids))
	for _, id := range ids {
		out = append(out, h.pending[id])
	}
	return out
}

// keyState is the sequential model's state for a SINGLE key: whether it is
// present, its current value, and its version (versions start at 1 and
// increment on each successful mutation; a delete resets the key so the next
// create starts at 1 again). It is a comparable struct, so Porcupine's
// default (==) state equality is exactly right and no custom Equal is needed.
type keyState struct {
	present bool
	value   string
	version uint64
}

// outcome is the model's per-operation output. For a completed operation
// known is true and res is what the client actually observed, which the model
// validates against the sequential spec. For an operation that was invoked but
// never completed (client crashed, request or response lost) known is false:
// the result was never observed, so the model constrains nothing about it and
// only requires that the transition is one the spec could have taken — this is
// the standard "possibly-took-effect" treatment of indeterminate operations.
type outcome struct {
	known bool
	res   kv.Result
}

// KVModel returns the Porcupine sequential model of the kv state machine over
// a single key. The whole history is partitioned by key first: linearizability
// is a local (per-object) property, so checking each key's sub-history
// independently is sound and exponentially cheaper than checking the joint
// history. Within a key, Step replays Get/Put/Delete/CAS against keyState.
//
// The model is NONDETERMINISTIC because indeterminate (never-completed)
// operations branch: a write whose result the client never observed may or may
// not have taken effect, so both successor states are admissible. This is why
// a lost write cannot be forced to contradict a later read. ToModel folds the
// nondeterministic model into the deterministic power-set model Porcupine's
// checker consumes.
func KVModel() porcupine.Model {
	nm := kvNondeterministicModel()
	return nm.ToModel()
}

func kvNondeterministicModel() porcupine.NondeterministicModel {
	return porcupine.NondeterministicModel{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := make(map[string][]porcupine.Operation)
			for _, op := range history {
				k := op.Input.(kv.Command).Key
				byKey[k] = append(byKey[k], op)
			}
			keys := make([]string, 0, len(byKey))
			for k := range byKey {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := make([][]porcupine.Operation, 0, len(keys))
			for _, k := range keys {
				out = append(out, byKey[k])
			}
			return out
		},
		Init: func() []interface{} { return []interface{}{keyState{}} },
		Step: func(state, input, output interface{}) []interface{} {
			return stepKV(state.(keyState), input.(kv.Command), output.(outcome))
		},
		Equal: func(a, b interface{}) bool { return a.(keyState) == b.(keyState) },
		DescribeOperation: func(input, output interface{}) string {
			out := output.(outcome)
			if !out.known {
				return describeCommand(input.(kv.Command)) + " -> <pending>"
			}
			return fmt.Sprintf("%s -> %s", describeCommand(input.(kv.Command)), describeResult(out.res))
		},
		DescribeState: func(state interface{}) string {
			st := state.(keyState)
			if !st.present {
				return "<absent>"
			}
			return fmt.Sprintf("%q@v%d", st.value, st.version)
		},
	}
}

// applyOp returns the state that results from cmd taking effect against st.
func applyOp(st keyState, cmd kv.Command) keyState {
	switch cmd.Op {
	case kv.OpPut:
		return keyState{present: true, value: string(cmd.Value), version: st.version + 1}
	case kv.OpDelete:
		// Delete removes the key entirely; a later create restarts versioning
		// at 1, so the modeled state resets to the zero (absent) value.
		return keyState{}
	case kv.OpCAS:
		if casMatches(st, cmd) {
			return keyState{present: true, value: string(cmd.Value), version: st.version + 1}
		}
		return st
	default: // OpGet and anything read-only
		return st
	}
}

func casMatches(st keyState, cmd kv.Command) bool {
	if cmd.Expect == nil {
		return !st.present // nil Expect = create-if-absent
	}
	return st.present && st.value == string(cmd.Expect)
}

// stepKV returns the set of states admissible after cmd runs against st.
//
// For a completed operation (out.known) the observed result must match what
// the sequential spec produces in st; on a match the single successor state is
// returned, on a mismatch the empty set rejects this ordering. For an
// indeterminate operation (never completed) the result is unobserved, so a
// mutating command branches: {state-after, state-before} — it may or may not
// have taken effect — while a read leaves state unchanged.
func stepKV(st keyState, cmd kv.Command, out outcome) []interface{} {
	if !out.known {
		after := applyOp(st, cmd)
		if after == st {
			return []interface{}{st}
		}
		return []interface{}{after, st}
	}

	res := out.res
	switch cmd.Op {
	case kv.OpGet:
		if st.present {
			if res.Status == kv.StatusOK && string(res.Value) == st.value && res.Version == st.version {
				return []interface{}{st}
			}
			return nil
		}
		if res.Status == kv.StatusNotFound {
			return []interface{}{st}
		}
		return nil

	case kv.OpPut:
		next := keyState{present: true, value: string(cmd.Value), version: st.version + 1}
		if res.Status == kv.StatusOK && res.Version == next.version {
			return []interface{}{next}
		}
		return nil

	case kv.OpDelete:
		if st.present {
			if res.Status == kv.StatusOK {
				return []interface{}{keyState{}}
			}
			return nil
		}
		if res.Status == kv.StatusNotFound {
			return []interface{}{st}
		}
		return nil

	case kv.OpCAS:
		if casMatches(st, cmd) {
			next := keyState{present: true, value: string(cmd.Value), version: st.version + 1}
			if res.Status == kv.StatusOK && res.Version == next.version {
				return []interface{}{next}
			}
			return nil
		}
		// Mismatch: state is unchanged and the current value (if any) is
		// echoed back to the client.
		if st.present {
			if res.Status == kv.StatusCASMismatch && string(res.Value) == st.value && res.Version == st.version {
				return []interface{}{st}
			}
			return nil
		}
		if res.Status == kv.StatusCASMismatch {
			return []interface{}{st}
		}
		return nil

	default:
		return nil
	}
}

// Check runs Porcupine over the history — completed operations plus any that
// were invoked but never completed (client crashed, request or response lost).
// The never-completed ops are given an infinite return time, the standard
// "possibly-took-effect" treatment: Porcupine is free to place them anywhere
// at or after their call, including after every observed operation, so a lost
// write neither forces nor forbids an effect that nothing observed.
//
// The result is Ok (a valid linearization exists), Illegal (none does — a real
// consistency bug), or Unknown (the bounded search timed out — inconclusive).
// It also returns the LinearizationInfo Porcupine's visualizer
// consumes.
func Check(h *History) (porcupine.CheckResult, porcupine.LinearizationInfo) {
	completed := h.Operations()
	pending := h.Pending()
	ops := make([]porcupine.Operation, 0, len(completed)+len(pending))
	for _, o := range completed {
		ops = append(ops, porcupine.Operation{
			ClientId: int(o.ClientID),
			Input:    o.Input,
			Output:   outcome{known: true, res: o.Output},
			Call:     o.Call,
			Return:   o.Return,
		})
	}
	for _, o := range pending {
		ops = append(ops, porcupine.Operation{
			ClientId: int(o.ClientID),
			Input:    o.Input,
			Output:   outcome{}, // unobserved: known=false
			Call:     o.Call,
			Return:   math.MaxInt64, // possibly-took-effect: placeable anywhere after Call
		})
	}
	return porcupine.CheckOperationsVerbose(KVModel(), ops, checkTimeout)
}

// checkTimeout bounds Porcupine's search so a pathological history cannot hang
// the run. Per-key partitioning keeps realistic histories far under this;
// callers decide how to report the Unknown verdict returned on timeout.
const checkTimeout = 20 * time.Second

func describeCommand(c kv.Command) string {
	switch c.Op {
	case kv.OpGet:
		return fmt.Sprintf("get(%q)", c.Key)
	case kv.OpPut:
		return fmt.Sprintf("put(%q, %q)", c.Key, c.Value)
	case kv.OpDelete:
		return fmt.Sprintf("delete(%q)", c.Key)
	case kv.OpCAS:
		return fmt.Sprintf("cas(%q, expect=%q, %q)", c.Key, c.Expect, c.Value)
	default:
		return "<invalid>"
	}
}

func describeResult(r kv.Result) string {
	var status string
	switch r.Status {
	case kv.StatusOK:
		status = "OK"
	case kv.StatusNotFound:
		status = "NotFound"
	case kv.StatusCASMismatch:
		status = "CASMismatch"
	case kv.StatusStaleSeq:
		status = "StaleSeq"
	default:
		status = "?"
	}
	return fmt.Sprintf("{%s v=%q ver=%d}", status, r.Value, r.Version)
}
