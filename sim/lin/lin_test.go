package lin

import (
	"testing"

	"github.com/anishathalye/porcupine"
	"github.com/iwang-1/parallax-kv/kv"
)

// TestInvokeCompleteRecordsWindow checks the basic record lifecycle: an
// invoked-then-completed operation lands in Operations with its call/return
// window and I/O, and is no longer pending.
func TestInvokeCompleteRecordsWindow(t *testing.T) {
	h := NewHistory()
	cmd := kv.Command{ClientID: 1, Seq: 1, Op: kv.OpPut, Key: "k0", Value: []byte("v")}
	id := h.Invoke(1, cmd, 100)
	if got := len(h.Pending()); got != 1 {
		t.Fatalf("pending after invoke = %d, want 1", got)
	}
	h.Complete(id, kv.Result{Status: kv.StatusOK, Version: 1}, 250)

	ops := h.Operations()
	if len(ops) != 1 {
		t.Fatalf("operations = %d, want 1", len(ops))
	}
	op := ops[0]
	if op.Call != 100 || op.Return != 250 {
		t.Fatalf("window = [%d,%d], want [100,250]", op.Call, op.Return)
	}
	if op.Input.Key != "k0" || op.Output.Version != 1 {
		t.Fatalf("unexpected op contents: %+v", op)
	}
	if got := len(h.Pending()); got != 0 {
		t.Fatalf("pending after complete = %d, want 0", got)
	}
}

// TestPendingOperationsStayPending confirms never-completed operations
// (crashed client / lost response) remain pending and are not surfaced as
// completed operations.
func TestPendingOperationsStayPending(t *testing.T) {
	h := NewHistory()
	h.Invoke(1, kv.Command{ClientID: 1, Seq: 1, Op: kv.OpGet, Key: "k0"}, 10)
	done := h.Invoke(2, kv.Command{ClientID: 2, Seq: 1, Op: kv.OpPut, Key: "k0"}, 20)
	h.Complete(done, kv.Result{Status: kv.StatusOK}, 30)

	if got := len(h.Operations()); got != 1 {
		t.Fatalf("completed operations = %d, want 1", got)
	}
	pend := h.Pending()
	if len(pend) != 1 || pend[0].ClientID != 1 {
		t.Fatalf("pending = %+v, want the client-1 get", pend)
	}
}

// TestCompleteUnknownIDIsNoop confirms completing an unknown/duplicate id
// does not panic or fabricate operations.
func TestCompleteUnknownIDIsNoop(t *testing.T) {
	h := NewHistory()
	h.Complete(42, kv.Result{Status: kv.StatusOK}, 5)
	id := h.Invoke(1, kv.Command{ClientID: 1, Seq: 1, Op: kv.OpGet, Key: "k"}, 1)
	h.Complete(id, kv.Result{Status: kv.StatusNotFound}, 2)
	h.Complete(id, kv.Result{Status: kv.StatusOK}, 3) // duplicate complete
	if got := len(h.Operations()); got != 1 {
		t.Fatalf("operations = %d, want 1", got)
	}
	if h.Operations()[0].Output.Status != kv.StatusNotFound {
		t.Fatal("duplicate complete overwrote the recorded result")
	}
}

// put/get/del/cas helpers build a completed operation with a virtual-time
// window [call, ret]. They keep the model tests readable.
func opPut(client uint64, key, val string, ver uint64, call, ret int64) Operation {
	return Operation{ClientID: client, Input: kv.Command{ClientID: client, Op: kv.OpPut, Key: key, Value: []byte(val)},
		Output: kv.Result{Status: kv.StatusOK, Version: ver}, Call: call, Return: ret}
}
func opGet(client uint64, key, val string, ver uint64, call, ret int64) Operation {
	return Operation{ClientID: client, Input: kv.Command{ClientID: client, Op: kv.OpGet, Key: key},
		Output: kv.Result{Status: kv.StatusOK, Value: []byte(val), Version: ver}, Call: call, Return: ret}
}

// checkOps runs the model over a fixed set of completed operations by feeding
// them through a History (invoke+complete), returning the porcupine verdict.
func checkOps(t *testing.T, ops []Operation) porcupine.CheckResult {
	t.Helper()
	h := NewHistory()
	for _, o := range ops {
		id := h.Invoke(o.ClientID, o.Input, o.Call)
		h.Complete(id, o.Output, o.Return)
	}
	res, _ := Check(h)
	return res
}

// TestModelAcceptsLinearizableHistory feeds a concurrent-but-linearizable
// history: two clients write the same key in disjoint windows and a later read
// observes the second write. A valid serialization exists, so the model must
// return Ok.
func TestModelAcceptsLinearizableHistory(t *testing.T) {
	ops := []Operation{
		opPut(1, "k", "a", 1, 0, 10),
		opPut(2, "k", "b", 2, 20, 30),
		opGet(1, "k", "b", 2, 40, 50),
	}
	if got := checkOps(t, ops); got != porcupine.Ok {
		t.Fatalf("linearizable history reported %v, want Ok", got)
	}
}

// TestModelRejectsStaleRead is the strictness guard: a read that returns an
// OLD value after a strictly-later write committed cannot be linearized. If
// the model accepted this it would be vacuous. The write [0,10] finishes
// before the read [20,30] begins, yet the read observes the pre-write value —
// no serialization places the read before the write, so it must be Illegal.
func TestModelRejectsStaleRead(t *testing.T) {
	ops := []Operation{
		opPut(1, "k", "a", 1, 0, 10),
		opPut(1, "k", "b", 2, 11, 19),
		opGet(2, "k", "a", 1, 20, 30), // stale: "b" is the only legal value here
	}
	if got := checkOps(t, ops); got != porcupine.Illegal {
		t.Fatalf("stale read reported %v, want Illegal", got)
	}
}

// TestModelPendingWriteMayNotTakeEffect confirms an indeterminate (never
// completed) write does not force an effect: a lost write is followed by a
// read that returns the PRE-write value, which must remain linearizable
// because the pending write is free to be ordered after the read (or to have
// never taken effect).
func TestModelPendingWriteMayNotTakeEffect(t *testing.T) {
	h := NewHistory()
	i0 := h.Invoke(1, kv.Command{ClientID: 1, Op: kv.OpPut, Key: "k", Value: []byte("a")}, 0)
	h.Complete(i0, kv.Result{Status: kv.StatusOK, Version: 1}, 10)
	// A pending write of "b" that never completes (response lost).
	h.Invoke(2, kv.Command{ClientID: 2, Op: kv.OpPut, Key: "k", Value: []byte("b")}, 20)
	// A later read still sees "a": legal, because the pending write can be
	// ordered after the read.
	i2 := h.Invoke(1, kv.Command{ClientID: 1, Op: kv.OpGet, Key: "k"}, 30)
	h.Complete(i2, kv.Result{Status: kv.StatusOK, Value: []byte("a"), Version: 1}, 40)
	if res, _ := Check(h); res != porcupine.Ok {
		t.Fatalf("pending write forced an effect: reported %v, want Ok", res)
	}
}
