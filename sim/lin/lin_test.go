package lin

import (
	"testing"

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
