package sim

import "container/heap"

// event is one scheduled action on the simulator timeline. Events are
// ordered by (At, Seq): At is the virtual time the action fires, and Seq is
// a monotonically increasing insertion counter that breaks ties in FIFO
// order. Because ordering never depends on the wall clock, map iteration,
// or goroutine scheduling, the sequence of dispatched events is a pure
// function of what was scheduled — the bedrock of the determinism gate.
type event struct {
	at   VirtualTime
	seq  uint64
	kind string // trace label, e.g. "tick", "deliver", "client"
	node uint64 // node the event pertains to (0 if none)
	// detail is a canonical rendering of the event payload, recorded into
	// the trace. It must not contain pointers, wall-clock values, or
	// map-ordered data.
	detail string
	// action performs the event's effect. Determinism comes from the
	// scheduling order, not from the closure contents; actions must
	// themselves avoid nondeterminism (they draw only from the run's one
	// seeded RNG).
	action func()
}

// eventQueue is a min-heap of events keyed by (at, seq). It is the single
// source of the simulator's ordering.
type eventQueue struct {
	items []*event
}

func (q *eventQueue) Len() int { return len(q.items) }

func (q *eventQueue) Less(i, j int) bool {
	a, b := q.items[i], q.items[j]
	if a.at != b.at {
		return a.at < b.at
	}
	return a.seq < b.seq
}

func (q *eventQueue) Swap(i, j int) { q.items[i], q.items[j] = q.items[j], q.items[i] }

func (q *eventQueue) Push(x any) { q.items = append(q.items, x.(*event)) }

func (q *eventQueue) Pop() any {
	old := q.items
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	q.items = old[:n-1]
	return it
}

// push schedules e.
func (q *eventQueue) push(e *event) { heap.Push(q, e) }

// pop removes and returns the earliest event, or nil if the queue is empty.
func (q *eventQueue) pop() *event {
	if len(q.items) == 0 {
		return nil
	}
	return heap.Pop(q).(*event)
}

// peek returns the earliest event without removing it, or nil if empty.
func (q *eventQueue) peek() *event {
	if len(q.items) == 0 {
		return nil
	}
	return q.items[0]
}
