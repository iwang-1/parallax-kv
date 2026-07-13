package sim

import "github.com/iwang-1/parallax-kv/raft"

// sendMessage routes one outbound message through the simulated network and
// schedules its delivery (and any duplicate) as future events. All timing
// and fault decisions are made now, at send time, drawing from the one
// seeded RNG; the resulting events carry no further randomness, so the
// delivery order is fully determined by the (time, seq) keys.
func (s *Simulator) sendMessage(from uint64, m raft.Message) {
	to := m.To
	// A message to self is delivered locally without touching the network
	// (the core may loop back a vote to itself, etc.).
	if to == from {
		s.scheduleDeliver(0, from, to, m)
		return
	}
	d := s.net.route(from, to, m)
	if !d.deliver && !d.duplicate {
		s.recordControl("drop", from, renderMessage(m))
		return
	}
	if d.deliver {
		s.scheduleDeliver(d.delay, from, to, m)
	} else {
		// Primary dropped but hook forced a duplicate: note the drop of the
		// primary for trace fidelity.
		s.recordControl("drop", from, renderMessage(m))
	}
	if d.duplicate {
		s.scheduleDeliver(d.dupDelay, from, to, m)
	}
}

// scheduleDeliver enqueues a delivery of m to node `to` after `delay`. The
// delivery event steps the message into the destination node if it is up;
// a crashed destination silently drops it (as a real network would, the
// packet arrives at a dead host).
func (s *Simulator) scheduleDeliver(delay VirtualTime, from, to uint64, m raft.Message) {
	detail := renderMessage(m)
	s.schedule(delay, "deliver", to, detail, func() {
		ns := s.nodes[to]
		if ns == nil || ns.crashed || ns.node == nil {
			return
		}
		s.step(ns, m)
	})
}
