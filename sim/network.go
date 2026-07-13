package sim

import (
	"math/rand"

	"github.com/iwang-1/parallax-kv/raft"
)

// network is the simulated message fabric. It owns no goroutines and no
// clock: sending a message schedules a delivery event on the simulator's
// queue, and every random decision (delay, drop, duplicate) draws from the
// run's single seeded RNG. Reordering is emergent: two messages sent in one
// order can be assigned delays that reverse their delivery order.
type network struct {
	cfg NetworkConfig
	rng *rand.Rand

	// partition assigns each node to a partition group. Two nodes can
	// exchange messages only if they share a group id. A value of 0 is the
	// default "all connected" group; Partition assigns distinct positive
	// group ids. Nil means fully connected.
	partition map[uint64]int

	hook MessageHook
}

func newNetwork(cfg NetworkConfig, rng *rand.Rand) *network {
	return &network{cfg: cfg, rng: rng}
}

// connected reports whether from and to are in the same partition group
// (and therefore able to exchange messages). A node with no explicit group
// is in the default group 0.
func (n *network) connected(from, to uint64) bool {
	if n.partition == nil {
		return true
	}
	return n.partition[from] == n.partition[to]
}

// setPartition installs a grouping. groups is a list of node-id slices;
// each slice becomes one isolated group. Any node not mentioned falls into
// an implicit final group (id len(groups)+1). Passing no groups is a no-op
// grouping equivalent to Heal.
func (n *network) setPartition(groups [][]uint64) {
	if len(groups) == 0 {
		n.partition = nil
		return
	}
	p := make(map[uint64]int)
	for gi, g := range groups {
		for _, id := range g {
			p[id] = gi + 1
		}
	}
	n.partition = p
}

// heal removes all partitions.
func (n *network) heal() { n.partition = nil }

// delivery is the outcome of routing one message: whether to deliver it,
// after what delay, and whether to schedule a duplicate (with its own
// independently drawn delay). The caller (the simulator) turns these into
// scheduled events so that all effects flow through the one event queue.
type delivery struct {
	deliver   bool
	delay     VirtualTime
	duplicate bool
	dupDelay  VirtualTime
}

// route decides the fate of one message. It draws from the RNG in a fixed
// order — drop, then delay, then duplicate, then hook adjustments — so the
// consumed random sequence is a deterministic function of the message
// stream. Messages across a partition boundary are dropped without
// consuming randomness (a partition is a structural fact, not a dice roll),
// keeping the RNG stream stable when only the topology changes.
func (n *network) route(from, to uint64, m raft.Message) delivery {
	if !n.connected(from, to) {
		return delivery{deliver: false}
	}

	d := delivery{deliver: true}
	if n.cfg.DropRate > 0 && n.rng.Float64() < n.cfg.DropRate {
		d.deliver = false
	}
	d.delay = n.drawDelay()
	if n.cfg.DupRate > 0 && n.rng.Float64() < n.cfg.DupRate {
		d.duplicate = true
		d.dupDelay = n.drawDelay()
	}

	// Scenario hook runs last and can override the distributions. It must
	// be deterministic (no wall clock, no private RNG).
	if n.hook != nil {
		v := n.hook(from, to, m)
		if v.Drop {
			d.deliver = false
			d.duplicate = false
		}
		if v.Duplicate {
			// A hook may force a duplicate even when the distribution did
			// not; give it the same delay as the primary if none was drawn.
			if !d.duplicate {
				d.duplicate = true
				d.dupDelay = d.delay
			}
		}
		d.delay += v.ExtraDelay
		if d.duplicate {
			d.dupDelay += v.ExtraDelay
		}
	}
	return d
}

// drawDelay returns a uniform delay in [DelayMin, DelayMax]. When the
// bounds are equal (or inverted) the delay is fixed at DelayMin, consuming
// no randomness so degenerate configs keep the RNG stream stable.
func (n *network) drawDelay() VirtualTime {
	if n.cfg.DelayMax <= n.cfg.DelayMin {
		return n.cfg.DelayMin
	}
	span := int64(n.cfg.DelayMax - n.cfg.DelayMin)
	return n.cfg.DelayMin + VirtualTime(n.rng.Int63n(span+1))
}
