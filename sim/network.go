package sim

import (
	"math/rand"

	"github.com/iwang-1/parallax-kv/raft"
)

// Network is the simulated message fabric.
type network struct {
	cfg NetworkConfig
	rng *rand.Rand

	// Partition assigns each node to a partition group. Two nodes can exchange messages only if they share a
	// group id.
	partition map[uint64]int

	hook MessageHook
}

func newNetwork(cfg NetworkConfig, rng *rand.Rand) *network {
	return &network{cfg: cfg, rng: rng}
}

// Connected reports whether from and to are in the same partition group (and therefore able to exchange
// messages).
func (n *network) connected(from, to uint64) bool {
	if n.partition == nil {
		return true
	}
	return n.partition[from] == n.partition[to]
}

// SetPartition installs a grouping. groups is a list of node-id slices; each slice becomes one isolated
// group.
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

// Delivery is the outcome of routing one message: whether to deliver it, after what delay, and whether to
// schedule a duplicate (with its own independently drawn delay).
type delivery struct {
	deliver   bool
	delay     VirtualTime
	duplicate bool
	dupDelay  VirtualTime
}

// Route decides the fate of one message. It draws from the rng in a fixed order — drop, then delay, then
// duplicate, then hook adjustments — so the consumed random sequence is a deterministic function of the message stream.
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

// DrawDelay returns a uniform delay in [DelayMin, DelayMax]. When the bounds are equal (or inverted) the
// delay is fixed at DelayMin, consuming no randomness so degenerate configs keep the rng stream stable.
func (n *network) drawDelay() VirtualTime {
	if n.cfg.DelayMax <= n.cfg.DelayMin {
		return n.cfg.DelayMin
	}
	span := int64(n.cfg.DelayMax - n.cfg.DelayMin)
	return n.cfg.DelayMin + VirtualTime(n.rng.Int63n(span+1))
}
