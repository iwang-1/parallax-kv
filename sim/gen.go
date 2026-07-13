package sim

import (
	"strconv"

	"github.com/iwang-1/parallax-kv/kv"
)

// This file holds the deterministic random draws the workload makes. Every
// draw goes through the simulator's single seeded RNG, in event order, so
// the whole workload is reproducible from the seed.

// genCommand builds the next command for a client: an operation type drawn
// from the configured mix, a key drawn from the key space, and a value
// derived deterministically from (client, seq) so payloads are stable
// across a replay.
func (s *Simulator) genCommand(c *client) kv.Command {
	w := s.cfg.Workload
	key := s.drawKey()
	cmd := kv.Command{ClientID: c.id, Seq: c.seq, Key: key}
	r := s.rng.Float64()
	switch {
	case r < w.PutRatio:
		cmd.Op = kv.OpPut
		cmd.Value = s.valueFor(c)
	case r < w.PutRatio+w.DeleteRatio:
		cmd.Op = kv.OpDelete
	case r < w.PutRatio+w.DeleteRatio+w.CasRatio:
		cmd.Op = kv.OpCAS
		cmd.Value = s.valueFor(c)
		// Expect a stable, likely-mismatching prior value most of the time;
		// occasionally target create-if-absent (nil Expect). The choice is
		// RNG-driven so it is reproducible.
		if s.rng.Float64() < 0.5 {
			cmd.Expect = []byte("v-" + strconv.FormatUint(c.id, 10))
		}
	default:
		cmd.Op = kv.OpGet
	}
	return cmd
}

// valueFor returns a deterministic value payload for a client's current
// operation. It embeds (client, seq) so distinct writes carry distinct
// bytes, which makes linearization histories easier to diagnose.
func (s *Simulator) valueFor(c *client) []byte {
	b := make([]byte, 0, 24)
	b = append(b, "v-"...)
	b = strconv.AppendUint(b, c.id, 10)
	b = append(b, '-')
	b = strconv.AppendUint(b, c.seq, 10)
	return b
}

// drawKey picks a key uniformly from a small keyspace. A small keyspace
// concentrates contention, which is what makes linearizability checking
// interesting. Keys default to a single key when Keys <= 1.
func (s *Simulator) drawKey() string {
	n := s.cfg.Workload.Keys
	if n <= 1 {
		return "k0"
	}
	return "k" + strconv.Itoa(s.rng.Intn(n))
}

// drawThink returns a think time uniform in [ThinkMin, ThinkMax]. When the
// bounds are unset or degenerate it falls back to one tick interval so
// clients still make progress without consuming randomness.
func (s *Simulator) drawThink() VirtualTime {
	lo, hi := s.cfg.Workload.ThinkMin, s.cfg.Workload.ThinkMax
	if hi <= lo {
		if lo > 0 {
			return lo
		}
		return s.cfg.TickEvery
	}
	span := int64(hi - lo)
	return lo + VirtualTime(s.rng.Int63n(span+1))
}

// retryTimeout is how long a client waits for an outstanding op before
// resubmitting it. It is generous relative to the election timeout so a
// single leader change does not cause a retry storm, and it draws no
// randomness (keeping the RNG stream tied to network/workload choices).
func (s *Simulator) retryTimeout() VirtualTime {
	return VirtualTime(s.cfg.ElectionTicks*4) * s.cfg.TickEvery
}
