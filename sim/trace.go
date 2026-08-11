package sim

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strconv"
)

// Recorder accumulates the run's canonical event trace.
type recorder struct {
	events []TraceEvent
	// hasher is the running SHA-256 state.
	hasher hashWriter
}

// hashWriter is the subset of hash.Hash the recorder uses; naming it keeps
// the struct field concrete and avoids an interface allocation surprise.
type hashWriter interface {
	Write(p []byte) (int, error)
	Sum(b []byte) []byte
}

func newRecorder() *recorder {
	return &recorder{hasher: sha256.New()}
}

// record appends an event and folds it into the running hash.
func (r *recorder) record(e TraceEvent) {
	r.events = append(r.events, e)
	// hash.Hash.Write never returns an error (documented contract); the
	// canonical bytes are folded into the running SHA-256 unconditionally.
	_, _ = r.hasher.Write(canonicalEvent(e))
}

// CanonicalEvent renders one event to its canonical byte form. Integers are big-endian fixed-width; strings
// are length-prefixed.
func canonicalEvent(e TraceEvent) []byte {
	// Sized to the common case; grows as needed.
	buf := make([]byte, 0, 32+len(e.Kind)+len(e.Detail))
	buf = binary.BigEndian.AppendUint64(buf, uint64(e.At))
	buf = binary.BigEndian.AppendUint64(buf, e.Seq)
	buf = binary.BigEndian.AppendUint64(buf, e.Node)
	buf = appendLenString(buf, e.Kind)
	buf = appendLenString(buf, e.Detail)
	return buf
}

func appendLenString(buf []byte, s string) []byte {
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(s)))
	return append(buf, s...)
}

// events returns a copy of the recorded events.
func (r *recorder) snapshot() []TraceEvent {
	out := make([]TraceEvent, len(r.events))
	copy(out, r.events)
	return out
}

// hash returns the lowercase-hex SHA-256 of the trace recorded so far.
func (r *recorder) hash() string {
	sum := r.hasher.Sum(nil)
	return hex.EncodeToString(sum)
}

// String renders a trace event in a stable, human-readable one-line form (used in failure dumps).
func (e TraceEvent) String() string {
	// at=<t> seq=<n> kind=<k> node=<id> <detail>
	b := make([]byte, 0, 48+len(e.Kind)+len(e.Detail))
	b = append(b, "at="...)
	b = strconv.AppendInt(b, int64(e.At), 10)
	b = append(b, " seq="...)
	b = strconv.AppendUint(b, e.Seq, 10)
	b = append(b, " kind="...)
	b = append(b, e.Kind...)
	b = append(b, " node="...)
	b = strconv.AppendUint(b, e.Node, 10)
	if e.Detail != "" {
		b = append(b, ' ')
		b = append(b, e.Detail...)
	}
	return string(b)
}
