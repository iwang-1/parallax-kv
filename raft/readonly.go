package raft

// readIndexStatus tracks one in-flight ReadIndex request: the commit index
// recorded when the request was made, the originating request message, and
// the set of peers that have confirmed leadership by acknowledging the
// heartbeat that carried this request's context.
type readIndexStatus struct {
	index uint64
	req   Message
	acks  map[uint64]bool
}

// readOnly serializes ReadIndex requests. Because a heartbeat quorum
// confirmation for a later request also confirms all earlier ones, requests
// are held in FIFO order and released as a batch. The context of the most
// recent request is the confirmation token broadcast in heartbeats.
type readOnly struct {
	pendingReadIndex map[string]*readIndexStatus
	readIndexQueue   []string
}

func newReadOnly() *readOnly {
	return &readOnly{pendingReadIndex: make(map[string]*readIndexStatus)}
}

// addRequest records a new ReadIndex request at commit index idx. Duplicate
// contexts are ignored.
func (ro *readOnly) addRequest(idx uint64, m Message) {
	s := string(m.Context)
	if _, ok := ro.pendingReadIndex[s]; ok {
		return
	}
	ro.pendingReadIndex[s] = &readIndexStatus{index: idx, req: m, acks: make(map[uint64]bool)}
	ro.readIndexQueue = append(ro.readIndexQueue, s)
}

// recvAck records that id acknowledged the heartbeat carrying context and
// returns the number of acks now recorded for it.
func (ro *readOnly) recvAck(id uint64, context []byte) int {
	rs, ok := ro.pendingReadIndex[string(context)]
	if !ok {
		return 0
	}
	rs.acks[id] = true
	return len(rs.acks)
}

// advance releases every request up to and including the one identified by
// context (the newest confirmed request), returning them in FIFO order.
func (ro *readOnly) advance(context []byte) []*readIndexStatus {
	target := string(context)
	var rss []*readIndexStatus
	found := false
	i := 0
	for _, ctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[ctx]
		if !ok {
			continue
		}
		rss = append(rss, rs)
		if ctx == target {
			found = true
			break
		}
	}
	if !found {
		return nil
	}
	ro.readIndexQueue = ro.readIndexQueue[i:]
	for _, rs := range rss {
		delete(ro.pendingReadIndex, string(rs.req.Context))
	}
	return rss
}

// lastPendingRequestCtx returns the context of the most recent request, which
// is the token broadcast in heartbeats for confirmation.
func (ro *readOnly) lastPendingRequestCtx() []byte {
	if len(ro.readIndexQueue) == 0 {
		return nil
	}
	return ro.pendingReadIndex[ro.readIndexQueue[len(ro.readIndexQueue)-1]].req.Context
}
