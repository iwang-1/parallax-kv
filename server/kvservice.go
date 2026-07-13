package server

import (
	"context"

	"github.com/iwang-1/parallax-kv/kv"
	"github.com/iwang-1/parallax-kv/proto/kvpb"
)

// kvService adapts the drive loop to the client-facing KVService gRPC API.
// Every handler routes its operation into the single drive loop and blocks
// on the answer; a non-leader answer carries a redirect hint so the client
// can chase the leader. Mutations carry (clientID, seq) for exactly-once
// apply; reads take the ReadIndex path and never enter the log.
type kvService struct {
	kvpb.UnimplementedKVServiceServer
	s *Server
}

// redirectHeader builds the response header for a not-leader answer,
// resolving the leader's client address from the server's ClientAddrs map.
func (k *kvService) redirectHeader(leaderID uint64) *kvpb.ResponseHeader {
	h := &kvpb.ResponseHeader{NotLeader: true, LeaderId: leaderID}
	if addr, ok := k.s.cfg.ClientAddrs[leaderID]; ok {
		h.LeaderAddr = addr
	}
	return h
}

func (k *kvService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	r, ok := k.s.submit(ctx, &request{kind: reqRead, key: req.GetKey()})
	if !ok {
		return nil, ctx.Err()
	}
	if r.notLeader {
		return &kvpb.GetResponse{Header: k.redirectHeader(r.leaderID)}, nil
	}
	resp := &kvpb.GetResponse{Header: &kvpb.ResponseHeader{}}
	if r.res.Status == kv.StatusOK {
		resp.Found = true
		resp.Value = r.res.Value
		resp.Version = r.res.Version
	}
	return resp, nil
}

func (k *kvService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	cmd := kv.Command{
		ClientID: req.GetHeader().GetClientId(),
		Seq:      req.GetHeader().GetSeq(),
		Op:       kv.OpPut,
		Key:      req.GetKey(),
		Value:    req.GetValue(),
	}
	r, ok := k.s.submit(ctx, &request{kind: reqPropose, cmd: cmd})
	if !ok {
		return nil, ctx.Err()
	}
	if r.notLeader {
		return &kvpb.PutResponse{Header: k.redirectHeader(r.leaderID)}, nil
	}
	return &kvpb.PutResponse{Header: &kvpb.ResponseHeader{}, Version: r.res.Version}, nil
}

func (k *kvService) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	cmd := kv.Command{
		ClientID: req.GetHeader().GetClientId(),
		Seq:      req.GetHeader().GetSeq(),
		Op:       kv.OpDelete,
		Key:      req.GetKey(),
	}
	r, ok := k.s.submit(ctx, &request{kind: reqPropose, cmd: cmd})
	if !ok {
		return nil, ctx.Err()
	}
	if r.notLeader {
		return &kvpb.DeleteResponse{Header: k.redirectHeader(r.leaderID)}, nil
	}
	return &kvpb.DeleteResponse{Header: &kvpb.ResponseHeader{}, Found: r.res.Status == kv.StatusOK}, nil
}

func (k *kvService) Cas(ctx context.Context, req *kvpb.CasRequest) (*kvpb.CasResponse, error) {
	cmd := kv.Command{
		ClientID: req.GetHeader().GetClientId(),
		Seq:      req.GetHeader().GetSeq(),
		Op:       kv.OpCAS,
		Key:      req.GetKey(),
		Value:    req.GetValue(),
	}
	// expect_absent selects the create-if-absent variant (nil Expect); an
	// explicit Expect otherwise. A present, non-nil Expect and expect_absent
	// are mutually exclusive on the wire; expect_absent wins if both set.
	if !req.GetExpectAbsent() {
		// Preserve nil-vs-empty: an empty non-nil Expect means "current value
		// must be empty bytes", distinct from create-if-absent.
		exp := req.GetExpect()
		if exp == nil {
			exp = []byte{}
		}
		cmd.Expect = exp
	}
	r, ok := k.s.submit(ctx, &request{kind: reqPropose, cmd: cmd})
	if !ok {
		return nil, ctx.Err()
	}
	if r.notLeader {
		return &kvpb.CasResponse{Header: k.redirectHeader(r.leaderID)}, nil
	}
	resp := &kvpb.CasResponse{
		Header:  &kvpb.ResponseHeader{},
		Swapped: r.res.Status == kv.StatusOK,
		Version: r.res.Version,
	}
	if r.res.Status == kv.StatusCASMismatch {
		resp.Current = r.res.Value
	}
	return resp, nil
}
