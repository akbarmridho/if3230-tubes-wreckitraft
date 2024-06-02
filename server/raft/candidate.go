package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
)

type RequestVoteArgs struct {
	term      uint64
	candidate struct {
		address shared.Address
		id      string
	}
	lastLogIndex uint64
	lastLogTerm  uint64
}

type RequestVoteResponse struct {
	term    uint64
	granted bool
	voterID string
}

func (r *RaftNode) sendRequestVote(req RequestVoteArgs, resp *RequestVoteResponse, peer NodeConfiguration) {
	rpcResp, err := peer.CallRPC("RaftNode.ReceiveRequestVote", req)
	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send request vote to: %s failed", peer.ID))
		return
	}
	*resp = rpcResp.(RequestVoteResponse)
}
