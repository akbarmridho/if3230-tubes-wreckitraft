package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
)

type RequestVoteArgs struct {
	Term         uint64
	CandidateID  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteResponse struct {
	Term    uint64
	Granted bool
	VoterID uint64
}

func (r *RaftNode) sendRequestVote(req RequestVoteArgs, resp *RequestVoteResponse, peer NodeConfiguration) {
	logger.Log.Info("Sending request vote to: ", peer.ID)
	err := peer.getRpcClient()

	if err != nil {
		logger.Log.Warn(err)
		return
	}

	err = peer.rpcClient.Call("RaftNode.ReceiveRequestVote", &req, resp)

	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send request vote to: %s failed", peer.ID))
		return
	}
	logger.Log.Info("Request vote response: ", *resp)
}
