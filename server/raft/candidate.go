package raft

import "if3230-tubes-wreckitraft/shared"

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
}

func (r *RaftNode) sendRequestVote(req RequestVoteArgs, resp *RequestVoteResponse, id string, address shared.Address) {

}
