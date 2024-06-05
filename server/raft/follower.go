package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
)

type ReceiveAppendEntriesArgs struct {
	Term         uint64
	LeaderConfig NodeConfiguration
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Log
	LeaderCommit uint64
}

type ReceiveAppendEntriesResponse struct {
	Term    uint64
	Success bool
}

// ReceiveAppendEntries Receive
func (r *RaftNode) ReceiveAppendEntries(args *ReceiveAppendEntriesArgs, reply *ReceiveAppendEntriesResponse) error {
	if r.getState() == CANDIDATE {
		logger.Log.Info("%s as candidate receive heartbeat, converted to follower", r.Config.ID)
		r.setState(FOLLOWER)
		r.setClusterLeader(args.LeaderConfig)
	}

	reply.Term = r.currentTerm

	// Receive heartbeat
	if r.getState() == FOLLOWER {
		logger.Log.Info("%s:%d receiving heartbeat", r.Config.Address.IP, r.Config.Address.Port)
		r.setLastContact()
	}

	if len(args.Entries) == 0 {
		return nil
	}

	if args.Term < r.currentTerm {
		logger.Log.Warn(fmt.Sprintf("Failed to receive append entries in node: %s because term < current term", r.Config.ID))
		reply.Success = false
		return nil
	}

	// Need to check if index from array logs and their index synchronized
	logs, _ := r.logs.GetLogs()
	if uint64(len(logs)) > args.PrevLogIndex {
		if logs[args.PrevLogIndex].Term != args.PrevLogTerm {
			logs = logs[:args.PrevLogIndex]
		}
	} else {
		logger.Log.Warn(fmt.Sprintf("Failed to receive append entries in node: %s because log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm", r.Config.ID))
		reply.Success = false
		return nil
	}

	for _, entry := range args.Entries {
		logs = append(logs, entry)
	}

	lastLog := logs[len(logs)-1]
	r.setLastLog(lastLog.Index, lastLog.Term)
	r.logs.StoreLogs(logs)

	if args.LeaderCommit > r.getCommitIndex() {
		index, _ := r.getLastLog()
		commitIdx := args.LeaderCommit
		if index < args.LeaderCommit {
			commitIdx = index
		}
		r.setCommitIndex(commitIdx)
	}

	reply.Success = true
	return nil
}
