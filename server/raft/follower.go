package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
)

type ReceiveAppendEntriesArgs struct {
	term         uint64
	leaderConfig NodeConfiguration
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []Log
	leaderCommit uint64
}

type ReceiveAppendEntriesResponse struct {
	term    uint64
	success bool
}

// ReceiveAppendEntries Receive
func (r *RaftNode) ReceiveAppendEntries(args *ReceiveAppendEntriesArgs, reply *ReceiveAppendEntriesResponse) error {
	if r.getState() == CANDIDATE {
		logger.Log.Info("%s as candidate receive heartbeat, converted to follower", r.Config.ID)
		r.setState(FOLLOWER)
		r.setClusterLeader(args.leaderConfig)
	}

	reply.term = r.currentTerm

	// Receive heartbeat
	if r.getState() == FOLLOWER {
		logger.Log.Info("%s:%d receiving heartbeat", r.Config.Address.IP, r.Config.Address.Port)
		r.setLastContact()
	}

	if len(args.entries) == 0 {
		return nil
	}

	if args.term < r.currentTerm {
		logger.Log.Warn(fmt.Sprintf("Failed to receive append entries in node: %s because term < current term", r.Config.ID))
		reply.success = false
		return nil
	}

	// Need to check if index from array logs and their index synchronized
	logs, _ := r.logs.GetLogs()
	if uint64(len(logs)) > args.prevLogIndex {
		if logs[args.prevLogIndex].Term != args.prevLogTerm {
			logs = logs[:args.prevLogIndex]
		}
	} else {
		logger.Log.Warn(fmt.Sprintf("Failed to receive append entries in node: %s because log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm", r.Config.ID))
		reply.success = false
		return nil
	}

	for _, entry := range args.entries {
		logs = append(logs, entry)
	}

	lastLog := logs[len(logs)-1]
	r.setLastLog(lastLog.Index, lastLog.Term)
	r.logs.StoreLogs(logs)

	if args.leaderCommit > r.getCommitIndex() {
		index, _ := r.getLastLog()
		commitIdx := args.leaderCommit
		if index < args.leaderCommit {
			commitIdx = index
		}
		r.setCommitIndex(commitIdx)
	}

	reply.success = true
	return nil
}
