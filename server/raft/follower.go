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
	r.setLastContact()
	config := r.GetConfig()

	currState := r.getState()
	if currState == CANDIDATE || (args.Term > r.currentTerm && currState == LEADER) {
		logger.Log.Info(fmt.Sprintf("%d as candidate receive heartbeat, converted to follower. ack leader %+v", config.ID, args.LeaderConfig))
		r.setState(FOLLOWER)
		r.setClusterLeader(args.LeaderConfig)
	}

	reply.Term = r.currentTerm

	// Receive heartbeat
	if currState == FOLLOWER {
		//logger.Log.Info(fmt.Sprintf("Node %d receiving heartbeat at: %s", config.ID, time.Now()))
	}

	if args.Term < r.currentTerm {
		logger.Log.Warn(
			fmt.Sprintf(
				"Failed to receive append entries in node: %d because term < current term", config.ID,
			),
		)
		reply.Success = false
		return nil
	}

	// Need to check if index from array logs and their index synchronized
	logs, _ := r.logs.GetLogs()
	if args.PrevLogIndex > 0 && uint64(len(logs)) >= args.PrevLogIndex {
		if logs[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			logs = logs[:args.PrevLogIndex-1]
		}
	} else if args.PrevLogIndex > 0 {
		logger.Log.Warn(
			fmt.Sprintf(
				"Failed to receive append entries in node: %d because log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm",
				config.ID,
			),
		)
		reply.Success = false
		return nil
	}

	for _, entry := range args.Entries {
		logs = append(logs, entry)
	}

	if len(logs) > 0 {
		lastLog := logs[len(logs)-1]

		r.setLastLog(lastLog.Index, lastLog.Term)
		r.logs.StoreLogs(logs)
	}

	if len(args.Entries) > 0 {
		//logger.Log.Debug(fmt.Sprintf("received entries with length %d", len(args.Entries)))
		for _, entry := range args.Entries {
			if entry.Type == CONFIGURATION {
				r.commitLatestConfiguration()
				decodedConfig, err := DecodeConfiguration(entry.Data)

				if err != nil {
					logger.Log.Error(fmt.Sprintf("Failed to decode configuration %s", err.Error()))
					continue
				}

				logger.Log.Debug(fmt.Sprintf("set latest received from %d", args.LeaderConfig.ID))

				r.setLatestConfiguration(*decodedConfig, entry.Index)
			}
		}
	}

	if args.LeaderCommit > r.getCommitIndex() {
		index, _ := r.getLastLog()
		commitIdx := args.LeaderCommit
		if index < args.LeaderCommit {
			commitIdx = index
		}
		r.commitLog(commitIdx)
		r.setCommitIndex(commitIdx)

		if r.configurations.latestIndex <= index {
			r.commitLatestConfiguration()
		}
	}

	reply.Success = true
	return nil
}
