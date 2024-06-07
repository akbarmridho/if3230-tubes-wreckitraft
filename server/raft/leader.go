package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
	"sort"
	"time"
)

func (r *RaftNode) sendAppendEntries(
	req ReceiveAppendEntriesArgs, resp *ReceiveAppendEntriesResponse, peer NodeConfiguration,
) error {
	logger.Log.Info(fmt.Sprintf("Sending append entries to: %d at %s", peer.ID, time.Now()))
	err := peer.getRpcClient()

	if err != nil {
		logger.Log.Warn(err)
		return err
	}

	err = peer.rpcClient.Call("RaftNode.ReceiveAppendEntries", &req, resp)

	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send append entries to: %d failed", peer.ID))
		return err
	}

	return nil
}

func (r *RaftNode) replicateLog() {
	// Reset heartbeat ticker
	r.resetHeartbeatTicker()

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			continue
		}

		// Log replication to followers
		logger.Log.Info(fmt.Sprintf("Leader is replicating log to node %d", peer.ID))
		go r.appendEntries(peer)
	}

	var matchIndices []uint64
	for _, index := range r.matchIndex {
		matchIndices = append(matchIndices, index)
	}
	sort.Slice(matchIndices, func(i, j int) bool { return matchIndices[i] < matchIndices[j] })

	majorityIndex := matchIndices[(len(matchIndices)-1)/2]

	logs, _ := r.logs.GetLogs()
	if majorityIndex > r.getCommitIndex() && logs[majorityIndex].Term == r.currentTerm {
		r.commitLog(majorityIndex)
		r.setCommitIndex(majorityIndex)
	}

}
