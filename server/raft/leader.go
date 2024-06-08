package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
	"sort"
	"sync"
	"time"
)

func (r *RaftNode) sendAppendEntries(
	req ReceiveAppendEntriesArgs, resp *ReceiveAppendEntriesResponse, peer NodeConfiguration,
) error {
	logger.Log.Info(fmt.Sprintf("Sending append entries to: %d at %s", peer.ID, time.Now()))
	client, err := peer.GetRpcClient()

	if err != nil {
		logger.Log.Warn(err)
		return err
	}

	err = client.Call("RaftNode.ReceiveAppendEntries", &req, resp)

	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send append entries to: %d failed", peer.ID))
		return err
	}

	return nil
}

func (r *RaftNode) replicateLog() {
	// Reset heartbeat ticker
	r.resetHeartbeatTicker()

	var wg sync.WaitGroup

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			continue
		}

		// Log replication to followers
		logger.Log.Info(fmt.Sprintf("Leader is replicating log to node %d", peer.ID))
		wg.Add(1)
		go func(peer NodeConfiguration) {
			defer wg.Done()
			r.appendEntries(peer)
		}(peer)
	}

	wg.Wait()
	r.sendHeartbeat()

	var matchIndices []uint64
	for _, index := range r.getMatchIndex() {
		matchIndices = append(matchIndices, index)
	}
	sort.Slice(matchIndices, func(i, j int) bool { return matchIndices[i] < matchIndices[j] })

	majorityIndex := matchIndices[(len(matchIndices)-1)/2]

	logs, _ := r.logs.GetLogs()

	if majorityIndex > r.getCommitIndex() && logs[majorityIndex-1].Term == r.currentTerm {
		r.commitLog(majorityIndex)
		r.setCommitIndex(majorityIndex)
	}
	r.sendHeartbeat()
}
