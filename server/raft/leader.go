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
	//r.resetHeartbeatTicker()

	var wg sync.WaitGroup

	config := r.GetConfig()

	r.clustersLock.RLock()

	// Read peers from latest configuration (could be commited or uncommited)
	// this is done in order to make the upcoming configuration could catch up
	for _, peer := range r.configurations.latest.Servers {
		if peer.ID == config.ID {
			continue
		}

		// Log replication to followers
		logger.Log.Info(fmt.Sprintf("Leader is replicating log to node %d", peer.ID))
		wg.Add(1)
		go func(peer NodeConfiguration) {
			defer wg.Done()
			r.appendEntries(peer, false)
		}(peer)
	}

	r.clustersLock.RUnlock()

	// todo check ini gak nunggu majority tapi semua?
	wg.Wait()

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
}
