package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
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
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	config := r.GetConfig()
	r.clustersLock.RLock()

	matchIndexCounter := make(map[uint64]int)
	majority := (r.clusters.VoterCount() / 2) + 1
	majorityAchieved := false

	// Read peers from latest configuration (could be commited or uncommited)
	// this is done in order to make the upcoming configuration could catch up
	for _, peer := range r.configurations.latest.Servers {
		if peer.ID == config.ID {
			continue
		}

		// Log replication to followers
		logger.Log.Info(fmt.Sprintf("Leader is replicating log to node %d", peer.ID))
		go func(peer NodeConfiguration) {
			r.appendEntries(peer, false)
			mu.Lock()
			matchIndex := r.matchIndex[peer.Address.Host()]
			matchIndexCounter[matchIndex]++
			if r.isMajority(matchIndexCounter, majority) {
				majorityAchieved = true
				cond.Broadcast()
			}
			mu.Unlock()
		}(peer)
	}

	r.clustersLock.RUnlock()

	mu.Lock()
	for !majorityAchieved {
		cond.Wait()
	}
	mu.Unlock()

	majorityIndex := r.getMajorityMatchIndex(matchIndexCounter)

	logs, _ := r.logs.GetLogs()

	if majorityIndex > r.getCommitIndex() && logs[majorityIndex-1].Term == r.currentTerm {
		r.commitLog(majorityIndex)
		r.setCommitIndex(majorityIndex)
	}
}

func (r *RaftNode) isMajority(matchIndexCounter map[uint64]int, majority int) bool {
	for _, count := range matchIndexCounter {
		if count >= majority {
			return true
		}
	}
	return false
}

func (r *RaftNode) getMajorityMatchIndex(matchIndexCounter map[uint64]int) uint64 {
	var highestKey uint64
	highestCount := -1

	for key, count := range matchIndexCounter {
		if count > highestCount {
			highestCount = count
			highestKey = key
		}
	}

	return highestKey
}
