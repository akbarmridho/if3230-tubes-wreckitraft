package raft

import (
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
	"sync"
)

func (r *RaftNode) sendAppendEntries(
	req ReceiveAppendEntriesArgs, resp *ReceiveAppendEntriesResponse, peer NodeConfiguration,
) error {
	//logger.Log.Info(fmt.Sprintf("Sending append entries to: %d at %s", peer.ID, time.Now()))
	client, err := peer.GetRpcClient()

	if err != nil {
		//logger.Log.Warn(err)
		return err
	}

	err = client.Call("RaftNode.ReceiveAppendEntries", &req, resp)

	if err != nil {
		//logger.Log.Warn(fmt.Sprintf("Send append entries to: %d failed", peer.ID))
		peer.UnsetRpcClient()
		return err
	}

	return nil
}

func (r *RaftNode) replicateLog() error {
	var mu sync.Mutex
	cond := sync.NewCond(&mu)

	config := r.GetConfig()
	r.clustersLock.RLock()

	matchIndexCounter := make(map[uint64]int)
	lastLogIndex, _ := r.getLastLog()
	matchIndexCounter[lastLogIndex]++

	majority := (r.clusters.VoterCount() / 2) + 1
	majorityAchieved := false

	// Read peers from latest configuration (could be commited or uncommited)
	// this is done in order to make the upcoming configuration could catch up
	for _, peer := range r.configurations.mergedServers.Servers {
		if peer.ID == config.ID {
			continue
		}

		// Log replication to followers
		logger.Log.Info(fmt.Sprintf("Leader is replicating log to node %d", peer.ID))
		go func(peer NodeConfiguration) {
			err := r.appendEntries(peer, false)
			mu.Lock()
			if err != nil {
				matchIndexCounter[uint64(0)]++
			} else {
				matchIndex := r.getMatchIndex(peer.Address.Host())
				matchIndexCounter[matchIndex]++
			}
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
	logger.Log.Info(fmt.Sprintf("Majority index %d Current commit %d", majorityIndex, r.getCommitIndex()))

	if majorityIndex == 0 {
		return ErrNotMajority
	}

	logs, _ := r.logs.GetLogs()

	if majorityIndex > r.getCommitIndex() && uint64(len(logs)) >= majorityIndex && logs[majorityIndex-1].Term == r.currentTerm {
		r.commitLatestConfiguration()
		r.commitLog(majorityIndex)
		r.setCommitIndex(majorityIndex)
	}

	return nil
}

var ErrNotMajority = errors.New("failed to replicate log, majority isn't reached")

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
