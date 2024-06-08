package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/server/raft/types"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"strconv"
)

func (r *RaftNode) appendConfigurationEntry(request ConfigurationChangeRequest) error {
	newConfig := r.configurations.latest.CloneWithCommand(request)

	logger.Log.Info(fmt.Sprintf("updating configuration. command %s server-id %s server-address %s", request.command.String(), strconv.FormatUint(request.ServerID, 10), request.ServerAddress))

	encoded, err := EncodeConfiguration(newConfig)

	if err != nil {
		return err
	}

	err = r.appendLog(LogRequest{Type: CONFIGURATION, Data: encoded})

	if err != nil {
		return err
	}

	r.setLatestConfiguration(newConfig, r.getLastIndex())
	r.replicateLog()

	return nil
}

// requestConfigChange is a helper for making configuration change request
func (r *RaftNode) requestConfigChange(request ConfigurationChangeRequest) error {
	if !r.IsLeader() {
		return types.NotALeaderError
	}

	return r.appendConfigurationEntry(request)
}

func (r *RaftNode) setLatestConfiguration(c Configuration, index uint64) {
	r.configurations.latest = c
	r.configurations.latestIndex = index
}

func (r *RaftNode) commitLatestConfiguration() {
	if r.configurations.commitedIndex != r.configurations.latestIndex {
		r.clustersLock.Lock()
		r.configurations.commited = r.configurations.latest.Clone()
		r.configurations.commitedIndex = r.configurations.latestIndex
		r.clusters = r.configurations.commited.Clone()
		r.clustersLock.Unlock()
	}
}

// AddVoter will add the given server to the cluster as a staging server.
// Should be run on leader. The leader will promote the staging server to
// a voter once that the server is ready (e.g. log entries is catched up)
func (r *RaftNode) AddVoter(id uint64, address shared.Address) error {
	return r.requestConfigChange(ConfigurationChangeRequest{
		command:       AddVoter,
		ServerID:      id,
		ServerAddress: &address,
	})
}

// AddNonvoter will add the given server but won't assign it a vote
// The server will receive log entries, but it won't participate in
// elections or log entry commitment. Should be run on leader
func (r *RaftNode) AddNonvoter(id uint64, address shared.Address) error {
	return r.requestConfigChange(ConfigurationChangeRequest{
		command:       AddNonvoter,
		ServerID:      id,
		ServerAddress: &address,
	})
}

// RemoveServer will remove the given server from the cluster.
// If the current leader is being removed, it will cause a new election
// to occur. Should be run on leader
func (r *RaftNode) RemoveServer(id uint64) error {
	return r.requestConfigChange(ConfigurationChangeRequest{
		command:  RemoveServer,
		ServerID: id,
	})
}

// DemoteVoter will take away a server's vote, if it has one. If present,
// the server will continue to receive log entries, but it won't participate
// in elections or log entry commitment. Should be run on leader.
func (r *RaftNode) DemoteVoter(id uint64) error {
	return r.requestConfigChange(ConfigurationChangeRequest{
		command:  DemoteVoter,
		ServerID: id,
	})
}
