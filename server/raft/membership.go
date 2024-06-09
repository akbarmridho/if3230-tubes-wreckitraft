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
	r.setLatestConfiguration(newConfig, r.getLastIndex())
	err = r.appendLog(LogRequest{Type: CONFIGURATION, Data: encoded})

	if err != nil {
		return err
	}

	r.replicateLog()

	return nil
}

// requestConfigChange is a helper for making configuration change request
func (r *RaftNode) requestConfigChange(request ConfigurationChangeRequest) error {
	if !r.IsLeader() || r.GetConfig().Status != Voter {
		return types.NotALeaderError
	}

	return r.appendConfigurationEntry(request)
}

func (r *RaftNode) setLatestConfiguration(c Configuration, index uint64) {
	if r.configurations.latestIndex != index {
		r.lock.Lock()
		r.configurations.latest = c
		r.configurations.latestIndex = index
		logger.Log.Debug(fmt.Sprintf("latest config set %+v with index %d\n", c, index))

		var merged = make(map[uint64]NodeConfiguration)

		// update matchindex nextindex
		for _, server := range c.Servers {
			merged[server.ID] = server

			host := server.Address.Host()
			_, okMatch := r.matchIndex[host]
			_, okNext := r.nextIndex[host]

			if !okMatch {
				r.matchIndex[host] = 0
			}

			if !okNext {
				r.nextIndex[host] = 1
			}
		}

		for _, server := range r.configurations.commited.Servers {
			_, exist := merged[server.ID]

			if !exist {
				merged[server.ID] = server
			}
		}

		var mergedList []NodeConfiguration

		for _, server := range merged {
			mergedList = append(mergedList, server)
		}

		r.configurations.mergedServers = Configuration{Servers: mergedList}
		r.lock.Unlock()

		if !r.configurations.latest.HasServer(r.id) {
			logger.Log.Info(fmt.Sprintf("This node has been removed from cluster. Exiting ..."))
			panic("This node has been removed from cluster. Exiting ...")
		}
	}
}

func (r *RaftNode) commitLatestConfiguration() {
	if r.configurations.commitedIndex != r.configurations.latestIndex {
		r.clustersLock.Lock()

		r.configurations.commited = r.configurations.latest.Clone()
		r.configurations.mergedServers = r.configurations.commited
		r.configurations.commitedIndex = r.configurations.latestIndex
		r.clusters = r.configurations.commited.Clone()

		logger.Log.Debug(fmt.Sprintf("commited config set %+v with index %d\n", r.clusters, r.configurations.commitedIndex))

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
