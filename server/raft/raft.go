package raft

import (
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/constant"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"if3230-tubes-wreckitraft/util"
	"math/rand"
	"sync"
	"time"
)

type RaftNode struct {
	raftState

	// Track the latest configuration and latest commited configuration
	configurations Configurations

	id                uint64
	clusters          Configuration
	clustersLock      sync.RWMutex
	clusterLeader     *NodeConfiguration
	clusterLeaderLock sync.RWMutex

	logs             LogStore
	stable           StableStore
	heartbeatTimeout time.Duration
	electionTimeout  time.Duration
	lastContact      time.Time
	lastContactLock  sync.RWMutex

	// channels for communication among threads
	shutdownChannel chan struct{} // for shutdown to exit
	shutdownLock    sync.Mutex

	// storage
	state string

	// FSM is the client state machine to apply commands to
	fsm FSM

	heartbeatTicker *time.Ticker
}

func NewRaftNode(address shared.Address, fsm FSM, localID uint64, initialCluster []NodeConfiguration) (*RaftNode, error) {
	store := Store{
		BaseDir: fmt.Sprintf("data_%d", localID),
	}

	currentTerm, err := store.Get(keyCurrentTerm)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	if currentTerm == nil {
		newTerm := uint64(0)
		currentTerm = &newTerm
	}

	logs, err := store.GetLogs()
	if err != nil && !errors.Is(err, ErrLogNotFound) {
		return nil, err
	}

	var lastLog Log
	if len(logs) > 0 {
		lastIndex := len(logs)
		lastLog = logs[lastIndex-1]
	}

	logger.Log.Info(fmt.Sprintf("Found %d logs", len(logs)))
	logger.Log.Info(fmt.Sprintf("logs %+v", logs))

	configLogFound := false
	var configLog Log

	for _, log := range logs {
		if log.Type == CONFIGURATION {
			configLogFound = true
			configLog = log
		}
	}

	var clusters []NodeConfiguration

	if configLogFound {
		logger.Log.Info("Previous node configuration found. Ignoring cluster data from program arguments")
		decodedConfig, err := DecodeConfiguration(configLog.Data)

		if err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to decode configuration %s", err.Error()))
			panic("Failed to load configuration data")
		}

		clusters = decodedConfig.Servers
	} else {
		clusters = append(clusters, initialCluster...)
		clusters = append(clusters, NodeConfiguration{ID: localID, Address: address})
	}

	logger.Log.Info("Current ID: ", localID)

	node := RaftNode{
		id:       localID,
		fsm:      fsm,
		logs:     store,
		stable:   store,
		clusters: Configuration{Servers: clusters},
		configurations: Configurations{
			latestIndex:   lastLog.Index,
			commitedIndex: lastLog.Index,
			commited:      Configuration{Servers: clusters},
			latest:        Configuration{Servers: clusters},
			mergedServers: Configuration{Servers: clusters},
		},
		electionTimeout: time.Millisecond * 500,
	}

	logger.Log.Info(fmt.Sprintf("used clusters %+v", clusters))

	node.setCurrentTerm(*currentTerm)
	node.setLastLog(lastLog.Index, lastLog.Term)

	node.nextIndex = map[string]uint64{}
	node.matchIndex = map[string]uint64{}

	node.setCommitIndex(0)

	// Set up heartbeat
	node.setHeartbeatTimeout()

	node.goFunc(node.run)
	return &node, nil
}

func (r *RaftNode) setHeartbeatTimeout() {
	minDuration := 1.5 * constant.HEARTBEAT_INTERVAL * time.Millisecond
	maxDuration := 2 * constant.HEARTBEAT_INTERVAL * time.Millisecond

	r.heartbeatTimeout = util.RandomTimeout(minDuration, maxDuration)
}

func (r *RaftNode) run() {
	for {
		logger.Log.Info("Current state: ", r.getState().getName())
		switch r.getState() {
		case FOLLOWER:
			r.runFollower()
		case CANDIDATE:
			r.runCandidate()
		case LEADER:
			r.runLeader()
		}
	}
}

func (r *RaftNode) runFollower() {
	heartbeatTimer := r.getTimeout(r.heartbeatTimeout)
	for r.getState() == FOLLOWER {
		select {
		case <-heartbeatTimer:
			// not timed out
			heartbeatTimeout := r.heartbeatTimeout
			if time.Since(r.getLastContact()) < heartbeatTimeout {
				//logger.Log.Info("Heartbeat not timed out")
				heartbeatTimer = r.getTimeout(heartbeatTimeout)
				continue
			}

			if r.GetConfig().Status == Voter {
				logger.Log.Warn("Heartbeat timeout")
				// time out occurs
				r.clusterLeader = nil
				r.setState(CANDIDATE)
			} else if r.clusterLeader != nil {
				logger.Log.Warn("Heartbeat timeout but node is not a voter. Waiting for new leader")
				r.clusterLeader = nil
			}

			return
		}
	}
}

func (r *RaftNode) runCandidate() {
	if r.GetConfig().Status == Nonvoter {
		return
	}

	logger.Log.Info(fmt.Sprintf("Running node: %d as candidate", r.GetConfig().ID))
	votesChannel := r.startElection()
	r.electionTimeout = util.RandomTimeout(
		constant.ELECTION_TIMEOUT_MIN*time.Millisecond, constant.ELECTION_TIMEOUT_MAX*time.Millisecond,
	)
	electionTimer := r.getTimeout(r.electionTimeout)

	r.clustersLock.RLock()
	voterCount := r.clusters.VoterCount()
	r.clustersLock.RUnlock()

	majorityThreshold := (voterCount / 2) + 1
	votesReceived := 0
	for r.getState() == CANDIDATE {
		select {
		case v := <-votesChannel:
			if v.Term > r.getCurrentTerm() {
				r.setState(FOLLOWER)
				r.stable.Set(keyCurrentTerm, v.Term)
				r.setCurrentTerm(v.Term)
				return
			}
			if v.Granted {
				votesReceived += 1
				logger.Log.Info(fmt.Sprintf("Received vote from: %d", v.VoterID))
				if votesReceived >= majorityThreshold {
					config := r.GetConfig()
					logger.Log.Info(fmt.Sprintf("%d won the election", config.ID))
					r.setState(LEADER)
					r.setClusterLeader(config)
					r.initializeLeaderAttributes()
					r.replicateLog()
					return
				}
			}
		case <-electionTimer:
			logger.Log.Warn(fmt.Sprintf("Election timeout for %d", r.GetConfig().ID))
			return
		}
	}
}

func (r *RaftNode) initializeLeaderAttributes() {
	nextIndex := map[string]uint64{}

	lastLogIndex, _ := r.getLastLog()

	for _, cluster := range r.configurations.mergedServers.Servers {
		if r.id == cluster.ID {
			continue
		} else {
			nextIndex[cluster.Address.Host()] = lastLogIndex + 1
			r.setMatchIndex(cluster.Address.Host(), 0)
		}
	}

	r.setNextIndex(nextIndex)
}

func (r *RaftNode) startElection() <-chan *RequestVoteResponse {
	r.clustersLock.RLock()
	voterCount := r.clusters.VoterCount()
	r.clustersLock.RUnlock()
	votesChannel := make(chan *RequestVoteResponse, voterCount)
	newTerm := r.getCurrentTerm() + 1
	r.setCurrentTerm(newTerm)
	r.stable.Set(keyCurrentTerm, newTerm)

	config := r.GetConfig()

	lastLogIndex, lastTerm := r.getLastLog()
	req := RequestVoteArgs{
		Term:         r.getCurrentTerm(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastTerm,
	}
	req.CandidateID = config.ID

	requestVoteFromPeer := func(peer NodeConfiguration) {
		r.goFunc(
			func() {
				var resp RequestVoteResponse
				r.sendRequestVote(req, &resp, peer)
				votesChannel <- &resp
			},
		)
	}

	r.clustersLock.RLock()

	for _, peer := range r.clusters.Servers {
		if peer.Status == Nonvoter {
			continue
		}

		if peer.ID == config.ID {
			votesChannel <- &RequestVoteResponse{
				Term:    req.Term,
				Granted: true,
				VoterID: config.ID,
			}
		} else {
			requestVoteFromPeer(peer)
		}
	}

	r.clustersLock.RUnlock()

	return votesChannel
}

func (r *RaftNode) runLeader() {
	r.sendHeartbeat()
	r.createHeartbeatTicker()
	defer r.stopHeartbeatTicker()

	for r.getState() == LEADER {
		select {
		case <-r.heartbeatTicker.C:
			r.sendHeartbeat()
		}
	}

	config := r.GetConfig()

	logger.Log.Info(fmt.Sprintf("%s is no longer the leader", config.GetHost()))
}

func (r *RaftNode) createHeartbeatTicker() {
	r.heartbeatTicker = time.NewTicker(time.Duration(constant.HEARTBEAT_INTERVAL) * time.Millisecond)
}

func (r *RaftNode) stopHeartbeatTicker() {
	if r.heartbeatTicker != nil {
		r.heartbeatTicker.Stop()
		r.heartbeatTicker = nil
	}
}

func (r *RaftNode) resetHeartbeatTicker() {
	r.stopHeartbeatTicker()
	r.createHeartbeatTicker()
}

func (r *RaftNode) sendHeartbeat() {
	// Sample sending heartbeat at 10%
	if rand.Float32() < 0.1 {
		logger.Log.Info(fmt.Sprintf("Sending heartbeat at: %s", time.Now()))
	}

	// send heartbeat to latest configuration (could be commited or uncommited)
	for _, peer := range r.configurations.mergedServers.Servers {
		if peer.ID == r.GetConfig().ID {
			continue
		}
		// Send heartbeat
		//logger.Log.Info(fmt.Sprintf("Leader is sending heartbeat to %s:%d", peer.Address.IP, peer.Address.Port))

		go r.appendEntries(peer, true)
	}
}

func (r *RaftNode) getTimeout(timeout time.Duration) <-chan time.Time {
	return time.After(timeout)
}

func (r *RaftNode) setClusterLeader(config NodeConfiguration) {
	logger.Log.Debug(fmt.Sprintf("set leader to %+v", config))
	r.clusterLeaderLock.Lock()
	r.clusterLeader = &config
	r.clusterLeaderLock.Unlock()
}

func (r *RaftNode) getLastContact() time.Time {
	r.lastContactLock.RLock()
	lastContact := r.lastContact
	r.lastContactLock.RUnlock()
	return lastContact
}

func (r *RaftNode) setLastContact() {
	r.lastContactLock.RLock()
	r.lastContact = time.Now()
	r.lastContactLock.RUnlock()
}

func (r *RaftNode) ReceiveRequestVote(args *RequestVoteArgs, reply *RequestVoteResponse) error {
	logger.Log.Info("Received request vote from: ", args.CandidateID)
	reply.Term = r.getCurrentTerm()
	reply.Granted = false
	reply.VoterID = r.GetConfig().ID

	if r.currentTerm > args.Term {
		return nil
	}

	if r.currentTerm == args.Term && r.IsLeader() {
		return nil
	}

	lastVotedTerm, err := r.stable.Get(keyLastVoteTerm)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil
	}
	lastVotedCand, err := r.stable.Get(keyLastVotedCand)
	if err != nil && !errors.Is(err, ErrKeyNotFound) {
		return nil
	}

	// if we have voted in this term, then don't give vote
	if lastVotedTerm != nil && *lastVotedTerm == args.Term && lastVotedCand != nil {
		return nil
	}

	lastLogTerm, lastLogIndex := r.getLastLog()
	if lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex {
		return nil
	}

	if lastLogTerm > args.LastLogTerm {
		return nil
	}

	r.stable.Set(keyLastVotedCand, args.CandidateID)
	r.stable.Set(keyLastVoteTerm, args.Term)
	reply.Granted = true
	return nil
}

func (r *RaftNode) appendLog(request LogRequest) error {
	logs, err := r.logs.GetLogs()
	if err != nil && !errors.Is(err, ErrLogNotFound) {
		return err
	}

	index := uint64(len(logs) + 1)
	term := r.getCurrentTerm()

	newLog := Log{
		Index: index,
		Term:  term,
		Type:  request.Type,
		Data:  request.Data,
	}

	logs = append(logs, newLog)
	err = r.logs.StoreLogs(logs)
	if err != nil {
		return err
	}

	r.setLastLog(index, term)
	return nil
}

func (r *RaftNode) appendEntries(peer NodeConfiguration, isHeartbeat bool) {
	logs, _ := r.logs.GetLogs()

	appendEntry := ReceiveAppendEntriesArgs{
		Term:         r.getCurrentTerm(),
		LeaderConfig: r.GetConfig(),
		Entries:      nil,
		LeaderCommit: r.getCommitIndex(),
	}

	nextIndex := r.getNextIndex()

	var resp ReceiveAppendEntriesResponse
	for {
		// break if not a leader anymore
		if !r.IsLeader() {
			break
		}

		r.lock.Lock()
		index, ok := nextIndex[peer.Address.Host()]
		r.lock.Unlock()

		prevLogIndex := uint64(0)
		if !ok {
			index = 0
		}
		if index > 0 {
			prevLogIndex = index - 1
		}

		appendEntry.PrevLogIndex = prevLogIndex
		appendEntry.PrevLogTerm = 0

		if prevLogIndex > 0 && (prevLogIndex-1) < uint64(len(logs)) {
			appendEntry.PrevLogTerm = logs[prevLogIndex-1].Term
		}

		lastLogIndex, _ := r.getLastLog()

		if lastLogIndex >= index && !isHeartbeat {
			logger.Log.Debug(fmt.Sprintf("last log index %d with index %d and logs length %d\n", lastLogIndex, index, len(logs)))
			appendEntry.Entries = logs[index-1:]
			logger.Log.Debug(fmt.Sprintf("append entry length %d", len(appendEntry.Entries)))
		}
		err := r.sendAppendEntries(appendEntry, &resp, peer)

		if err != nil {
			break
		}

		// TODO: handle resp.term
		if resp.Success {
			//logger.Log.Info(fmt.Sprintf("Success send append entries to %d", peer.ID))
			if len(appendEntry.Entries) > 0 && !isHeartbeat {
				r.lock.Lock()
				nextIndex[peer.Address.Host()] += uint64(len(appendEntry.Entries))
				r.lock.Unlock()
				r.setNextIndex(nextIndex)
			}
			r.setMatchIndex(peer.Address.Host(), nextIndex[peer.Address.Host()]-1)
			break
		} else {
			logger.Log.Info(fmt.Sprintf("Failed send append entries to %d", peer.ID))
			r.lock.Lock()
			nextIndex[peer.Address.Host()]--
			r.lock.Unlock()
		}
	}
}

func (r *RaftNode) commitLog(newCommitIndex uint64) {
	currentCommitIdx := r.getCommitIndex()
	if newCommitIndex <= currentCommitIdx {
		return
	}
	logs, _ := r.logs.GetLogs()
	for i := currentCommitIdx + 1; i <= newCommitIndex; i++ {
		if i == 0 {
			continue
		}
		logger.Log.Info(fmt.Sprintf("From node:%d applying log to fsm with index %d", r.GetConfig().ID, i))

		log := logs[i-1]

		if log.Type == COMMAND {
			r.fsm.Apply(&log)
		} else if log.Type == CONFIGURATION {
			// todo is there something need to be done here?
		}
	}
}

// Apply is used to apply a command to the FSM in a highly consistent
// manner. This returns a future that can be used to wait on the application.
// An optional timeout can be provided to limit the amount of time we wait
// for the command to be started. This must be run on the leader or it
// will fail.
//
// If the node discovers it is no longer the leader while applying the command,
// it will return ErrLeadershipLost. There is no way to guarantee whether the
// write succeeded or failed in this case. For example, if the leader is
// partitioned it can't know if a quorum of followers wrote the log to disk. If
// at least one did, it may survive into the next leader's term.
//
// If a user snapshot is restored while the command is in-flight, an
// ErrAbortedByRestore is returned. In this case the write effectively failed
// since its effects will not be present in the FSM after the restore.
func (r *RaftNode) Apply(payload []byte) error {
	err := r.appendLog(LogRequest{
		Type: COMMAND,
		Data: payload,
	})
	if err != nil {
		return err
	}
	r.replicateLog()
	return nil
}

type LogArgs struct{}

type LogReply struct {
	Log           []string
	LeaderAddress string
}

func (r *RaftNode) IsLeader() bool {
	r.clusterLeaderLock.RLock()
	defer r.clusterLeaderLock.RUnlock()
	return r.clusterLeader != nil && r.clusterLeader.ID == r.GetConfig().ID
}

func (r *RaftNode) IsCandidate() bool {
	r.clusterLeaderLock.RLock()
	defer r.clusterLeaderLock.RUnlock()
	return r.raftState.state == CANDIDATE
}

func (r *RaftNode) GetLeaderAddress() string {
	r.clusterLeaderLock.RLock()
	defer r.clusterLeaderLock.RUnlock()
	if r.clusterLeader != nil {
		return fmt.Sprintf("%s:%d", r.clusterLeader.Address.IP, r.clusterLeader.Address.Port)
	}
	return ""
}

func (r *RaftNode) GetRequestLog() ([]Log, error) {
	return r.logs.GetLogs()
}
func (r *RaftNode) GetConfig() NodeConfiguration {
	r.clustersLock.RLock()
	defer r.clustersLock.RUnlock()

	var me NodeConfiguration

	for _, server := range r.clusters.Servers {
		if server.ID == r.id {
			me = server
			break
		}
	}

	return me
}
