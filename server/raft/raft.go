package raft

import (
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/constant"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"if3230-tubes-wreckitraft/util"
	"sync"
	"time"
)

type RaftNode struct {
	raftState
	Config NodeConfiguration

	clusters          []NodeConfiguration
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

func NewRaftNode(address shared.Address, fsm FSM, localID uint64) (*RaftNode, error) {
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

	// hardcode clusters. TODO delete
	clusters := []NodeConfiguration{
		NewNodeConfiguration(
			0, shared.Address{
				IP:   "localhost",
				Port: 5000,
			},
		),
		NewNodeConfiguration(
			1, shared.Address{
				IP:   "localhost",
				Port: 5001,
			},
		),
		NewNodeConfiguration(
			2, shared.Address{
				IP:   "localhost",
				Port: 5002,
			},
		),
	}

	var self NodeConfiguration
	nextIndex := map[shared.Address]uint64{}
	matchIndex := map[shared.Address]uint64{}

	logger.Log.Info("Current ID: ", self.ID)

	for _, cluster := range clusters {
		if localID == cluster.ID {
			logger.Log.Info("masuk euy")
			logger.Log.Info(cluster)
			self = cluster
		} else {
			nextIndex[cluster.Address] = lastLog.Index + 1
			matchIndex[cluster.Address] = 0
		}
	}

	node := RaftNode{
		Config:          self,
		fsm:             fsm,
		logs:            store,
		stable:          store,
		clusters:        clusters,
		electionTimeout: time.Millisecond * 500,
	}
	node.setCurrentTerm(*currentTerm)
	node.setLastLog(lastLog.Index, lastLog.Term)

	node.setNextIndex(nextIndex)
	node.setMatchIndex(matchIndex)

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
				logger.Log.Info("Heartbeat not timed out")
				heartbeatTimer = r.getTimeout(heartbeatTimeout)
				continue
			}
			logger.Log.Warn("Heartbeat timeout")
			// time out occurs
			r.clusterLeader = nil
			r.setState(CANDIDATE)

			return
		}
	}
}

func (r *RaftNode) runCandidate() {
	logger.Log.Info(fmt.Sprintf("Running node: %d as candidate", r.Config.ID))
	votesChannel := r.startElection()
	r.electionTimeout = util.RandomTimeout(
		constant.ELECTION_TIMEOUT_MIN*time.Millisecond, constant.ELECTION_TIMEOUT_MAX*time.Millisecond,
	)
	electionTimer := r.getTimeout(r.electionTimeout)
	majorityThreshold := (len(r.clusters) / 2) + 1
	votesReceived := 0
	for r.getState() == CANDIDATE {
		select {
		case v := <-votesChannel:
			if v.Term > r.getCurrentTerm() {
				r.setState(FOLLOWER)
				r.setCurrentTerm(v.Term)
				return
			}
			if v.Granted {
				votesReceived += 1
				logger.Log.Info(fmt.Sprintf("Received vote from: %d", v.VoterID))
				if votesReceived >= majorityThreshold {
					logger.Log.Info(fmt.Sprintf("%d won the election", r.Config.ID))
					r.setState(LEADER)
					r.setClusterLeader(r.Config)
					return
				}
			}
		case <-electionTimer:
			logger.Log.Warn(fmt.Sprintf("Election timeout for %d", r.Config.ID))
			return
		}
	}
}

func (r *RaftNode) startElection() <-chan *RequestVoteResponse {
	votesChannel := make(chan *RequestVoteResponse, len(r.clusters))
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastLogIndex, lastTerm := r.getLastLog()
	req := RequestVoteArgs{
		Term:         r.getCurrentTerm(),
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastTerm,
	}
	req.CandidateID = r.Config.ID

	requestVoteFromPeer := func(peer NodeConfiguration) {
		r.goFunc(
			func() {
				var resp RequestVoteResponse
				r.sendRequestVote(req, &resp, peer)
				votesChannel <- &resp
			},
		)
	}

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			votesChannel <- &RequestVoteResponse{
				Term:    req.Term,
				Granted: true,
				VoterID: r.Config.ID,
			}
		} else {
			requestVoteFromPeer(peer)
		}
	}

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

	logger.Log.Info(fmt.Sprintf("%s:%d is no longer the leader", r.Config.GetHost(), r.Config.Address.Port))
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
	logger.Log.Info(fmt.Sprintf("Sending heartbeat at: %s", time.Now()))

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			continue
		}

		// Send heartbeat
		logger.Log.Info(fmt.Sprintf("Leader is sending heartbeat to %s:%d", peer.Address.IP, peer.Address.Port))

		go r.appendEntries(peer)
	}
}

func (r *RaftNode) getTimeout(timeout time.Duration) <-chan time.Time {
	return time.After(timeout)
}

func (r *RaftNode) setClusterLeader(config NodeConfiguration) {
	r.clusterLeaderLock.Lock()
	defer r.clusterLeaderLock.Unlock()

	r.clusterLeader = &config
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
	reply.VoterID = r.Config.ID

	if r.currentTerm > args.Term {
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

func (r *RaftNode) appendLog(data []byte) error {
	logs, err := r.logs.GetLogs()
	if err != nil && !errors.Is(err, ErrLogNotFound) {
		return err
	}

	index := uint64(len(logs) + 1)
	term := r.getCurrentTerm()

	newLog := Log{
		Index: index,
		Term:  term,
		Type:  COMMAND,
		Data:  data,
	}

	logs = append(logs, newLog)
	err = r.logs.StoreLogs(logs)
	if err != nil {
		return err
	}

	r.setLastLog(index, term)
	return nil
}

func (r *RaftNode) appendEntries(peer NodeConfiguration) {
	logs, _ := r.logs.GetLogs()

	appendEntry := ReceiveAppendEntriesArgs{
		Term:         r.getCurrentTerm(),
		LeaderConfig: r.Config,
		Entries:      nil,
		LeaderCommit: r.getCommitIndex(),
	}

	nextIndex := r.getNextIndex()
	matchIndex := r.getMatchIndex()

	var resp ReceiveAppendEntriesResponse
	for {
		index, ok := nextIndex[peer.Address]
		prevLogIndex := uint64(0)
		if !ok {
			index = 0
		}
		if index > 0 {
			prevLogIndex = index - 1
		}

		appendEntry.PrevLogIndex = prevLogIndex
		appendEntry.PrevLogTerm = 0
		if prevLogIndex > 0 {
			appendEntry.PrevLogTerm = logs[prevLogIndex-1].Term
		}

		if r.lastLogIndex >= index {
			appendEntry.Entries = logs[index-1:]
		}
		err := r.sendAppendEntries(appendEntry, &resp, peer)

		if err != nil {
			continue
		}

		// TODO: handle resp.term
		if resp.Success {
			logger.Log.Info(fmt.Sprintf("Success send append entries to %d", peer.ID))
			if len(appendEntry.Entries) > 0 {
				nextIndex[peer.Address]++
				r.setNextIndex(nextIndex)
				matchIndex[peer.Address]++
				r.setMatchIndex(matchIndex)
			}
			break
		} else {
			logger.Log.Info(fmt.Sprintf("Failed send append entries to %d", peer.ID))
			nextIndex[peer.Address]--
		}
	}

}

func (r *RaftNode) commitLog(newCommitIndex uint64) {
	currentCommitIdx := r.getCommitIndex()
	if newCommitIndex <= currentCommitIdx {
		return
	}
	logs, _ := r.logs.GetLogs()
	for i := currentCommitIdx; i <= newCommitIndex; i++ {
		if i == 0 {
			continue
		}
		logger.Log.Info(fmt.Sprintf("From node:%d applying log to fsm with index %d", r.Config.ID, i))
		r.fsm.Apply(&logs[i-1])
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
	err := r.appendLog(payload)
	if err != nil {
		return err
	}
	r.replicateLog()
	return nil
}

type LogArgs struct{}

type LogReply struct {
	Log []string
}

func (r *RaftNode) IsLeader() bool {
	r.clusterLeaderLock.RLock()
	defer r.clusterLeaderLock.RUnlock()
	return r.clusterLeader != nil && r.clusterLeader.ID == r.Config.ID
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
