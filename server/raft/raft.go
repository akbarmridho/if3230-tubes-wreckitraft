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

	inMemLogs     []Log
	inMemLogsLock sync.RWMutex

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
	state        string
	storage      map[string]string
	storageLock  sync.RWMutex
	commitIndex  uint64
	lastLogIndex uint64
	lastLogTerm  uint64
	currentTerm  uint64
}

func NewRaftNode(address shared.Address, localID uint64) (*RaftNode, error) {
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
	if len(logs) > 1 {
		lastIndex := len(logs) - 1
		lastLog = logs[lastIndex]
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
			nextIndex[cluster.Address] = lastLog.Index
			matchIndex[cluster.Address] = 0
		}
	}

	node := RaftNode{
		Config:          self,
		logs:            store,
		stable:          store,
		clusters:        clusters,
		electionTimeout: time.Millisecond * 500,
	}
	node.setCurrentTerm(*currentTerm)
	node.setLastLog(lastLog.Index, lastLog.Term)

	// Set up heartbeat
	node.setHeartbeatTimeout()

	node.goFunc(node.run)
	return &node, nil
}

func (r *RaftNode) setHeartbeatTimeout() {
	minDuration := constant.HEARTBEAT_INTERVAL * time.Millisecond
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
			r.setHeartbeatTimeout()
			if time.Since(r.getLastContact()) < r.heartbeatTimeout {
				heartbeatTimer = r.getTimeout(r.heartbeatTimeout)
				continue
			}
			logger.Log.Warn(fmt.Sprintf("Timeout from node: %d", r.Config.ID))

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
	// Create a ticker to signal when to send a heartbeat
	heartbeatTicker := time.NewTicker(time.Duration(constant.HEARTBEAT_INTERVAL) * time.Millisecond)
	defer heartbeatTicker.Stop()

	for r.getState() == LEADER {
		select {
		case <-heartbeatTicker.C:
			r.sendHeartbeat()
		default:
			time.Sleep(1 * time.Millisecond)
		}
	}

	logger.Log.Info(fmt.Sprintf("%s:%d is no longer the leader", r.Config.host, r.Config.Address.Port))
}

func (r *RaftNode) sendHeartbeat() {
	logger.Log.Info("Leader is sending heartbeats...")

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			continue
		}

		// Send heartbeat
		logger.Log.Info(fmt.Sprintf("Leader is sending heartbeat to %s:%d", peer.Address.IP, peer.Address.Port))
		go r.appendEntries(peer.Address)
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
	r.stable.Set(keyLastVotedCand, args.CandidateID)
	r.stable.Set(keyLastVoteTerm, args.Term)
	reply.Granted = true
	return nil
}

func (r *RaftNode) getInMemLogs() []Log {
	r.inMemLogsLock.Lock()
	log := r.inMemLogs
	r.inMemLogsLock.Unlock()
	return log
}

func (r *RaftNode) setInMemLogs(logs []Log) {
	r.inMemLogsLock.Lock()
	r.inMemLogs = logs
	r.inMemLogsLock.Unlock()
}

func (r *RaftNode) appendLog(data []byte) bool {
	inMemLogs := r.getInMemLogs()
	index, term := r.getLastLog()
	index += 1
	newLog := Log{
		Index: index,
		Term:  term,
		Type:  COMMAND,
		Data:  data,
	}
	inMemLogs = append(inMemLogs, newLog)
	r.setInMemLogs(inMemLogs)
	r.setLastLog(index, term)
	return true
}

func (r *RaftNode) commitLog() bool {
	logs, err := r.logs.GetLogs()
	if err != nil {
		return false
	}
	index, _ := r.getLastLog()
	newLog := r.inMemLogs[index]
	logs = append(logs, newLog)
	err = r.logs.StoreLogs(logs)
	if err != nil {
		return false
	}
	r.commitIndex = index
	return true
}

func (r *RaftNode) appendEntries(address shared.Address) {
	r.setLastContact()

	index, term := r.getLastLog()
	appendEntry := ReceiveAppendEntriesArgs{
		term:         r.getCurrentTerm(),
		leaderID:     r.clusterLeader.ID,
		prevLogIndex: index,
		prevLogTerm:  term,
		entries:      nil,
		leaderCommit: r.getCommitIndex(),
	}

	nextIndex := r.getNextIndex()
	index, ok := nextIndex[address]

	if !ok {
		index = 0
	}

	if r.lastLogIndex >= index {
		logs, _ := r.logs.GetLogs()
		appendEntry.entries = logs[index:]

		// Send request to follower address here and handle the response
	}
	// Send request for empty entries (heartbeat)

}

func (r *RaftNode) Set(key, value string) error {
	r.storageLock.Lock()
	defer r.storageLock.Unlock()
	r.storage[key] = value
	return nil
}

func (r *RaftNode) Get(key string) (string, error) {
	r.storageLock.RLock()
	defer r.storageLock.RUnlock()
	value, ok := r.storage[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (r *RaftNode) Delete(key string) error {
	r.storageLock.Lock()
	defer r.storageLock.Unlock()
	delete(r.storage, key)
	return nil
}

func (r *RaftNode) Append(key, value string) error {
	r.storageLock.Lock()
	defer r.storageLock.Unlock()
	r.storage[key] += value
	return nil
}

func (r *RaftNode) Strln(key string) (string, error) {
	r.storageLock.RLock()
	defer r.storageLock.RUnlock()
	value, ok := r.storage[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return fmt.Sprintf("%d", len(value)), nil
}

func (r *RaftNode) ApplySet(key, value string) error {
	// Apply the command through Raft consensus
	return r.Set(key, value)
}

func (r *RaftNode) ApplyDel(key string) error {
	// Apply the command through Raft consensus
	return r.Delete(key)
}

func (r *RaftNode) ApplyAppend(key, value string) error {
	// Apply the command through Raft consensus
	return r.Append(key, value)
}

type CommandArgs struct {
	Command string
	Key     string
	Value   string
}

type CommandReply struct {
	Result        string
	LeaderAddress string
}

type LogArgs struct{}

type LogReply struct {
	Log []string
}
