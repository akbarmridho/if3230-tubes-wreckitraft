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

	// channels for
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

	go func() {
		for {
			select {
			case <-heartbeatTicker.C:
				go r.sendHeartbeat()
			default:
				if r.getState() != LEADER {
					logger.Log.Info("%s:%d is no longer the leader", r.Config.Address.IP, r.Config.Address.Port)
					logger.Log.Info(fmt.Sprintf("%s:%d is no longer the leader", r.Config.host, r.Config.Address.Port))
					return
				}
			}
		}
	}()
}

func (r *RaftNode) sendHeartbeat() {
	logger.Log.Info("Leader is sending heartbeats...")

	for _, peer := range r.clusters {
		if peer.ID == r.Config.ID {
			continue
		}

		// Send heartbeat
		logger.Log.Info("Leader is sending heartbeat to %s:%d", peer.Address.IP, peer.Address.Port)
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

//
//// Separate command handling methods
//func (rn *RaftNode) Ping() string {
//	return "PONG"
//}
//
//func (rn *RaftNode) Get(key string) string {
//	rn.mu.Lock()
//	defer rn.mu.Unlock()
//	value, ok := rn.store[key]
//	if !ok {
//		return ""
//	}
//	return value
//}
//
//func (rn *RaftNode) Set(key, value string) string {
//	rn.mu.Lock()
//	defer rn.mu.Unlock()
//	rn.store[key] = value
//	return "OK"
//}
//
//func (rn *RaftNode) Strln(key string) string {
//	rn.mu.Lock()
//	defer rn.mu.Unlock()
//	value, ok := rn.store[key]
//	if !ok {
//		return "0"
//	}
//	return strconv.Itoa(len(value))
//}
//
//func (rn *RaftNode) Del(key string) string {
//	rn.mu.Lock()
//	defer rn.mu.Unlock()
//	value, ok := rn.store[key]
//	if !ok {
//		return ""
//	}
//	delete(rn.store, key)
//	return value
//}
//
//func (rn *RaftNode) Append(key, value string) string {
//	rn.mu.Lock()
//	defer rn.mu.Unlock()
//	_, ok := rn.store[key]
//	if !ok {
//		rn.store[key] = ""
//	}
//	rn.store[key] += value
//	return "OK"
//}
//
//// Execute command on leader
//func (rn *RaftNode) Execute(args *CommandArgs, reply *CommandReply) error {
//	if !rn.IsLeader() {
//		return errors.New("not the leader")
//	}
//	var result string
//	switch args.Command {
//	case "ping":
//		result = rn.Ping()
//	case "get":
//		result = rn.Get(args.Key)
//	case "set":
//		result = rn.Set(args.Key, args.Value)
//	case "strln":
//		result = rn.Strln(args.Key)
//	case "del":
//		result = rn.Del(args.Key)
//	case "append":
//		result = rn.Append(args.Key, args.Value)
//	default:
//		return errors.New("unknown command")
//	}
//	reply.Result = result
//	rn.AppendLog(args.Command + " " + args.Key + " " + args.Value)
//	return nil
//}

type CommandArgs struct {
	Command string
	Key     string
	Value   string
}

type CommandReply struct {
	Result string
}

type LogArgs struct{}

type LogReply struct {
	Log []string
}
