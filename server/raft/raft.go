package raft

import (
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/constant"
	"if3230-tubes-wreckitraft/logger"
	"if3230-tubes-wreckitraft/shared"
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

	// channels for
}

func NewRaftNode(address shared.Address, localID string) (*RaftNode, error) {
	store := Store{
		BaseDir: "data",
	}

	currentTerm, err := store.Get(keyCurrentTerm)
	if err != nil && errors.Is(err, ErrKeyNotFound) {
		return nil, err
	}

	if currentTerm == nil {
		newTerm := uint64(0)
		currentTerm = &newTerm
	}

	logs, err := store.GetLogs()
	if err != nil {
		return nil, err
	}

	var lastLog Log
	if len(logs) > 1 {
		lastIndex := len(logs) - 1
		lastLog = logs[lastIndex]
	}

	// hardcode clusters. TODO delete
	clusters := []NodeConfiguration{
		NewNodeConfiguration("0", shared.Address{
			IP:   "localhost",
			Port: 5000,
		}),
		NewNodeConfiguration("1", shared.Address{
			IP:   "localhost",
			Port: 5001,
		}),
		NewNodeConfiguration("2", shared.Address{
			IP:   "localhost",
			Port: 5002,
		}),
	}

	var self NodeConfiguration

	for _, cluster := range clusters {
		if self.ID == cluster.ID {
			self = cluster
		}
	}

	node := RaftNode{
		Config:   self,
		logs:     store,
		stable:   store,
		clusters: clusters,
	}
	node.setCurrentTerm(*currentTerm)
	node.setLastLog(lastLog.Index, lastLog.Term)

	// Set up heartbeat
	node.setHeartbeatTimeout()

	return &node, nil
}

func (r *RaftNode) setHeartbeatTimeout() {
	minDuration := constant.HEARTBEAT_INTERVAL * time.Millisecond
	maxDuration := 2 * constant.HEARTBEAT_INTERVAL * time.Millisecond

	r.heartbeatTimeout = util.RandomTimeout(minDuration, maxDuration)
}

func (r *RaftNode) run() {
	for {
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
			if time.Since(r.getLastContact()) < r.heartbeatTimeout {
				heartbeatTimer = r.getTimeout(r.heartbeatTimeout)
				continue
			}
			logger.Log.Warn(fmt.Sprintf("Timeout from node: %s", r.Config.ID))

			// time out occurs
			r.clusterLeader = nil
			r.setState(CANDIDATE)

			// Reset the heartbeatTimeout
			r.setHeartbeatTimeout()
			r.electionTimeout = util.RandomTimeout(constant.ELECTION_TIMEOUT_MIN*time.Millisecond, constant.ELECTION_TIMEOUT_MAX*time.Millisecond)

			return
		}
	}
}

func (r *RaftNode) runCandidate() {
	logger.Log.Info(fmt.Sprintf("Running node: %s as candidate", r.Config.ID))
	votesChannel := r.startElection()
	electionTimer := r.getTimeout(r.electionTimeout)
	majorityThreshold := (len(r.clusters) / 2) + 1
	votesReceived := 0
	for r.getState() == CANDIDATE {
		select {
		case v := <-votesChannel:
			if v.term > r.currentTerm {
				logger.Log.Warn(fmt.Sprintf("Encountered higher term during election for %s", r.Config.ID))
				r.setState(FOLLOWER)
				r.setCurrentTerm(v.term)
				return
			}
			if v.granted {
				votesReceived += 1
				if votesReceived >= majorityThreshold {
					logger.Log.Warn(fmt.Sprintf("%s won the election", r.Config.ID))
					r.setState(LEADER)
					r.setClusterLeader(r.Config)
					return
				}
			}
		case <-electionTimer:
			logger.Log.Warn(fmt.Sprintf("Election timeout for %s", r.Config.ID))
			return
		}
	}
}

func (r *RaftNode) startElection() <-chan *RequestVoteResponse {
	votesChannel := make(chan *RequestVoteResponse, len(r.clusters))
	r.setCurrentTerm(r.getCurrentTerm() + 1)

	lastLogIndex, lastTerm := r.getLastLog()
	req := RequestVoteArgs{
		term:         r.getCurrentTerm(),
		lastLogIndex: lastLogIndex,
		lastLogTerm:  lastTerm,
	}
	req.candidate.address = r.Config.Address
	req.candidate.id = r.Config.ID

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
				term:    req.term,
				granted: true,
				voterID: r.Config.ID,
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
		// go r.appendEntries(addr)
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

func (r *RaftNode) ReceiveRequestVote(req RequestVoteArgs) RequestVoteResponse {
	resp := RequestVoteResponse{
		term:    r.getCurrentTerm(),
		granted: false,
		voterID: r.Config.ID,
	}
	if r.currentTerm > req.term {
		return resp
	}

	lastVoted, err := r.stable.Get(keyLastVotedCand)
	if err != nil || lastVoted != nil {
		return resp
	}

	resp.granted = true
	return resp
}

func (r *RaftNode) appendLog(data []byte) bool {
	logs, err := r.logs.GetLogs()
	if err != nil {
		return false
	}
	index, term := r.getLastLog()
	index += 1
	newLog := Log{
		Index: index,
		Term:  term,
		Type:  COMMAND,
		Data:  data,
	}
	logs = append(logs, newLog)
	err = r.logs.StoreLogs(logs)
	if err != nil {
		return false
	}
	r.setLastLog(index, term)
	//still not sure update ini dmn
	//r.commitIndex = r.lastLogIndex
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

	index, ok := r.getNextIndex(address)
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
