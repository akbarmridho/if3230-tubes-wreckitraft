package raft

import (
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/logger"
	"if3230-tubes-wreckitraft/shared"
	"sync"
	"time"
)

type RaftNode struct {
	raftState
	Address shared.Address
	LocalID string

	clusters         []shared.Address
	clusterLeader    *shared.Address
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

	node := RaftNode{
		LocalID: localID,
		Address: address,
		logs:    store,
		stable:  store,
	}
	node.setCurrentTerm(*currentTerm)
	node.setLastLog(lastLog.Index, lastLog.Term)

	// set up heartbeat here
	return &node, nil
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
	heartbeatTimer := r.getTimeout()
	for r.getState() == FOLLOWER {
		select {
		case <-heartbeatTimer:
			// not timed out
			if time.Since(r.getLastContact()) < r.heartbeatTimeout {
				heartbeatTimer = r.getTimeout()
				continue
			}
			logger.Log.Warn(fmt.Sprintf("Timeout from node: %s", r.LocalID))
			// time out occurs
			r.clusterLeader = nil
			r.setState(CANDIDATE)
			return
		}
	}
}

func (r *RaftNode) runCandidate() {
	logger.Log.Info(fmt.Sprintf("Running node: %s as candidate", r.LocalID))
	votesChannel := r.startElection()
	select {
	case <-votesChannel:

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
	req.candidate.address = r.Address
	req.candidate.id = r.LocalID

	requestVoteFromPeer := func(peer shared.Address) {
		r.goFunc(
			func() {
				var resp RequestVoteResponse
				r.sendRequestVote(req, &resp, peer)
				votesChannel <- &resp
			},
		)
	}

	for _, peer := range r.clusters {
		if peer.IP == r.Address.IP && peer.Port == r.Address.Port {
			votesChannel <- &RequestVoteResponse{
				term:    req.term,
				granted: true,
				voterID: r.LocalID,
			}
		} else {
			requestVoteFromPeer(peer)
		}
	}

	return votesChannel
}

func (r *RaftNode) runLeader() {

}

func (r *RaftNode) getTimeout() <-chan time.Time {
	return time.After(r.heartbeatTimeout)
}

func (r *RaftNode) getLastContact() time.Time {
	r.lastContactLock.RLock()
	lastContact := r.lastContact
	r.lastContactLock.RUnlock()
	return lastContact
}

func (r *RaftNode) ReceiveRequestVote(req RequestVoteArgs) RequestVoteResponse {
	resp := RequestVoteResponse{
		term:    r.getCurrentTerm(),
		granted: false,
		voterID: r.LocalID,
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
