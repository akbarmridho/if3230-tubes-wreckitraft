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
	address          shared.Address
	clusters         []shared.Address
	clusterLeader    *shared.Address
	logs             LogStore
	stable           StableStore
	localID          string
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
		localID: localID,
		address: address,
		logs:    store,
		stable:  store,
	}
	node.setCurrentTerm(currentTerm)
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

	for r.getState() == FOLLOWER {
		select {
		case <-time.After(r.heartbeatTimeout):
			// not timed out
			if time.Since(r.getLastContact()) < r.heartbeatTimeout {
				continue
			}
			logger.Log.Warn(fmt.Sprintf("Timeout from node: %s", r.localID))
			// time out occurs
			r.clusterLeader = nil
			r.setState(CANDIDATE)
			return
		}
	}
}

func (r *RaftNode) runCandidate() {

}

func (r *RaftNode) runLeader() {

}

func (r *RaftNode) getLastContact() time.Time {
	r.lastContactLock.RLock()
	lastContact := r.lastContact
	r.lastContactLock.RUnlock()
	return lastContact
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
