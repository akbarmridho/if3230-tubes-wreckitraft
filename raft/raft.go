package raft

import (
	"errors"
	"strconv"
	"sync"
)

type RaftNode struct {
	mu         sync.Mutex
	isLeader   bool
	logEntries []string
	store      map[string]string
}

func NewRaftNode() *RaftNode {
	return &RaftNode{
		logEntries: make([]string, 0),
		store:      make(map[string]string),
	}
}

func (rn *RaftNode) IsLeader() bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.isLeader
}

func (rn *RaftNode) SetLeader(isLeader bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.isLeader = isLeader
}

func (rn *RaftNode) AppendLog(entry string) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.logEntries = append(rn.logEntries, entry)
}

func (rn *RaftNode) GetLog() []string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	return rn.logEntries
}

// Separate command handling methods
func (rn *RaftNode) Ping() string {
	return "PONG"
}

func (rn *RaftNode) Get(key string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	value, ok := rn.store[key]
	if !ok {
		return ""
	}
	return value
}

func (rn *RaftNode) Set(key, value string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.store[key] = value
	return "OK"
}

func (rn *RaftNode) Strln(key string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	value, ok := rn.store[key]
	if !ok {
		return "0"
	}
	return strconv.Itoa(len(value))
}

func (rn *RaftNode) Del(key string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	value, ok := rn.store[key]
	if !ok {
		return ""
	}
	delete(rn.store, key)
	return value
}

func (rn *RaftNode) Append(key, value string) string {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	_, ok := rn.store[key]
	if !ok {
		rn.store[key] = ""
	}
	rn.store[key] += value
	return "OK"
}

// Execute command on leader
func (rn *RaftNode) Execute(args *CommandArgs, reply *CommandReply) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}
	var result string
	switch args.Command {
	case "ping":
		result = rn.Ping()
	case "get":
		result = rn.Get(args.Key)
	case "set":
		result = rn.Set(args.Key, args.Value)
	case "strln":
		result = rn.Strln(args.Key)
	case "del":
		result = rn.Del(args.Key)
	case "append":
		result = rn.Append(args.Key, args.Value)
	default:
		return errors.New("unknown command")
	}
	reply.Result = result
	rn.AppendLog(args.Command + " " + args.Key + " " + args.Value)
	return nil
}

// RequestLog Return log held by the leader
func (rn *RaftNode) RequestLog(args *LogArgs, reply *LogReply) error {
	if !rn.IsLeader() {
		return errors.New("not the leader")
	}
	reply.Log = rn.GetLog()
	return nil
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
