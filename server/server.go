package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"if3230-tubes-wreckitraft/server/raft"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"net"
	"net/http"
	"net/rpc"
	"sync"
)

type CommandArgs struct {
	Command string
	Key     string
	Value   string
}

type CommandReply struct {
	Result        string
	LeaderAddress string
}

type Server struct {
	raftNode    *raft.RaftNode
	storage     map[string]string
	storageLock sync.RWMutex
}

func NewServer(ID uint64, address shared.Address) (*Server, error) {
	server := Server{}

	raftNode, err := raft.NewRaftNode(
		address,
		&server,
		ID,
	)

	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to start raft node %s", err.Error()))
		return nil, err
	}

	server.raftNode = raftNode

	return &server, nil
}

func (s *Server) Apply(log *raft.Log) interface{} {
	var c CommandArgs
	if err := json.Unmarshal(log.Data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal command: %s", err.Error()))
	}

	switch c.Command {
	case "set":
		return s.ApplySet(c.Key, c.Value)
	case "delete":
		return s.ApplyDel(c.Key)
	case "append":
		return s.ApplyAppend(c.Key, c.Value)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Command))
	}
	return nil
}

func (s *Server) Start() error {
	// Register Server
	err1 := rpc.Register(s)
	if err1 != nil {
		return err1
	}

	// Register Server Raft Node
	err := rpc.Register(s.raftNode)
	if err != nil {
		return err
	}

	rpc.HandleHTTP()
	port := fmt.Sprintf(":%d", s.raftNode.Config.Address.Port)
	// Network Listener
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	go http.Serve(listener, nil)

	return nil
}

func (s *Server) Set(key, value string) error {
	s.storageLock.Lock()
	defer s.storageLock.Unlock()
	s.storage[key] = value
	return nil
}

func (s *Server) Get(key string) (string, error) {
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()
	value, ok := s.storage[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return value, nil
}

func (s *Server) Delete(key string) error {
	s.storageLock.Lock()
	defer s.storageLock.Unlock()
	delete(s.storage, key)
	return nil
}

func (s *Server) Append(key, value string) error {
	s.storageLock.Lock()
	defer s.storageLock.Unlock()
	s.storage[key] += value
	return nil
}

func (s *Server) Strln(key string) (string, error) {
	s.storageLock.RLock()
	defer s.storageLock.RUnlock()
	value, ok := s.storage[key]
	if !ok {
		return "", errors.New("key not found")
	}
	return fmt.Sprintf("%d", len(value)), nil
}

func (s *Server) ApplySet(key, value string) error {
	// Apply the command through Raft consensus
	return s.Set(key, value)
}

func (s *Server) ApplyDel(key string) error {
	// Apply the command through Raft consensus
	return s.Delete(key)
}

func (s *Server) ApplyAppend(key, value string) error {
	// Apply the command through Raft consensus
	return s.Append(key, value)
}

func (s *Server) Execute(args *CommandArgs, reply *CommandReply) error {
	//TODO Check Leader
	switch args.Command {
	case "set":
		b, err := json.Marshal(args)

		if err != nil {
			return err
		}
		return s.raftNode.Apply(b)
	case "get":
		value, err := s.Get(args.Key)
		if err != nil {
			return err
		}
		reply.Result = value
	case "del":
		b, err := json.Marshal(args)

		if err != nil {
			return err
		}
		return s.raftNode.Apply(b)
	case "append":
		b, err := json.Marshal(args)

		if err != nil {
			return err
		}
		return s.raftNode.Apply(b)
	case "strln":
		value, err := s.Strln(args.Key)
		if err != nil {
			return err
		}
		reply.Result = value
	case "ping":
		reply.Result = "pong"
	default:
		return errors.New("unknown command")
	}
	return nil
}
