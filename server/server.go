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
	"strconv"
	"strings"
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

func NewServer(ID uint64, address shared.Address, cluster []string) (*Server, error) {
	server := Server{}

	var clusterConfig []raft.NodeConfiguration

	for _, clust := range cluster {
		splitted := strings.Split(clust, ":")

		if len(splitted) != 3 {
			logger.Log.Fatal(fmt.Sprintf("Invalid cluster format %s. Should follow id:host:port", clust))
		}

		id, err := strconv.ParseUint(splitted[0], 10, 64)

		if err != nil {
			logger.Log.Fatal(fmt.Sprintf("Invalid cluster format %s. Should follow id:host:port", clust))
		}

		port, err := strconv.ParseInt(splitted[2], 10, 32)

		if err != nil {
			logger.Log.Fatal(fmt.Sprintf("Invalid cluster format %s. Should follow id:host:port", clust))
		}

		clusterConfig = append(clusterConfig, raft.NodeConfiguration{
			ID: id,
			Address: shared.Address{
				IP:   splitted[1],
				Port: int(port),
			},
		})
	}

	raftNode, err := raft.NewRaftNode(
		address,
		&server,
		ID,
		clusterConfig,
	)

	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to start raft node %s", err.Error()))
		return nil, err
	}

	server.raftNode = raftNode
	server.storage = make(map[string]string)

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
	case "del":
		return s.ApplyDel(c.Key)
	case "append":
		return s.ApplyAppend(c.Key, c.Value)
	default:
		panic(fmt.Sprintf("unrecognized command op: %s", c.Command))
	}
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

	config := s.raftNode.GetConfig()

	address := fmt.Sprintf("%s:%d", config.Address.IP, config.Address.Port)
	logger.Log.Info(fmt.Sprintf("Server is running on %s", address))
	// Network Listener
	listener, err := net.Listen("tcp", address)
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
	logger.Log.Info(fmt.Sprintf("Received command: %s", args.Command))
	reply.LeaderAddress = ""
	if s.raftNode.IsCandidate() {
		reply.Result = "[FAIL] failed to execute command, node is candidate"
		return nil
	}

	if !s.raftNode.IsLeader() {
		reply.LeaderAddress = s.raftNode.GetLeaderAddress()
		reply.Result = "[FAIL] Node is not the leader"
		return nil
	}

	var err error

	switch args.Command {
	case "set":
		b, err := json.Marshal(args)
		if err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to marshal command: %s", err.Error()))
			return err
		}
		err = s.raftNode.Apply(b)
		if err == nil {
			reply.Result = fmt.Sprintf("[OK] Set %s-%s successful", args.Key, args.Value)
		}

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
		err = s.raftNode.Apply(b)
		if err == nil {
			value, _ := s.Get(args.Key)
			reply.Result = value
		}

	case "append":
		b, err := json.Marshal(args)
		if err != nil {
			return err
		}
		err = s.raftNode.Apply(b)
		if err == nil {
			reply.Result = fmt.Sprintf("[OK] Append %s with %s successful", args.Key, args.Value)
		}

	case "strln":
		value, err := s.Strln(args.Key)
		if err != nil {
			return err
		}
		reply.Result = value

	case "ping":
		reply.Result = "pong"

	case "add_voter":
		address, err := shared.StringToAddress(args.Value)

		if err != nil {
			reply.Result = "Cannot parse address"
		} else {
			id, err := strconv.ParseUint(args.Key, 10, 64)
			if err != nil {
				reply.Result = "Cannot parse id"
			} else {
				err := s.raftNode.AddVoter(id, *address)
				if err != nil {
					reply.Result = err.Error()
					return err
				} else {
					reply.Result = "Ok"
				}
			}
		}
	case "add_nonvoter":
		address, err := shared.StringToAddress(args.Value)

		if err != nil {
			reply.Result = "Cannot parse address"
		} else {
			id, err := strconv.ParseUint(args.Key, 10, 64)
			if err != nil {
				reply.Result = "Cannot parse id"
			} else {
				err := s.raftNode.AddNonvoter(id, *address)
				if err != nil {
					reply.Result = err.Error()
					return err
				} else {
					reply.Result = "Ok"
				}
			}
		}
	case "remove_server":
		id, err := strconv.ParseUint(args.Key, 10, 64)
		if err != nil {
			reply.Result = "Cannot parse id"
		} else {
			err := s.raftNode.RemoveServer(id)
			if err != nil {
				reply.Result = err.Error()
				return err
			} else {
				reply.Result = "Ok"
			}
		}
	case "demote_voter":
		id, err := strconv.ParseUint(args.Key, 10, 64)
		if err != nil {
			reply.Result = "Cannot parse id"
		} else {
			err := s.raftNode.DemoteVoter(id)
			if err != nil {
				reply.Result = err.Error()
				return err
			} else {
				reply.Result = "Ok"
			}
		}
	default:
		return errors.New("unknown command")
	}

	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to apply command: %s", err.Error()))
		return err
	}

	return nil
}

func (s *Server) RequestLog(_ *raft.LogArgs, reply *raft.LogReply) error {
	reply.LeaderAddress = ""
	if s.raftNode.IsCandidate() {
		fmt.Println("[FAIL] failed to execute command, node is candidate")
		return nil
	}

	if !s.raftNode.IsLeader() {
		reply.LeaderAddress = s.raftNode.GetLeaderAddress()
		fmt.Println("[FAIL] Node is not the leader")
		return nil
	}
	logs, err := s.raftNode.GetRequestLog()
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Failed to get request log: %s", err.Error()))
		return err
	}
	var c CommandArgs
	for _, log := range logs {
		if err := json.Unmarshal(log.Data, &c); err != nil {
			logger.Log.Error(fmt.Sprintf("Failed to unmarshal command: %s", err.Error()))
			return err
		}
		reply.Log = append(reply.Log, fmt.Sprintf("%s %s %s", c.Command, c.Key, c.Value))
	}
	logger.Log.Info(fmt.Sprintf("Request log: %v", reply.Log))
	return nil
}

func (s *Server) Ping(args struct{}, reply *bool) error {
	*reply = true
	return nil
}
