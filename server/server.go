package server

import (
	"fmt"
	"if3230-tubes-wreckitraft/server/raft"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"errors"
)
type command struct {
    Op    string `json:"op,omitempty"`
    Key   string `json:"key,omitempty"`
    Value string `json:"value,omitempty"`
}
type Server struct {
	raftNode *raft.RaftNode
}

func NewServer(node *raft.RaftNode) *Server {
	return &Server{
		raftNode: node,
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
	port := fmt.Sprintf(":%d", s.raftNode.Config.Address.Port)
	// Network Listener
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}

	go http.Serve(listener, nil)

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatalf("listener close error: %v", err)
			return
		}
	}(listener)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("connection accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func (s *Server) Apply(args *raft.CommandArgs, reply *raft.CommandReply) error {
	//TODO Check Leader
	switch args.Command {
	case "set":
		return s.raftNode.ApplySet(args.Key, args.Value)
	case "get":
		value, err := s.raftNode.Get(args.Key)
		if err != nil {
			return err
		}
		reply.Result = value
	case "del":
		return s.raftNode.ApplyDel(args.Key)
	case "append":
		return s.raftNode.ApplyAppend(args.Key, args.Value)
	case "strln":
		value, err := s.raftNode.Strln(args.Key)
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
//func (s *Server) Execute(args *raft.CommandArgs, reply *raft.CommandReply) error {
//	log.Printf("Received Execute command: %s %s %s", args.Command, args.Key, args.Value)
//	return s.raftNode.Execute(args, reply)
//}
//
//func (s *Server) RequestLog(args *raft.LogArgs, reply *raft.LogReply) error {
//	log.Println("Received RequestLog command")
//	return s.raftNode.RequestLog(args, reply)
//}
