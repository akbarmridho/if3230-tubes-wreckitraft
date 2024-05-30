package server

import (
	"if3230-tubes-wreckitraft/raft"
	"log"
	"net"
	"net/rpc"
)

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
	err := rpc.Register(s)
	if err != nil {
		return err
	}

	// Register Server Raft Node
	err = rpc.Register(s.raftNode)
	if err != nil {
		return err
	}

	// Network Listener
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		return err
	}

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

func (s *Server) Execute(args *raft.CommandArgs, reply *raft.CommandReply) error {
	log.Printf("Received Execute command: %s %s %s", args.Command, args.Key, args.Value)
	return s.raftNode.Execute(args, reply)
}

func (s *Server) RequestLog(args *raft.LogArgs, reply *raft.LogReply) error {
	log.Println("Received RequestLog command")
	return s.raftNode.RequestLog(args, reply)
}