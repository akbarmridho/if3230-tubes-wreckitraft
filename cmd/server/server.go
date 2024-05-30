package main

import (
	"if3230-tubes-wreckitraft/raft"
	"if3230-tubes-wreckitraft/server"
	"log"
)

func main() {
	// Initialize Raft
	raftNode := raft.NewRaftNode()
	raftNode.SetLeader(true)

	// Initialize Server
	srv := server.NewServer(raftNode)
	log.Println("Starting server...")
	// Start Server
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
