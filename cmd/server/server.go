package main

import (
	"github.com/alexflint/go-arg"
	"if3230-tubes-wreckitraft/logger"
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/server/raft"
	"if3230-tubes-wreckitraft/shared"
	"log"
)

var args struct {
	id   string `arg:"required" help:"node id"`
	host string `arg:"required" help:"host of the node"`
	port int    `arg:"required" help:"port of the node"`
}

func main() {
	// Initialize Raft
	arg.MustParse(&args)

	raftNode, err := raft.NewRaftNode(shared.Address{
		Port: args.port,
		IP:   args.host,
	}, args.id)

	if err != nil {
		logger.Log.Fatal("Failed to start raft node %s", err.Error())
	}

	// Initialize Server
	srv := server.NewServer(raftNode)
	log.Println("Starting server...")
	// Start Server
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
