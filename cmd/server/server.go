package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/server/raft"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"log"
)

var args struct {
	ID   string `arg:"required" help:"node ID"`
	Host string `arg:"required" help:"host of the node"`
	Port int    `arg:"required" help:"port of the node"`
}

func main() {
	// Initialize Raft
	arg.MustParse(&args)

	raftNode, err := raft.NewRaftNode(shared.Address{
		Port: args.Port,
		IP:   args.Host,
	}, args.ID)

	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to start raft node %s", err.Error()))
	}

	// Initialize Server
	srv := server.NewServer(raftNode)
	log.Println("Starting server...")
	// Start Server
	if err := srv.Start(); err != nil {
		log.Fatalf("failed to start server: %v", err)
	}
}
