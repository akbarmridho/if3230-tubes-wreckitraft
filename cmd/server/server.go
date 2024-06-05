package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/server/raft"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"os"
	"bytes"
	"encoding/json"
	"net/http"
)

var args struct {
	ID   uint64 `arg:"required" help:"node ID"`
	Host string `arg:"required" help:"host of the node"`
	Port int    `arg:"required" help:"port of the node"`
}

func registerServer(address string) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	server := struct {
		Address string `json:"address"`
	}{
		Address: address,
	}
	jsonValue, _ := json.Marshal(server)
	resp, err := http.Post("http://localhost:8080/register", "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to register with registry: %v", err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		logger.Log.Fatal(fmt.Sprintf("Failed to register with registry: %s", resp.Status))
	}
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	// Initialize Raft
	arg.MustParse(&args)

	raftNode, err := raft.NewRaftNode(
		shared.Address{
			Port: args.Port,
			IP:   args.Host,
		}, args.ID,
	)

	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to start raft node %s", err.Error()))
	}

	// Register the server with the registry
	serverAddress := fmt.Sprintf("%s:%d", args.Host, args.Port)
	registerServer(serverAddress)

	// Initialize Server
	srv := server.NewServer(raftNode)
	logger.Log.Info("Starting server")
	// Start Server
	if err := srv.Start(); err != nil {
		logger.Log.Fatal("failed to start server: %v", err)
	}
}
