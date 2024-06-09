package main

import (
	"fmt"
	"github.com/alexflint/go-arg"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"os"
)

var args struct {
	ID       uint64   `arg:"required" help:"node ID"`
	Host     string   `arg:"required" help:"host of the node"`
	Port     int      `arg:"required" help:"port of the node"`
	Clusters []string `arg:"-s,separate" help:"id:host:port"`
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	// Initialize Raft
	arg.MustParse(&args)

	// Initialize Server
	srv, err := server.NewServer(args.ID, shared.Address{
		IP:   args.Host,
		Port: args.Port,
	}, args.Clusters)

	if err != nil {
		logger.Log.Fatal(fmt.Sprintf("Failed to create server %s", err.Error()))
	}

	logger.Log.Info("Starting server")
	// Start Server
	if err := srv.Start(); err != nil {
		logger.Log.Fatal("failed to start server: %v", err)
	}
	select {}
}
