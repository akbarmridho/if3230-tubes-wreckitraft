package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
	"time"
)

func (r *RaftNode) sendAppendEntries(
	req ReceiveAppendEntriesArgs, resp *ReceiveAppendEntriesResponse, peer NodeConfiguration,
) error {
	logger.Log.Info(fmt.Sprintf("Sending append entries to: %d at %s", peer.ID, time.Now()))
	err := peer.getRpcClient()

	if err != nil {
		logger.Log.Warn(err)
		return err
	}

	err = peer.rpcClient.Call("RaftNode.ReceiveAppendEntries", &req, resp)

	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send append entries to: %d failed", peer.ID))
		return err
	}

	return nil
}
