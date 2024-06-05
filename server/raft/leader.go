package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/shared/logger"
)

func (r *RaftNode) sendAppendEntries(req ReceiveAppendEntriesArgs, resp *ReceiveAppendEntriesResponse, peer NodeConfiguration) {
	logger.Log.Info("Sending append entries to: ", peer.ID)
	err := peer.getRpcClient()

	if err != nil {
		logger.Log.Warn(err)
		return
	}

	err = peer.rpcClient.Call("RaftNode.ReceiveAppendEntries", &req, resp)

	if err != nil {
		logger.Log.Warn(fmt.Sprintf("Send append entries to: %d failed", peer.ID))
		return
	}
	logger.Log.Info("Append entries response: ", *resp)
}
