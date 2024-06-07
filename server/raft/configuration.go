package raft

import (
	"fmt"
	"if3230-tubes-wreckitraft/server/raft/types"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"net/rpc"
)

type NodeConfiguration struct {
	ID        uint64
	Address   shared.Address
	rpcClient *rpc.Client
}

func NewNodeConfiguration(id uint64, address shared.Address) NodeConfiguration {
	return NodeConfiguration{
		ID:      id,
		Address: address,
	}
}

func (n *NodeConfiguration) GetHost() string {
	return fmt.Sprintf("%s:%d", n.Address.IP, n.Address.Port)
}

func (n *NodeConfiguration) GetRpcClient() (*rpc.Client, error) {
	if n.rpcClient != nil {
		return n.rpcClient, nil
	}

	host := n.GetHost()
	client, err := rpc.DialHTTP("tcp", host)

	if err != nil {
		logger.Log.Warn("Node %s-%s is unreachable, %s", n.ID, host, err.Error())
		return nil, types.NodeNetworkError
	}

	n.rpcClient = client

	return n.rpcClient, nil
}
