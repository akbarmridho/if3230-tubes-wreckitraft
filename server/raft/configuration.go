package raft

import (
	"if3230-tubes-wreckitraft/server/raft/types"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"net/rpc"
)

type NodeConfiguration struct {
	ID        string
	host      string
	Address   shared.Address
	rpcClient *rpc.Client
}

func NewNodeConfiguration(id string, address shared.Address) NodeConfiguration {
	var host string

	if address.IsHTTPS {
		host = "https://" + address.IP + ":" + string(rune(address.Port))
	} else {
		host = "http://" + address.IP + ":" + string(rune(address.Port))
	}

	return NodeConfiguration{
		ID:      id,
		host:    host,
		Address: address,
	}
}

func (n *NodeConfiguration) getRpcClient() error {

	client, err := rpc.DialHTTP("tcp", n.host)

	if err != nil {
		logger.Log.Warn("Node %s-%s is unreachable, %s", n.ID, n.host, err.Error())
		return types.NodeNetworkError
	}

	n.rpcClient = client

	return nil
}

func (n *NodeConfiguration) CallRPC(method string, args interface{}) (interface{}, error) {
	err := n.getRpcClient()

	if err != nil {
		return nil, err
	}

	var response interface{}

	err = n.rpcClient.Call(method, args, &response)

	if err != nil {
		logger.Log.Warn("RPC call %s on node %s-%s fail %s", method, n.ID, n.host, err.Error())
		return nil, types.NodeNetworkError
	}

	return response, nil
}
