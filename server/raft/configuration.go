package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"if3230-tubes-wreckitraft/server/raft/types"
	"if3230-tubes-wreckitraft/shared"
	"if3230-tubes-wreckitraft/shared/logger"
	"net/rpc"
)

var rpcClients = make(map[uint64]*rpc.Client)

type NodeConfiguration struct {
	ID      uint64
	Address shared.Address
	Status  ServerConfigurationStatus
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

func (n *NodeConfiguration) Clone() NodeConfiguration {
	return NodeConfiguration{
		ID: n.ID,
		Address: shared.Address{
			Port: n.Address.Port,
			IP:   n.Address.IP,
		},
		Status: n.Status,
	}
}

func (n *NodeConfiguration) GetRpcClient() (*rpc.Client, error) {
	current, isExist := rpcClients[n.ID]

	if isExist {
		return current, nil
	}

	host := n.GetHost()
	client, err := rpc.DialHTTP("tcp", host)

	if err != nil {
		logger.Log.Warn("Node %s-%s is unreachable, %s", n.ID, host, err.Error())
		return nil, types.NodeNetworkError
	}

	rpcClients[n.ID] = client

	return client, nil
}

type Configuration struct {
	Servers []NodeConfiguration
}

func (c *Configuration) HasServer(ID uint64) bool {
	for _, server := range c.Servers {
		if server.ID == ID {
			return true
		}
	}

	return false
}

func (c *Configuration) HasVote(ID uint64) bool {
	for _, server := range c.Servers {
		if server.ID == ID {
			return server.Status == Voter
		}
	}

	return false
}

func (c *Configuration) VoterCount() int {
	count := 0

	for _, server := range c.Servers {
		if server.Status == Voter {
			count++
		}
	}

	return count
}

func (c *Configuration) Clone() (copy Configuration) {
	for _, config := range c.Servers {
		copy.Servers = append(copy.Servers, config.Clone())
	}
	return
}

func (c *Configuration) CloneWithCommand(change ConfigurationChangeRequest) Configuration {
	result := c.Clone()

	switch change.command {
	case AddVoter:
		newNode := NodeConfiguration{
			Status:  Voter,
			ID:      change.ServerID,
			Address: *change.ServerAddress,
		}

		found := false

		for i, node := range result.Servers {
			if node.ID == change.ServerID {
				result.Servers[i] = newNode
				found = true
				break
			}

		}

		if !found {
			result.Servers = append(result.Servers, newNode)
		}
		break
	case AddNonvoter:
		newNode := NodeConfiguration{
			Status:  Nonvoter,
			ID:      change.ServerID,
			Address: *change.ServerAddress,
		}

		found := false

		for i, node := range result.Servers {
			if node.ID == change.ServerID {
				result.Servers[i] = newNode
				found = true
				break
			}
		}

		if !found {
			result.Servers = append(result.Servers, newNode)
		}
		break
	case DemoteVoter:
		for i, node := range result.Servers {
			if node.ID == change.ServerID {
				result.Servers[i].Status = Nonvoter
				break
			}
		}
		break
	case RemoveServer:
		for i, server := range result.Servers {
			if server.ID == change.ServerID {
				result.Servers = append(result.Servers[:i], result.Servers[i+1:]...)
				break
			}
		}
		break
	}

	return result
}

type ConfigurationChangeRequest struct {
	command       ConfigurationChangeCommand
	ServerID      uint64
	ServerAddress *shared.Address
}

type Configurations struct {
	commited      Configuration
	commitedIndex uint64
	latest        Configuration
	latestIndex   uint64
}

func (c *Configurations) Clone() (copy Configurations) {
	copy.commited = c.commited.Clone()
	copy.latest = c.latest.Clone()
	copy.commitedIndex = c.commitedIndex
	copy.latestIndex = c.latestIndex
	return
}

func EncodeConfiguration(configuration Configuration) ([]byte, error) {
	var indexBuffer bytes.Buffer
	encoder := gob.NewEncoder(&indexBuffer)
	if err := encoder.Encode(configuration); err != nil {
		return nil, err
	}

	return indexBuffer.Bytes(), nil
}

func DecodeConfiguration(data []byte) (*Configuration, error) {
	buffer := bytes.NewBuffer(data)

	decoder := gob.NewDecoder(buffer)

	var result Configuration

	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}
