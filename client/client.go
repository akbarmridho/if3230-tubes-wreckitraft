package client

import (
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/server/raft"
	"log"
	"net/rpc"
)

type Client struct {
	serverAddress string
	rpcClient     *rpc.Client
}

func NewClient(address string) (*Client, error) {
	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &Client{
		serverAddress: address,
		rpcClient:     client,
	}, nil
}

func (c *Client) Execute(command, key, value string) string {
	args := &server.CommandArgs{Command: command, Key: key, Value: value}
	var reply server.CommandReply
	err := c.rpcClient.Call("Server.Apply", args, &reply)
	if err != nil {
		// if reply.LeaderAddress != "" {
		log.Printf("Redirecting to leader at %s", reply.LeaderAddress)
		c.serverAddress = reply.LeaderAddress
		c.rpcClient, err = rpc.Dial("tcp", c.serverAddress)
		if err != nil {
			log.Fatalf("Failed to connect to new leader: %v", err)
		}
		err = c.rpcClient.Call("Server.Execute", args, &reply)
		if err != nil {
			log.Fatalf("Execute error after redirection: %v", err)
		}
		// } else {
		//     log.Fatalf("Execute error: %v", err)
		// }
	}
	return reply.Result
}

// TODO: Request Log
func (c *Client) RequestLog() []string {
	var args raft.LogArgs
	var reply raft.LogReply
	err := c.rpcClient.Call("Server.RequestLog", &args, &reply)
	if err != nil {
		log.Fatalf("RequestLog error: %v", err)
	}
	return reply.Log
}

func (c *Client) Ping() string {
	return c.Execute("ping", "", "")
}

func (c *Client) Get(key string) string {
	return c.Execute("get", key, "")
}

func (c *Client) Set(key, value string) string {
	return c.Execute("set", key, value)
}

func (c *Client) Strln(key string) string {
	return c.Execute("strln", key, "")
}

func (c *Client) Del(key string) string {
	return c.Execute("del", key, "")
}

func (c *Client) Append(key, value string) string {
	return c.Execute("append", key, value)
}
