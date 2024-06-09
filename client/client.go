package client

import (
	"fmt"
	"if3230-tubes-wreckitraft/server"
	"if3230-tubes-wreckitraft/server/raft"
	"if3230-tubes-wreckitraft/shared"
	"log"
	"net/rpc"
	"strconv"
)

type Client struct {
	serverAddress string
	rpcClient     *rpc.Client
	servers       []string
}

// NewClient
func NewClient(servers []string) (*Client, error) {
	var lastErr error
	for _, server := range servers {
		log.Printf("Trying to connect to server %s", server)
		client, err := rpc.DialHTTP("tcp", server)
		if err == nil {
			log.Printf("[OK] Connected to server %s", server)
			return &Client{
				serverAddress: server,
				rpcClient:     client,
				servers:       servers,
			}, nil
		}
		lastErr = err
	}
	return nil, fmt.Errorf("fail to connect to any server: %v", lastErr)
}

func (c *Client) ChangeClient(port string) error {
	newAddress := "localhost:" + port
	newClient, err := rpc.DialHTTP("tcp", newAddress)
	if err != nil {
		return err
	}
	c.serverAddress = newAddress
	c.rpcClient = newClient
	log.Printf("Successfully changed server to %s", newAddress)
	return nil
}

func (c *Client) HealthCheck() error {
	var reply bool
	err := c.rpcClient.Call("Server.Ping", struct{}{}, &reply)
	if err != nil || !reply {
		return fmt.Errorf("health check failed: %v", err)
	}
	return nil
}

func (c *Client) Reconnect() error {
	var lastErr error
	for _, server := range c.servers {
		if server == c.serverAddress {
			continue
		}
		log.Printf("Trying to connect to server %s", server)
		client, err := rpc.DialHTTP("tcp", server)
		if err == nil {
			log.Printf("[OK] Connected to server %s", server)
			c.serverAddress = server
			c.rpcClient = client
			return nil
		}
		lastErr = err
	}
	return fmt.Errorf("fail to reconnect to any server: %v", lastErr)
}

func (c *Client) Execute(command, key, value string) string {
	args := &server.CommandArgs{Command: command, Key: key, Value: value}
	reply := c.executeCommand(args, 0)
	return reply.Result
}

func (c *Client) executeCommand(args *server.CommandArgs, redirects int) server.CommandReply {
	const maxRedirects = 4
	var reply server.CommandReply
	var err error

	if redirects >= maxRedirects {
		log.Fatalf("Exceeded maximum number of redirects: %d", maxRedirects)
	}

	if err := c.HealthCheck(); err != nil {
		log.Printf("Health check failed for server %s: %v", c.serverAddress, err)
		if err := c.Reconnect(); err != nil {
			log.Printf("Failed to reconnect: %v", err)
		}
	}

	_ = c.rpcClient.Call("Server.Execute", args, &reply)

	if reply.LeaderAddress != "" {
		fmt.Println(reply.Result)
		log.Printf("Redirecting to leader at %s", reply.LeaderAddress)
		c.serverAddress = reply.LeaderAddress
		c.rpcClient, err = rpc.DialHTTP("tcp", c.serverAddress)
		if err != nil {
			log.Printf("Failed to connect to new leader: %v", err)
		}
		return c.executeCommand(args, redirects+1)
	}

	return reply
}


// RequestLog retrieves the log entries from the server
func (c *Client) RequestLog() []string {
	var args raft.LogArgs
	var reply raft.LogReply
	if err := c.HealthCheck(); err != nil {
		log.Printf("Health check failed for server %s: %v", c.serverAddress, err)
		if err := c.Reconnect(); err != nil {
			log.Fatalf("Failed to reconnect: %v", err)
		}
	}

	err := c.rpcClient.Call("Server.RequestLog", &args, &reply)
	if err != nil {
		log.Printf("RequestLog error: %v", err)
	}
	if reply.LeaderAddress != "" {
		log.Printf("Redirecting to leader at %s", reply.LeaderAddress)
		c.serverAddress = reply.LeaderAddress
		c.rpcClient, err = rpc.DialHTTP("tcp", c.serverAddress)
		if err != nil {
			log.Fatalf("Failed to connect to new leader: %v", err)
		}
		err = c.rpcClient.Call("Server.RequestLog", args, &reply)
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

func (c *Client) AddVoter(id uint64, address shared.Address) string {
	return c.Execute("add_voter", strconv.FormatUint(id, 10), address.Host())
}

func (c *Client) RemoveServer(id uint64) string {
	return c.Execute("remove_server", strconv.FormatUint(id, 10), "")
}

func (c *Client) AddNonvoter(id uint64, address shared.Address) string {
	return c.Execute("add_nonvoter", strconv.FormatUint(id, 10), address.Host())
}

func (c *Client) DemoteVoter(id uint64) string {
	return c.Execute("demote_voter", strconv.FormatUint(id, 10), "")
}
