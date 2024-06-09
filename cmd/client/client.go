package main

import (
	"bufio"
	"fmt"
	"github.com/alexflint/go-arg"
	"if3230-tubes-wreckitraft/client"
	"if3230-tubes-wreckitraft/shared"
	"log"
	"os"
	"strconv"
	"strings"
)

var args struct {
	Host string `default:"localhost:5000" help:"host of the node. Example: localhost:5001"`
}

func main() {
	// Parse arg
	arg.MustParse(&args)

	// List of predefined servers
	servers := append([]string{args.Host}, []string{
		"localhost:5001",
		"localhost:5002",
	}...)

	// Connect to the first available server
	log.Printf("got host %s", args.Host)
	cli, err := client.NewClient(servers)
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Distributed Key-Value Store Client")
	fmt.Println("---------------------")

	for {
		fmt.Print("> ")
		text, _ := reader.ReadString('\n')
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}

		parts := strings.SplitN(text, " ", 3)
		command := parts[0]
		switch command {
		case "ping":
			response := cli.Ping()
			fmt.Println(response)

		case "get":
			if len(parts) < 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			key := parts[1]
			response := cli.Get(key)
			fmt.Printf("\"%s\"\n", response)

		case "set":
			if len(parts) < 3 {
				fmt.Println("Usage: set <key> <value>")
				continue
			}
			key := parts[1]
			value := parts[2]
			response := cli.Set(key, value)
			fmt.Println(response)

		case "strln":
			if len(parts) < 2 {
				fmt.Println("Usage: strln <key>")
				continue
			}
			key := parts[1]
			response := cli.Strln(key)
			fmt.Println(response)

		case "del":
			if len(parts) < 2 {
				fmt.Println("Usage: del <key>")
				continue
			}
			key := parts[1]
			response := cli.Del(key)
			fmt.Printf("\"%s\"\n", response)

		case "append":
			if len(parts) < 3 {
				fmt.Println("Usage: append <key> <value>")
				continue
			}
			key := parts[1]
			value := parts[2]
			response := cli.Append(key, value)
			fmt.Println(response)

		case "request_log":
			logEntries := cli.RequestLog()
			for _, entry := range logEntries {
				fmt.Println(entry)
			}

		case "add_voter":
			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid server id. Should be an unsigned integer")
				continue
			}

			parsedHost := strings.Split(parts[2], ":")

			if len(parsedHost) != 2 {
				fmt.Println("Invalid host format. Example: localhost:5000")
				continue
			}

			port, err := strconv.ParseInt(parsedHost[1], 10, 32)

			if err != nil {
				fmt.Println("Invalid port value")
			}

			fmt.Println(cli.AddVoter(id, shared.Address{IP: parsedHost[0], Port: int(port)}))

		case "add_nonvoter":
			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid server id. Should be an unsigned integer")
				continue
			}

			parsedHost := strings.Split(parts[2], ":")

			if len(parsedHost) != 2 {
				fmt.Println("Invalid host format. Example: localhost:5000")
				continue
			}

			port, err := strconv.ParseInt(parsedHost[1], 10, 32)

			if err != nil {
				fmt.Println("Invalid port value")
			}

			fmt.Println(cli.AddNonvoter(id, shared.Address{IP: parsedHost[0], Port: int(port)}))

		case "remove_server":
			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid server id. Should be an unsigned integer")
				continue
			}

			fmt.Println(cli.RemoveServer(id))

		case "demote_voter":
			id, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				fmt.Println("Invalid server id. Should be an unsigned integer")
				continue
			}

			fmt.Println(cli.RemoveServer(id))

		default:
			fmt.Println("Unknown command")
		}
	}
}
