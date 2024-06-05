package main

import (
	"bufio"
	"fmt"
	"if3230-tubes-wreckitraft/client"
	"log"
	"os"
	"strings"
	"net/http"
	"encoding/json"
	"math/rand"
	"time"
)


func getServers() []string {
	resp, err := http.Get("http://localhost:8080/servers")
	if err != nil {
		log.Fatalf("Failed to get servers: %v", err)
	}
	defer resp.Body.Close()

	var servers []string
	if err := json.NewDecoder(resp.Body).Decode(&servers); err != nil {
		log.Fatalf("Failed to decode servers: %v", err)
	}
	return servers
}

func main() {
	servers := getServers()

	// Seed the random number generator
	rand.Seed(time.Now().UnixNano())

	// Select a random server
	selectedServer := servers[rand.Intn(len(servers))]

	// Connect to the selected server
	cli, err := client.NewClient(selectedServer)
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

		default:
			fmt.Println("Unknown command")
		}
	}
}
