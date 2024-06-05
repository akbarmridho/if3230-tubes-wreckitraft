package main

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

var (
	mu      sync.Mutex
	servers = make(map[string]bool)
)

func registerServer(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	var server struct {
		Address string `json:"address"`
	}
	if err := json.NewDecoder(r.Body).Decode(&server); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	servers[server.Address] = true
	log.Printf("Server registered: %s", server.Address)
	w.WriteHeader(http.StatusOK)
}

func getServers(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	var serverList []string
	for server := range servers {
		serverList = append(serverList, server)
	}
	json.NewEncoder(w).Encode(serverList)
}

func main() {
	http.HandleFunc("/register", registerServer)
	http.HandleFunc("/servers", getServers)
	log.Println("Starting registry service on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
