# if3230-tubes-wreckitraft

## Available Commands

1. `ping` pong
2. `get <key>`
3. `set <key> <value>`
4. `strln <key>`
5. `del <key>`
6. `append <key> <value>`
7. `request_log`
8. `add_voter <id> <host>:<port>`
9. `add_nonvoter <id> <host>:<port>`
10. `demote_voter <id>`
11. `remove_server <id>`

## How to Run
1. Clone this repository
```
git clone https://github.com/Sister20/if3230-tubes-wreckitraft.git
```

2. Change directory to the repository
```
cd if3230-tubes-wreckitraft
```

3. Install the dependencies
```
go mod tidy
```

4. Build client program
```
go build cmd/client/client.go
```

5. Build server program
```
go build cmd/server/server.go
```

6. Run Multiple server

```
./server --id=0 --host=localhost --port=5000 -s 1:localhost:5001 -s 2:localhost:5002 -s 3:localhost:5003 -s 4:localhost:5004 -s 5:localhost:5005
./server --id=1 --host=localhost --port=5001 -s 0:localhost:5000 -s 2:localhost:5002 -s 3:localhost:5003 -s 4:localhost:5004 -s 5:localhost:5005
./server --id=2 --host=localhost --port=5002 -s 0:localhost:5000 -s 1:localhost:5001 -s 3:localhost:5003 -s 4:localhost:5004 -s 5:localhost:5005
./server --id=3 --host=localhost --port=5003 -s 0:localhost:5000 -s 1:localhost:5001 -s 2:localhost:5002 -s 4:localhost:5004 -s 5:localhost:5005
./server --id=4 --host=localhost --port=5004 -s 0:localhost:5000 -s 1:localhost:5001 -s 2:localhost:5002 -s 3:localhost:5003 -s 5:localhost:5005
./server --id=5 --host=localhost --port=5005 -s 0:localhost:5000 -s 1:localhost:5001 -s 2:localhost:5002 -s 3:localhost:5003 -s 4:localhost:5004
```
7. Run the server and client in different terminal
```
./client
```

## Membership Change

### How to add a new node to cluster

From example above, suppose we want to add node with id `6` and host `localhost:5006`.

Start the server.

```
./server --id=6 --host=localhost --port=5006
```

On client, add the node as nonvoter

```
add_nonvoter 6 localhost:5006
```

After the log is replicated, set the node as voter

```
add_voter 6 localhost:5006
```

### How to remove node from cluster

From example above, suppose we want to remove node with id 3 from cluster

Set the node as nonvoter

```
demote_voter 3
```

After the node is set as nonvoter, remote it from cluster

```
remove_server 3
```

## Team Members
| NIM | Nama |
| --- | ---- |
| 13521044 | Rachel Gabriela Chen | 
| 13521046 | Jeffrey Chow | 
| 13521093 | Akbar Maulana Ridho | 
| 13521094 | Angela Livia Arumsari |
| 13521100 | Alexander Jason |
