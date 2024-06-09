# if3230-tubes-wreckitraft

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
./server --id=0 --host=localhost --port=5000 -s 1:localhost:5001 -s 2:localhost:5002
./server --id=1 --host=localhost --port=5001 -s 0:localhost:5000 -s 2:localhost:5002
./server --id=2 --host=localhost --port=5002 -s 0:localhost:5000 -s 1:localhost:5001
```
7. Run the server and client in different terminal
```
./client
```

## Team Members
| NIM | Nama |
| --- | ---- |
| 13521044 | Rachel Gabriela Chen | 
| 13521046 | Jeffrey Chow | 
| 13521093 | Akbar Maulana Ridho | 
| 13521094 | Angela Livia Arumsari |
| 13521100 | Alexander Jason |
