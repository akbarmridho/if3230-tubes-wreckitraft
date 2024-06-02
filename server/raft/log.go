package raft

type LogType string

const (
	COMMAND LogType = "COMMAND" // COMMAND is client's operations
	NOOP    LogType = "NOOP"    // NOOP is when a node is promoted as leader
)

type Log struct {
	Index uint64
	Term  uint64
	Type  LogType
	Data  []byte
}

type LogStore interface {
	GetLogs() ([]Log, error)
	StoreLogs(logs []Log) error
}
