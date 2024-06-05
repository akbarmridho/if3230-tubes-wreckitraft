package raft

type FSM interface {
	// Apply is called once a log entry is commited by a majority of the cluster
	// Apply should apply the log to the FSM. Apply must be deterministic and
	// produce the same result on all peers in the cluster
	Apply(log *Log) interface{}
}
