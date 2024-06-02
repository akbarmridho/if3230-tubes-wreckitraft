package raft

type NodeType uint32

const (
	FOLLOWER  NodeType = 0 // FOLLOWER SHOULD BE 0 for default value
	LEADER    NodeType = 1
	CANDIDATE NodeType = 2
)

func (n NodeType) getName() string {
	switch n {
	case LEADER:
		return "LEADER"
	case CANDIDATE:
		return "CANDIDATE"
	case FOLLOWER:
		return "FOLLOWER"
	default:
		return "UNKNOWN"
	}
}
