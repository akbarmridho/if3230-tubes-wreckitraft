package raft

type ReceiveAppendEntriesArgs struct {
	term         uint64
	leaderID     string
	prevLogIndex uint64
	prevLogTerm  uint64
	entries      []Log
	leaderCommit uint64
}

type ReceiveAppendEntriesResponse struct {
	term    uint64
	success bool
}

// ReceiveAppendEntries Receive
func (r *RaftNode) ReceiveAppendEntries(args *ReceiveAppendEntriesArgs, reply *ReceiveAppendEntriesResponse) error {

	response := ReceiveAppendEntriesResponse{}
	reply = &response

	return nil
}
