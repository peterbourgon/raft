package raft

type appendEntriesTuple struct {
	Request  appendEntries
	Response chan appendEntriesResponse
}

type requestVoteTuple struct {
	Request  requestVote
	Response chan requestVoteResponse
}

// appendEntries represents an appendEntries RPC.
type appendEntries struct {
	Term         uint64     `json:"term"`
	LeaderID     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []logEntry `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

// appendEntriesResponse represents the response to an appendEntries RPC.
type appendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

// requestVote represents a requestVote RPC.
type requestVote struct {
	Term         uint64 `json:"term"`
	CandidateID  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// requestVoteResponse represents the response to a requestVote RPC.
type requestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}
