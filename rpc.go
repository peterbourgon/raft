package raft

type appendEntriesTuple struct {
	Request  AppendEntries
	Response chan AppendEntriesResponse
}

type requestVoteTuple struct {
	Request  RequestVote
	Response chan RequestVoteResponse
}

// AppendEntries represents an AppendEntries RPC.
type AppendEntries struct {
	Term         uint64     `json:"term"`
	LeaderID     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []logEntry `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

// AppendEntriesResponse represents the response to an AppendEntries RPC.
type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

// RequestVote represents a RequestVote RPC.
type RequestVote struct {
	Term         uint64 `json:"term"`
	CandidateID  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse represents the response to a RequestVote RPC.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}
