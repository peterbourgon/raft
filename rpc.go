package raft

type appendEntriesTuple struct {
	Request  AppendEntries
	Response chan AppendEntriesResponse
}

type requestVoteTuple struct {
	Request  RequestVote
	Response chan RequestVoteResponse
}

type AppendEntries struct {
	Term         uint64     `json:"term"`
	LeaderId     uint64     `json:"leader_id"`
	PrevLogIndex uint64     `json:"prev_log_index"`
	PrevLogTerm  uint64     `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	CommitIndex  uint64     `json:"commit_index"`
}

type AppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
	reason  string
}

type RequestVote struct {
	Term         uint64 `json:"term"`
	CandidateId  uint64 `json:"candidate_id"`
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	reason      string
}
