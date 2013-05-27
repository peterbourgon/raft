package raft

import (
	"bytes"
	"testing"
)

func TestFollowerAllegiance(t *testing.T) {
	// a follower with allegiance to leader=2
	s := Server{
		id:     1,
		term:   5,
		state:  &serverState{value: Follower},
		leader: 2,
		log:    NewLog(&bytes.Buffer{}, noop),
	}

	// receives an AppendEntries from a future term and different leader
	_, stepDown := s.handleAppendEntries(AppendEntries{
		Term:     6,
		LeaderId: 3,
	})

	// should now step down and have a new term
	if !stepDown {
		t.Errorf("wasn't told to step down (i.e. abandon leader)")
	}
	if s.term != 6 {
		t.Errorf("no term change")
	}
}

func TestStrongLeader(t *testing.T) {
	// a leader in term=2
	s := Server{
		id:     1,
		term:   2,
		state:  &serverState{value: Leader},
		leader: 1,
		log:    NewLog(&bytes.Buffer{}, noop),
	}

	// receives a RequestVote from someone also in term=2
	resp, stepDown := s.handleRequestVote(RequestVote{
		Term:         2,
		CandidateId:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	})

	// and should retain his leadership
	if resp.VoteGranted {
		t.Errorf("shouldn't have granted vote")
	}
	if stepDown {
		t.Errorf("shouldn't have stepped down")
	}
}

func TestLimitedClientPatience(t *testing.T) {
	// a client issues a command

	// it's written to a leader log

	// but the leader is deposed before he can replicate it

	// the new leader truncates the command away

	// the client should not be stuck forever
}

func TestLenientCommit(t *testing.T) {
	// a log with lastIndex=5 commitIndex=5
	log := &Log{
		entries: []LogEntry{
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 2},
			LogEntry{Index: 4, Term: 2},
			LogEntry{Index: 5, Term: 2},
		},
		commitIndex: 5,
	}

	// belongs to a follower
	s := Server{
		id:     100,
		term:   2,
		leader: 101,
		log:    log,
		state:  &serverState{value: Follower},
	}

	// a leader attempts to AppendEntries with PrevLogIndex=5 CommitIndex=4
	resp, stepDown := s.handleAppendEntries(AppendEntries{
		Term:         2,
		LeaderId:     101,
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		CommitIndex:  4,
	})

	// this should not fail
	if !resp.Success {
		t.Errorf("failed (%s)", resp.reason)
	}
	if stepDown {
		t.Errorf("shouldn't step down")
	}
}
