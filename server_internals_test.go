package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
)

func TestFollowerAllegiance(t *testing.T) {
	// a follower with allegiance to leader=2
	s := Server{
		id:     1,
		term:   5,
		state:  &protectedString{value: Follower},
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
		state:  &protectedString{value: Leader},
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
	// a log that's fully committed
	log := &Log{
		entries: []LogEntry{
			LogEntry{Index: 1, Term: 1},
			LogEntry{Index: 2, Term: 1},
			LogEntry{Index: 3, Term: 2},
			LogEntry{Index: 4, Term: 2},
			LogEntry{Index: 5, Term: 2},
		},
		commitPos: 4,
	}

	// belongs to a follower
	s := Server{
		id:     100,
		term:   2,
		leader: 101,
		log:    log,
		state:  &protectedString{value: Follower},
	}

	// an AppendEntries comes with correct PrevLogIndex but older CommitIndex
	resp, stepDown := s.handleAppendEntries(AppendEntries{
		Term:         2,
		LeaderId:     101,
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		CommitIndex:  4, // i.e. commitPos=3
	})

	// this should not fail
	if !resp.Success {
		t.Errorf("failed (%s)", resp.reason)
	}
	if stepDown {
		t.Errorf("shouldn't step down")
	}
}

func TestConfigurationReceipt(t *testing.T) {
	// a follower
	s := Server{
		id:     2,
		term:   1,
		leader: 1,
		log: &Log{
			entries:   []LogEntry{LogEntry{Index: 1, Term: 1}},
			commitPos: 0,
		},
		state:         &protectedString{value: Follower},
		configuration: NewConfiguration(Peers{}),
	}

	// receives a configuration change
	peers := Peers{
		1: serializablePeer{1, "foo"},
		2: serializablePeer{2, "bar"},
		3: serializablePeer{3, "baz"},
	}
	configurationBuf := &bytes.Buffer{}
	gob.Register(&serializablePeer{})
	if err := gob.NewEncoder(configurationBuf).Encode(peers); err != nil {
		t.Fatal(err)
	}

	// via an AppendEntries
	aer, _ := s.handleAppendEntries(AppendEntries{
		Term:         1,
		LeaderId:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []LogEntry{
			LogEntry{
				Index:           2,
				Term:            1,
				Command:         configurationBuf.Bytes(),
				isConfiguration: true,
			},
		},
		CommitIndex: 1,
	})

	// it should succeed
	if !aer.Success {
		t.Fatalf("AppendEntriesResponse: no success: %s", aer.reason)
	}

	// and the follower's configuration should be immediately updated
	if expected, got := 3, s.configuration.AllPeers().Count(); expected != got {
		t.Fatalf("follower peer count: expected %d, got %d", expected, got)
	}
	peer, ok := s.configuration.Get(3)
	if !ok {
		t.Fatal("follower didn't get peer 3")
	}
	if peer.Id() != 3 {
		t.Fatal("follower got bad peer 3")
	}
}

type serializablePeer struct {
	MyId uint64
	Err  string
}

func (p serializablePeer) Id() uint64 { return p.MyId }
func (p serializablePeer) AppendEntries(AppendEntries) AppendEntriesResponse {
	return AppendEntriesResponse{}
}
func (p serializablePeer) RequestVote(rv RequestVote) RequestVoteResponse {
	return RequestVoteResponse{}
}
func (p serializablePeer) Command([]byte, chan []byte) error {
	return fmt.Errorf("%s", p.Err)
}
func (p serializablePeer) SetConfiguration(Peers) error {
	return fmt.Errorf("%s", p.Err)
}
