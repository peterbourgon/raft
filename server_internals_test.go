package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"
)

func TestFollowerAllegiance(t *testing.T) {
	// a follower with allegiance to leader=2
	s := Server{
		id:     1,
		term:   5,
		state:  &protectedString{value: follower},
		leader: 2,
		log:    newRaftLog(&bytes.Buffer{}, noop),
	}

	// receives an appendEntries from a future term and different leader
	_, stepDown := s.handleAppendEntries(appendEntries{
		Term:     6,
		LeaderID: 3,
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
		state:  &protectedString{value: leader},
		leader: 1,
		log:    newRaftLog(&bytes.Buffer{}, noop),
	}

	// receives a requestVote from someone also in term=2
	resp, stepDown := s.handleRequestVote(requestVote{
		Term:         2,
		CandidateID:  3,
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
	log := &raftLog{
		entries: []logEntry{
			logEntry{Index: 1, Term: 1},
			logEntry{Index: 2, Term: 1},
			logEntry{Index: 3, Term: 2},
			logEntry{Index: 4, Term: 2},
			logEntry{Index: 5, Term: 2},
		},
		commitPos: 4,
	}

	// belongs to a follower
	s := Server{
		id:     100,
		term:   2,
		leader: 101,
		log:    log,
		state:  &protectedString{value: follower},
	}

	// an appendEntries comes with correct PrevLogIndex but older CommitIndex
	resp, stepDown := s.handleAppendEntries(appendEntries{
		Term:         2,
		LeaderID:     101,
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
		log: &raftLog{
			entries:   []logEntry{logEntry{Index: 1, Term: 1}},
			commitPos: 0,
		},
		state:  &protectedString{value: follower},
		config: newConfiguration(peerMap{}),
	}

	// receives a configuration change
	pm := makePeerMap(
		serializablePeer{1, "foo"},
		serializablePeer{2, "bar"},
		serializablePeer{3, "baz"},
	)
	configurationBuf := &bytes.Buffer{}
	gob.Register(&serializablePeer{})
	if err := gob.NewEncoder(configurationBuf).Encode(pm); err != nil {
		t.Fatal(err)
	}

	// via an appendEntries
	aer, _ := s.handleAppendEntries(appendEntries{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []logEntry{
			logEntry{
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
		t.Fatalf("appendEntriesResponse: no success: %s", aer.reason)
	}

	// and the follower's configuration should be immediately updated
	if expected, got := 3, s.config.allPeers().count(); expected != got {
		t.Fatalf("follower peer count: expected %d, got %d", expected, got)
	}
	peer, ok := s.config.get(3)
	if !ok {
		t.Fatal("follower didn't get peer 3")
	}
	if peer.id() != 3 {
		t.Fatal("follower got bad peer 3")
	}
}

func TestNonLeaderExpulsion(t *testing.T) {
	// a follower
	s := Server{
		id:     2,
		term:   1,
		leader: 1,
		log: &raftLog{
			store:     &bytes.Buffer{},
			entries:   []logEntry{logEntry{Index: 1, Term: 1}},
			commitPos: 0,
		},
		state:  &protectedString{value: follower},
		config: newConfiguration(peerMap{}),
		quit:   make(chan chan struct{}),
	}

	// receives a configuration change that doesn't include itself
	pm := makePeerMap(
		serializablePeer{1, "foo"},
		serializablePeer{3, "baz"},
		serializablePeer{5, "bat"},
	)
	configurationBuf := &bytes.Buffer{}
	gob.Register(&serializablePeer{})
	if err := gob.NewEncoder(configurationBuf).Encode(pm); err != nil {
		t.Fatal(err)
	}

	// via an appendEntries
	s.handleAppendEntries(appendEntries{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries: []logEntry{
			logEntry{
				Index:           2,
				Term:            1,
				Command:         configurationBuf.Bytes(),
				isConfiguration: true,
			},
		},
		CommitIndex: 1,
	})

	// and once committed
	s.handleAppendEntries(appendEntries{
		Term:         1,
		LeaderID:     1,
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		CommitIndex:  2,
	})

	// the follower should shut down
	select {
	case q := <-s.quit:
		q <- struct{}{}
	case <-time.After(maximumElectionTimeout()):
		t.Fatal("didn't shut down")
	}
}

type serializablePeer struct {
	MyID uint64
	Err  string
}

func (p serializablePeer) id() uint64 { return p.MyID }
func (p serializablePeer) callAppendEntries(appendEntries) appendEntriesResponse {
	return appendEntriesResponse{}
}
func (p serializablePeer) callRequestVote(requestVote) requestVoteResponse {
	return requestVoteResponse{}
}
func (p serializablePeer) callCommand([]byte, chan<- []byte) error {
	return fmt.Errorf("%s", p.Err)
}
func (p serializablePeer) callSetConfiguration(...Peer) error {
	return fmt.Errorf("%s", p.Err)
}
