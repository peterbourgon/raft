package raft

import (
	"bytes"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"
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

func TestCatchUp(t *testing.T) {
	// (since we actually enter the loop on this one, let's squelch some things)
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := ResetElectionTimeoutMs(2500, 5000)
	defer ResetElectionTimeoutMs(oldMin, oldMax)

	// (common log for all servers)
	getLog := func() *Log {
		return &Log{
			store: &bytes.Buffer{},
			entries: []LogEntry{
				LogEntry{Index: 1, Term: 1},
				LogEntry{Index: 2, Term: 1},
				LogEntry{Index: 3, Term: 2},
				LogEntry{Index: 4, Term: 2},
			},
			commitIndex: 4,
			apply:       noop,
		}
	}

	// a leader
	leader := &Server{
		id:     2,
		term:   2,
		leader: 2,
		log:    getLog(),
		state:  &serverState{value: Leader},
	}

	// has two synchronized followers
	f5 := &Server{
		id:                5,
		term:              2,
		leader:            2,
		log:               getLog(),
		state:             &serverState{value: Follower},
		running:           &serverRunning{value: false},
		appendEntriesChan: make(chan appendEntriesTuple),
		quit:              make(chan chan struct{}),
	}
	f5.Start()
	defer f5.Stop()
	f6 := &Server{
		id:                6,
		term:              2,
		leader:            2,
		log:               getLog(),
		state:             &serverState{value: Follower},
		running:           &serverRunning{value: false},
		appendEntriesChan: make(chan appendEntriesTuple),
		quit:              make(chan chan struct{}),
	}
	f6.Start()
	defer f6.Stop()

	// one of which will delay in sending the first response
	firstDelay, subsequentDelays := 50*time.Millisecond, 0*time.Second
	peers := Peers{
		5: &delayPeer{f5, firstDelay, subsequentDelays, 0},
		6: NewLocalPeer(f6),
	}

	// but both having the same prevLogIndex in nextIndex
	ni := &nextIndex{m: map[uint64]uint64{5: 4, 6: 4}}

	// the leader gets a new entry
	leader.log.appendEntry(LogEntry{Index: 5, Term: 2, Command: []byte(`{}`)})

	// and wants to flush it (without advancing commitIndex) to its followers
	leader.concurrentFlush(peers, ni, firstDelay/2)

	// one follower handles it fine, and the leader's nextIndex is updated
	if ni.PrevLogIndex(6) != 5 {
		t.Errorf("good follower should have prevLogIndex=5")
	}

	// the slow follower has timed out, from the leader's perspective
	// but we wait for it to receive and apply the AppendEntries locally
	time.Sleep(2 * firstDelay)

	// the slow follower has applied the entry to its log
	if f5.log.lastIndex() != 5 {
		t.Fatalf("slow follower didn't get the AppendEntries")
	}
	if f5.log.getCommitIndex() != 4 {
		t.Fatalf("slow follower somehow got a bad commitIndex")
	}

	// but because it didn't respond in time, leader's nextIndex isn't updated
	if ni.PrevLogIndex(5) != 4 {
		t.Errorf("slow follower should have prevLogIndex=4")
	}

	// anyway: the leader got a quorum of successes, so it can commit
	if err := leader.log.commitTo(5); err != nil {
		t.Fatalf("leader commitTo(5) failed")
	}

	// and after 1 more flush
	leader.concurrentFlush(peers, ni, firstDelay/2)

	// both followers should have identical state in nextIndex
	if ni.PrevLogIndex(6) != 5 {
		t.Errorf("good follower should have prevLogIndex=5")
	}
	if ni.PrevLogIndex(5) != 5 {
		t.Errorf("slow follower should have prevLogIndex=5")
	}

	// and should be committed to the same point
	if f6.log.lastIndex() != 5 {
		t.Errorf("good follower should have lastIndex=5")
	}
	if f6.log.getCommitIndex() != 5 {
		t.Errorf("good follower should have commitIndex=5")
	}
	if f5.log.lastIndex() != 5 {
		t.Errorf("slow follower should have lastIndex=5")
	}
	if f5.log.getCommitIndex() != 5 {
		t.Errorf("slow follower should have commitIndex=5")
	}

	// meaning we can add a new entry,
	leader.log.appendEntry(LogEntry{Index: 6, Term: 2, Command: []byte(`{}`)})

	// flush it,
	leader.concurrentFlush(peers, ni, firstDelay/2)

	// commit it,
	leader.log.commitTo(6)

	// flush the commit,
	leader.concurrentFlush(peers, ni, firstDelay/2)

	// and both followers should again be identical in our nextIndex
	if ni.PrevLogIndex(6) != 6 {
		t.Errorf("good follower should have prevLogIndex=5")
	}
	if ni.PrevLogIndex(5) != 6 {
		t.Errorf("slow follower should have prevLogIndex=5")
	}

	// and should have the same log state
	if f6.log.lastIndex() != 6 {
		t.Errorf("good follower should have lastIndex=6")
	}
	if f6.log.getCommitIndex() != 6 {
		t.Errorf("good follower should have commitIndex=6")
	}
	if f5.log.lastIndex() != 6 {
		t.Errorf("slow follower should have lastIndex=6")
	}
	if f5.log.getCommitIndex() != 6 {
		t.Errorf("slow follower should have commitIndex=6")
	}
}

type delayPeer struct {
	server           *Server
	firstDelay       time.Duration
	subsequentDelays time.Duration
	requests         uint32
}

func (p *delayPeer) Id() uint64 { return p.server.id }

func (p *delayPeer) delay() {
	if atomic.LoadUint32(&p.requests) == 0 {
		time.Sleep(p.firstDelay)
	} else {
		time.Sleep(p.subsequentDelays)
	}
	atomic.AddUint32(&p.requests, 1)
}

func (p *delayPeer) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	p.delay()
	return p.server.AppendEntries(ae)
}

func (p *delayPeer) RequestVote(rv RequestVote) RequestVoteResponse {
	p.delay()
	return p.server.RequestVote(rv)
}

func (p *delayPeer) Command(cmd []byte, response chan []byte) error {
	p.delay()
	return p.server.Command(cmd, response)
}
