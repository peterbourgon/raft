package raft

import (
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const (
	MinimumElectionTimeoutMs = 250
)

var (
	ErrNotLeader             = errors.New("not the leader")
	ErrDeposed               = errors.New("deposed during replication")
	ErrAppendEntriesRejected = errors.New("AppendEntries RPC rejected")
)

func ElectionTimeout() time.Duration {
	n := rand.Intn(MinimumElectionTimeoutMs)
	d := MinimumElectionTimeoutMs + n
	return time.Duration(d) * time.Millisecond
}

func BroadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 10
	return time.Duration(d) * time.Millisecond
}

type concurrentState struct {
	sync.RWMutex
	value string
}

func (s *concurrentState) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *concurrentState) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

type Server struct {
	Id                uint64 // of this server, for elections and redirects
	State             *concurrentState
	Term              uint64 // "current term number, which increases monotonically"
	vote              uint64 // who we voted for this term, if applicable
	Log               *Log
	peers             Peers
	appendEntriesChan chan appendEntriesTuple
	requestVoteChan   chan requestVoteTuple
	commandChan       chan commandTuple
	electionTick      <-chan time.Time
}

func NewServer(id uint64, store io.Writer, apply func([]byte) ([]byte, error)) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	s := &Server{
		Id:                id,
		State:             &concurrentState{value: Follower}, // "when servers start up they begin as followers"
		Term:              1,                                 // TODO is this correct?
		Log:               NewLog(store, apply),
		peers:             nil,
		appendEntriesChan: make(chan appendEntriesTuple),
		requestVoteChan:   make(chan requestVoteTuple),
		commandChan:       make(chan commandTuple),
		electionTick:      time.NewTimer(ElectionTimeout()).C, // one-shot
	}
	return s
}

func (s *Server) Start() {
	go s.loop()
}

// TODO Command accepts client commands, which will (hopefully) get replicated
// across all state machines. Note that Command is completely out-of-band of
// Raft-domain RPC.
type commandTuple struct {
	Command  []byte
	Response chan []byte
	Err      chan error
}

func (s *Server) Command(cmd []byte) ([]byte, error) {
	t := commandTuple{cmd, make(chan []byte), make(chan error)}
	s.commandChan <- t
	select {
	case resp := <-t.Response:
		return resp, nil
	case err := <-t.Err:
		return []byte{}, err
	}
}

func (s *Server) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	t := appendEntriesTuple{
		Request:  ae,
		Response: make(chan AppendEntriesResponse),
	}
	s.appendEntriesChan <- t
	return <-t.Response
}

func (s *Server) RequestVote(rv RequestVote) RequestVoteResponse {
	t := requestVoteTuple{
		Request:  rv,
		Response: make(chan RequestVoteResponse),
	}
	s.requestVoteChan <- t
	return <-t.Response
}

func (s *Server) SetPeers(p Peers) {
	s.peers = p
}

//                                  times out,
//                                 new election
//     |                             .-----.
//     |                             |     |
//     v         times out,          |     v     receives votes from
// +----------+  starts election  +-----------+  majority of servers  +--------+
// | Follower |------------------>| Candidate |---------------------->| Leader |
// +----------+                   +-----------+                       +--------+
//     ^ ^                              |                                 |
//     | |    discovers current leader  |                                 |
//     | |                 or new term  |                                 |
//     | '------------------------------'                                 |
//     |                                                                  |
//     |                               discovers server with higher term  |
//     '------------------------------------------------------------------'
//
//

func (s *Server) loop() {
	for {
		switch s.State.Get() {
		case Follower:
			s.followerSelect()
		case Candidate:
			s.candidateSelect()
		case Leader:
			s.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State '%s'", s.State))
		}
	}
}

func (s *Server) resetElectionTimeout() {
	s.electionTick = time.NewTimer(ElectionTimeout()).C
}

func (s *Server) logGeneric(format string, args ...interface{}) {
	prefix := fmt.Sprintf("id=%d term=%d state=%s: ", s.Id, s.Term, s.State.Get())
	log.Printf(prefix+format, args...)
}

func (s *Server) logAppendEntriesResponse(req AppendEntries, resp AppendEntriesResponse, stepDown bool) {
	s.logGeneric(
		"got AppendEntries, sz=%d prevIndex/Term=%d/%d commitIndex=%d: responded with success=%v (%s) stepDown=%v",
		len(req.Entries),
		req.PrevLogIndex,
		req.PrevLogTerm,
		req.CommitIndex,
		resp.Success,
		resp.reason,
		stepDown,
	)
}
func (s *Server) logRequestVoteResponse(req RequestVote, resp RequestVoteResponse, stepDown bool) {
	s.logGeneric(
		"got RequestVote, candidate=%d: responded with granted=%v (%s) stepDown=%v",
		req.CandidateId,
		resp.VoteGranted,
		resp.reason,
		stepDown,
	)
}

func (s *Server) followerSelect() {
	for {
		select {
		case commandTuple := <-s.commandChan:
			commandTuple.Err <- ErrNotLeader // TODO forward instead
			continue

		case <-s.electionTick:
			// 5.2 Leader election: "A follower increments its current term and
			// transitions to candidate state."
			s.logGeneric("election timeout, becoming candidate")
			s.Term++
			s.State.Set(Candidate)
			s.resetElectionTimeout()
			return

		case t := <-s.appendEntriesChan:
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
		}
	}
}

func (s *Server) candidateSelect() {
	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	responses, canceler := s.peers.Except(s.Id).RequestVotes(RequestVote{
		Term:         s.Term,
		CandidateId:  s.Id,
		LastLogIndex: s.Log.LastIndex(),
		LastLogTerm:  s.Log.LastTerm(),
	})
	defer canceler.Cancel()
	votesReceived := 1 // already have a vote from myself
	votesRequired := s.peers.Quorum()
	s.logGeneric("election started, %d vote(s) required", votesRequired)

	// catch a bad state
	if votesReceived >= votesRequired {
		s.logGeneric("%d-node cluster; I win", s.peers.Count())
		s.State.Set(Leader)
		return
	}

	// "A candidate continues in this state until one of three things happens:
	// (a) it wins the election, (b) another server establishes itself as
	// leader, or (c) a period of time goes by with no winner."
	for {
		select {
		case commandTuple := <-s.commandChan:
			commandTuple.Err <- ErrNotLeader // TODO forward instead
			continue

		case r := <-responses:
			s.logGeneric("got vote: term=%d granted=%v", r.Term, r.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if r.Term != s.Term {
				// TODO what if r.Term > s.Term? do we lose the election?
				continue
			}
			if r.VoteGranted {
				votesReceived++
			}
			// "Once a candidate wins an election, it becomes leader."
			if votesReceived >= votesRequired {
				s.logGeneric("%d >= %d: win", votesReceived, votesRequired)
				s.State.Set(Leader)
				return // win
			}

		case t := <-s.appendEntriesChan:
			// "While waiting for votes, a candidate may receive an
			// AppendEntries RPC from another server claiming to be leader.
			// If the leader's term (included in its RPC) is at least as
			// large as the candidate's current term, then the candidate
			// recognizes the leader as legitimate and steps down, meaning
			// that it returns to follower state."
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("stepping down to Follower")
				s.State.Set(Follower)
				return // lose
			}

		case t := <-s.requestVoteChan:
			// We can also be defeated by a more recent candidate
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("stepping down to Follower")
				s.State.Set(Follower)
				return // lose
			}

		case <-s.electionTick: //  "a period of time goes by with no winner"
			s.logGeneric("election ended with no winner")
			s.resetElectionTimeout()
			return // draw
		}
	}
}

//
//
//

type nextIndex struct {
	sync.RWMutex
	m map[uint64]uint64 // followerId: nextIndex
}

func newNextIndex(peers Peers, defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for id, _ := range peers {
		ni.m[id] = defaultNextIndex
	}
	return ni
}

func (ni *nextIndex) PrevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}

func (ni *nextIndex) Decrement(id uint64) {
	ni.Lock()
	defer ni.Unlock()
	if i, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	} else if i > 0 {
		// This value can reach 0, so it should not be passed
		// directly to log.EntriesAfter.
		ni.m[id]--
	}
}

func (ni *nextIndex) Set(id, index uint64) {
	ni.Lock()
	defer ni.Unlock()
	ni.m[id] = index
}

// Flush generates and forwards an AppendEntries request that attempts to bring
// the given follower "in sync" with our log. It's idempotent, so it's used for
// both heartbeats and replicating commands.
//
// The AppendEntries request we build represents our best attempt at a "delta"
// between our log and the follower's log. The passed nextIndex structure
// manages that state.
func (s *Server) Flush(peer Peer, ni *nextIndex) error {
	peerId := peer.Id()
	currentTerm := s.Term
	prevLogIndex := ni.PrevLogIndex(peerId)
	entries, prevLogTerm := s.Log.EntriesAfter(prevLogIndex, currentTerm)
	commitIndex := s.Log.CommitIndex()
	resp := peer.AppendEntries(AppendEntries{
		Term:         currentTerm,
		LeaderId:     s.Id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		CommitIndex:  commitIndex,
	})
	if resp.Term > currentTerm {
		return ErrDeposed
	}
	if !resp.Success {
		ni.Decrement(peerId)
		return ErrAppendEntriesRejected
	}

	if len(entries) > 0 {
		ni.Set(peer.Id(), entries[len(entries)-1].Index)
	}
	return nil
}

func (s *Server) leaderSelect() {
	// 5.3 Log replication: "The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will send to that
	// follower. When a leader first comes to power it initializes all nextIndex
	// values to the index just after the last one in its log."
	ni := newNextIndex(s.peers, s.Log.LastIndex()+1)

	heartbeatTick := time.Tick(BroadcastInterval())
	for {
		select {
		case commandTuple := <-s.commandChan:
			// Append the command to our (leader) log
			currentTerm := s.Term
			entry := LogEntry{
				Index:   s.Log.LastIndex() + 1,
				Term:    currentTerm,
				Command: commandTuple.Command,
			}
			if err := s.Log.AppendEntry(entry); err != nil {
				commandTuple.Err <- err
				continue
			}

			// From here forward, we'll always attempt to replicate the command
			// to our followers, via the heartbeat mechanism. This timeout is
			// purely for our present response to the client.
			timeout := time.After(ElectionTimeout())

			// Scatter flush requests to all peers
			responses := make(chan error, len(s.peers))
			for _, peer := range s.peers.Except(s.Id) {
				go func(peer0 Peer) {
					err := s.Flush(peer0, ni)
					if err != nil {
						s.logGeneric("replicate: flush to %d: %s", peer0.Id(), err)
					}
					responses <- err
				}(peer)
			}

			// Gather responses and signal a deposition or successful commit
			committed := make(chan struct{})
			deposed := make(chan struct{})
			go func() {
				have, required := 1, s.peers.Quorum()
				for err := range responses {
					if err == ErrDeposed {
						close(deposed)
						return
					}
					if err == nil {
						have++
					}
					if have > required {
						close(committed)
						return
					}
				}
			}()

			// Return a response
			select {
			case <-deposed:
				commandTuple.Err <- ErrDeposed
				return
			case <-timeout:
				commandTuple.Err <- ErrTimeout
				continue
			case <-committed:
				// Commit our local log
				if err := s.Log.CommitTo(entry.Index); err != nil {
					panic(err)
				}
				// Push out another update, to sync that commit
				for _, peer := range s.peers.Except(s.Id) {
					s.Flush(peer, ni) // TODO I think this is OK?
				}
				commandTuple.Response <- []byte{} // TODO actual response
				continue
			}

		case <-heartbeatTick:
			// Heartbeats attempt to sync the follower log with ours.
			// That requires per-follower state in the form of nextIndex.
			recipients := s.peers.Except(s.Id)
			wg := sync.WaitGroup{}
			wg.Add(len(recipients))
			for _, peer := range recipients {
				go func(peer0 Peer) {
					defer wg.Done()
					err := s.Flush(peer0, ni)
					if err != nil {
						s.logGeneric(
							"heartbeat: flush to %d: %s (nextIndex now %d)",
							peer0.Id(),
							err,
							ni.PrevLogIndex(peer0.Id()),
						)
					}
				}(peer)
			}
			wg.Wait()

		case t := <-s.appendEntriesChan:
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.State.Set(Follower)
				return
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.State.Set(Follower)
				return
			}
		}
	}
}

func (s *Server) handleRequestVote(r RequestVote) (RequestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", r.Term, s.Term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.Term {
		s.Term = r.Term
		s.vote = 0
		stepDown = true
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != r.CandidateId {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.Log.LastIndex() > r.LastLogIndex || s.Log.LastTerm() > r.LastLogTerm {
		return RequestVoteResponse{
			Term:        s.Term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				s.Log.LastIndex(),
				s.Log.LastTerm(),
				r.LastLogIndex,
				r.LastLogTerm,
			),
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	s.vote = r.CandidateId
	s.resetElectionTimeout() // TODO why?
	return RequestVoteResponse{
		Term:        s.Term,
		VoteGranted: true,
	}, stepDown
}

func (s *Server) handleAppendEntries(r AppendEntries) (AppendEntriesResponse, bool) {
	// Spec is ambiguous here; basing this on benbjohnson's impl

	// Maybe a nicer way to handle this is to define explicit handler functions
	// for each Server state. Then, we won't try to hide too much logic (i.e.
	// too many protocol rules) in one code path.

	// If the request is from an old term, reject
	if r.Term < s.Term {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.Term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.Term {
		s.Term = r.Term
		s.vote = 0
		stepDown = true
	}

	// In any case, reset our election timeout
	s.resetElectionTimeout()

	// // Special case
	// if len(r.Entries) == 0 && r.CommitIndex == s.Log.CommitIndex() {
	// 	return AppendEntriesResponse{
	// 		Term:    s.Term,
	// 		Success: true,
	// 		reason:  "nothing to do",
	// 	}, stepDown
	// }

	// Reject if log doesn't contain a matching previous entry
	if err := s.Log.EnsureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return AppendEntriesResponse{
			Term:    s.Term,
			Success: false,
			reason: fmt.Sprintf(
				"while ensuring last log entry had index=%d term=%d: error: %s",
				r.PrevLogIndex,
				r.PrevLogTerm,
				err,
			),
		}, stepDown
	}

	// Append entries to the log
	for i, entry := range r.Entries {
		if err := s.Log.AppendEntry(entry); err != nil {
			return AppendEntriesResponse{
				Term:    s.Term,
				Success: false,
				reason: fmt.Sprintf(
					"AppendEntry %d/%d failed: %s",
					i+1,
					len(r.Entries),
					err,
				),
			}, stepDown
		}
	}

	// Commit up to the commit index
	if r.CommitIndex > 0 { // TODO perform this check, or let it fail?
		if err := s.Log.CommitTo(r.CommitIndex); err != nil {
			return AppendEntriesResponse{
				Term:    s.Term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	// all good
	return AppendEntriesResponse{
		Term:    s.Term,
		Success: true,
	}, stepDown
}
