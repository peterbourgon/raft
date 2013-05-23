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
	unknownLeader = 0
)

var (
	MinimumElectionTimeoutMs = 250
	MaximumElectionTimeoutMs = 2 * MinimumElectionTimeoutMs
)

var (
	ErrNotLeader             = errors.New("not the leader")
	ErrUnknownLeader         = errors.New("unknown leader")
	ErrDeposed               = errors.New("deposed during replication")
	ErrAppendEntriesRejected = errors.New("AppendEntries RPC rejected")
	ErrReplicationFailed     = errors.New("command replication failed (but will keep retrying)")
)

func init() {
	if MaximumElectionTimeoutMs <= MinimumElectionTimeoutMs {
		panic(fmt.Sprintf(
			"MaximumElectionTimeoutMs (%d) must be > MinimumElectionTimeoutMs (%d)",
			MaximumElectionTimeoutMs,
			MinimumElectionTimeoutMs,
		))
	}
}

// ElectionTimeout returns a variable time.Duration, between
// MinimumElectionTimeoutMs and twice that value.
func ElectionTimeout() time.Duration {
	n := rand.Intn(MaximumElectionTimeoutMs - MinimumElectionTimeoutMs)
	d := MinimumElectionTimeoutMs + n
	return time.Duration(d) * time.Millisecond
}

// BroadcastInterval returns the interval between heartbeats (AppendEntry RPCs)
// broadcast from the leader. It is MinimumElectionTimeoutMs / 10, as dictated
// by the spec: BroadcastInterval << ElectionTimeout << MTBF.
func BroadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMs / 10
	return time.Duration(d) * time.Millisecond
}

// serverState is just a string protected by a mutex.
type serverState struct {
	sync.RWMutex
	value string
}

func (s *serverState) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *serverState) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// serverRunning is just a bool protected by a mutex.
type serverRunning struct {
	sync.RWMutex
	value bool
}

func (s *serverRunning) Get() bool {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *serverRunning) Set(value bool) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// Server is the agent that performs all of the Raft protocol logic.
// In a typical application, each running process that wants to be part of
// the distributed state machine will contain a server component.
type Server struct {
	Id                uint64 // id of this server (do not modify)
	state             *serverState
	running           *serverRunning
	leader            uint64 // who we believe is the leader
	term              uint64 // "current term number, which increases monotonically"
	vote              uint64 // who we voted for this term, if applicable
	log               *Log
	peers             Peers
	appendEntriesChan chan appendEntriesTuple
	requestVoteChan   chan requestVoteTuple
	commandChan       chan commandTuple
	responsesChan     chan []byte
	electionTick      <-chan time.Time
	quit              chan chan struct{}
}

// NewServer returns an initialized, un-started server.
// The ID must be unique in the Raft network, and greater than 0.
// The store will be used by the distributed log as a persistence layer.
// The apply function will be called whenever a (user-domain) command has been
// safely replicated to this server, and can be considered committed.
func NewServer(id uint64, store io.ReadWriter, apply func([]byte) ([]byte, error)) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	s := &Server{
		Id:                id,
		state:             &serverState{value: Follower}, // "when servers start up they begin as followers"
		running:           &serverRunning{value: false},
		leader:            unknownLeader, // unknown at startup
		term:              1,             // TODO is this correct?
		log:               NewLog(store, apply),
		peers:             nil,
		appendEntriesChan: make(chan appendEntriesTuple),
		requestVoteChan:   make(chan requestVoteTuple),
		commandChan:       make(chan commandTuple),
		responsesChan:     make(chan []byte),
		electionTick:      time.NewTimer(ElectionTimeout()).C, // one-shot
		quit:              make(chan chan struct{}),
	}
	return s
}

// SetPeers injects the set of peers that this server will attempt to
// communicate with, in its Raft network. The set peers should include a peer
// that represents this server, so that quorum is calculated correctly.
func (s *Server) SetPeers(p Peers) {
	s.peers = p
}

// State returns the current state: follower, candidate, or leader.
func (s *Server) State() string {
	return s.state.Get()
}

// Start triggers the server to begin communicating with its peers.
func (s *Server) Start() {
	go s.loop()
}

// Stop terminates the server. Stopped servers should not be restarted.
func (s *Server) Stop() {
	q := make(chan struct{})
	s.quit <- q
	<-q
	s.logGeneric("server stopped")
}

type commandTuple struct {
	Command []byte
	Err     chan error
}

// Command appends the passed command to the leader log. If error is nil, the
// command will eventually get replicated throughout the Raft network. When the
// command gets committed to the local server log, it's passed to the apply
// function, and the response from that function is provided on the
// CommandResponses channel.
//
// This is a public method only to facilitate the construction of peers
// on arbitrary transports.
func (s *Server) Command(cmd []byte) error {
	err := make(chan error)
	s.commandChan <- commandTuple{cmd, err}
	return <-err
}

// CommandResponses yields a channel that contains ordered responses to every
// command issued via Command. Clients are obliged to consume every response
// from this channel in a timely manner.
//
// This is a public method only to facilitate the construction of peers
// on arbitrary transports.
func (s *Server) CommandResponses() <-chan []byte {
	return s.responsesChan
}

// AppendEntries processes the given RPC and returns the response.
//
// This is a public method only to facilitate the construction of peers
// on arbitrary transports.
func (s *Server) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	t := appendEntriesTuple{
		Request:  ae,
		Response: make(chan AppendEntriesResponse),
	}
	s.appendEntriesChan <- t
	return <-t.Response
}

// RequestVote processes the given RPC and returns the response.
//
// This is a public method only to facilitate the construction of Peers
// on arbitrary transports.
func (s *Server) RequestVote(rv RequestVote) RequestVoteResponse {
	t := requestVoteTuple{
		Request:  rv,
		Response: make(chan RequestVoteResponse),
	}
	s.requestVoteChan <- t
	return <-t.Response
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
	s.running.Set(true)
	for s.running.Get() {
		switch state := s.State(); state {
		case Follower:
			s.followerSelect()
		case Candidate:
			s.candidateSelect()
		case Leader:
			s.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State '%s'", state))
		}
	}
}

func (s *Server) resetElectionTimeout() {
	s.electionTick = time.NewTimer(ElectionTimeout()).C
}

func (s *Server) logGeneric(format string, args ...interface{}) {
	prefix := fmt.Sprintf("id=%d term=%d state=%s: ", s.Id, s.term, s.State())
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

func (s *Server) forwardCommand(t commandTuple) {
	switch s.leader {
	case unknownLeader:
		s.logGeneric("got command, but don't know leader")
		t.Err <- ErrUnknownLeader

	case s.Id: // I am the leader
		panic("impossible state in forwardCommand")

	default:
		leader, ok := s.peers[s.leader]
		if !ok {
			panic("invalid state in peers")
		}
		s.logGeneric("got command, forwarding to %d", s.leader)
		// We're blocking our {follower,candidate}Select function in the
		// receive-command branch. If we continue to block while forwarding
		// the command, the leader won't be able to get a response from us!
		go func() { t.Err <- leader.Command(t.Command) }()
	}
}

func (s *Server) followerSelect() {
	for {
		select {
		case q := <-s.quit:
			s.running.Set(false)
			close(s.responsesChan)
			close(q)
			return

		case t := <-s.commandChan:
			s.forwardCommand(t)

		case <-s.electionTick:
			// 5.2 Leader election: "A follower increments its current term and
			// transitions to candidate state."
			s.logGeneric("election timeout, becoming candidate")
			s.term++
			s.state.Set(Candidate)
			s.leader = unknownLeader
			s.resetElectionTimeout()
			return

		case t := <-s.appendEntriesChan:
			if s.leader == unknownLeader {
				s.leader = t.Request.LeaderId
				s.logGeneric("discovered Leader %d", s.leader)
			}
			if s.leader != t.Request.LeaderId {
				s.logGeneric("our leader (%d) conflicts with AppendEntries (%d)", s.leader, t.Request.LeaderId)
				panic("inconsistent leader state in network")
			}
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
	s.leader = unknownLeader
	responses, canceler := s.peers.Except(s.Id).requestVotes(RequestVote{
		Term:         s.term,
		CandidateId:  s.Id,
		LastLogIndex: s.log.lastIndex(),
		LastLogTerm:  s.log.lastTerm(),
	})
	defer canceler.Cancel()
	s.vote = s.Id      // vote for myself
	votesReceived := 1 // already have a vote from myself
	votesRequired := s.peers.Quorum()
	s.logGeneric("election started, %d vote(s) required", votesRequired)

	// catch a bad state
	if votesReceived >= votesRequired {
		s.logGeneric("%d-node cluster; I win", s.peers.Count())
		s.state.Set(Leader)
		return
	}

	// "A candidate continues in this state until one of three things happens:
	// (a) it wins the election, (b) another server establishes itself as
	// leader, or (c) a period of time goes by with no winner."
	for {
		select {
		case q := <-s.quit:
			s.running.Set(false)
			close(s.responsesChan)
			close(q)
			return

		case t := <-s.commandChan:
			s.forwardCommand(t)

		case r := <-responses:
			s.logGeneric("got vote: term=%d granted=%v", r.Term, r.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if r.Term > s.term {
				s.logGeneric("got future term (%d>%d); abandoning election", r.Term, s.term)
				s.leader = unknownLeader
				s.state.Set(Follower)
				s.vote = 0
				return // lose
			}
			if r.Term < s.term {
				s.logGeneric("got vote from past term (%d<%d); ignoring", r.Term, s.term)
				break
			}
			if r.VoteGranted {
				votesReceived++
			}
			// "Once a candidate wins an election, it becomes leader."
			if votesReceived >= votesRequired {
				s.logGeneric("%d >= %d: win", votesReceived, votesRequired)
				s.leader = s.Id
				s.state.Set(Leader)
				s.vote = 0
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
				s.logGeneric("stepping down to Follower (leader=%s)", s.leader)
				s.leader = t.Request.LeaderId
				s.state.Set(Follower)
				s.vote = 0
				return // lose
			}

		case t := <-s.requestVoteChan:
			// We can also be defeated by a more recent candidate
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("stepping down to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(Follower)
				s.vote = 0
				return // lose
			}

		case <-s.electionTick: //  "a period of time goes by with no winner"
			// "The third possible outcome is that a candidate neither wins nor
			// loses the election: if many followers become candidates at the
			// same time, votes could be split so that no candidate obtains a
			// majority. When this happens, each candidate will start a new
			// election by incrementing its term and initiating another round of
			// RequestVote RPCs."
			s.logGeneric("election ended with no winner; incrementing term and trying again")
			s.resetElectionTimeout()
			s.term++
			s.vote = 0
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

// flush generates and forwards an AppendEntries request that attempts to bring
// the given follower "in sync" with our log. It's idempotent, so it's used for
// both heartbeats and replicating commands.
//
// The AppendEntries request we build represents our best attempt at a "delta"
// between our log and the follower's log. The passed nextIndex structure
// manages that state.
//
// flush is synchronous and can block forever if the peer is nonresponsive.
func (s *Server) flush(peer Peer, ni *nextIndex) error {
	peerId := peer.Id()
	currentTerm := s.term
	prevLogIndex := ni.PrevLogIndex(peerId)
	entries, prevLogTerm := s.log.entriesAfter(prevLogIndex, currentTerm)
	commitIndex := s.log.getCommitIndex()
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

// flushTimeout calls flush, and returns ErrTimeout if it doesn't return
// within the given timeout.
func (s *Server) flushTimeout(peer Peer, ni *nextIndex, timeout time.Duration) error {
	err := make(chan error)
	cancel := make(chan struct{})
	go func() {
		select {
		case err <- s.flush(peer, ni):
			break
		case <-cancel:
			break
		}
	}()

	select {
	case e := <-err:
		return e
	case <-time.After(timeout):
		close(cancel)
		return ErrTimeout
	}
}

func (s *Server) leaderSelect() {
	s.leader = s.Id

	// 5.3 Log replication: "The leader maintains a nextIndex for each follower,
	// which is the index of the next log entry the leader will send to that
	// follower. When a leader first comes to power it initializes all nextIndex
	// values to the index just after the last one in its log."
	//
	// I changed this from lastIndex+1 to simply lastIndex. Every initial
	// communication from leader to follower was being rejected and we were
	// doing the decrement. This was just annoying, except if you manage to
	// sneak in a command before the first heartbeat. Then, it will never get
	// properly replicated (it seemed).
	ni := newNextIndex(s.peers, s.log.lastIndex()) // +1)

	heartbeatTick := time.Tick(BroadcastInterval())
	for {
		select {
		case q := <-s.quit:
			s.running.Set(false)
			close(s.responsesChan)
			close(q)
			return

		case t := <-s.commandChan:
			// Append the command to our (leader) log
			currentTerm := s.term
			entry := LogEntry{
				Index:   s.log.lastIndex() + 1,
				Term:    currentTerm,
				Command: t.Command,
			}
			if err := s.log.appendEntry(entry); err != nil {
				t.Err <- err
				continue
			}

			// From here forward, we'll always attempt to replicate the command
			// to our followers, via the heartbeat mechanism. This timeout is
			// purely for our present response to the client.
			timeout := time.After(BroadcastInterval() * 3) // TODO arbitrary

			// Scatter flush requests to all peers
			recipients := s.peers.Except(s.Id)
			responses := make(chan error, len(recipients))
			for _, peer := range recipients {
				go func(peer0 Peer) {
					// We can use a blocking flush here, because the timeout is
					// handled at the transaction layer.
					err := s.flush(peer0, ni)
					if err != nil {
						s.logGeneric("replicate: flush to %d: %s", peer0.Id(), err)
					}
					responses <- err
				}(peer)
			}

			// Gather responses and signal a deposition or successful commit
			commit := make(chan struct{})
			failed := make(chan struct{})
			deposed := make(chan struct{})
			go func() {
				have, required := 1, s.peers.Quorum()
				// a 1-node cluster has have == required, len(recipients) == 0
				for i := 0; i < len(recipients); i++ {
					err := <-responses
					if err == ErrDeposed {
						close(deposed)
						return
					}
					if err == nil {
						have++
					}
					if have >= required {
						break
					}
				}
				if have >= required {
					close(commit)
					return
				}
				close(failed)
			}()

			// resolve
			select {
			case <-commit:
				s.logGeneric("replication of command succeeded; committing")
				if err := s.log.commitTo(entry.Index, s.responsesChan); err != nil {
					panic(err)
				}
				for _, peer := range s.peers.Except(s.Id) {
					// Technically, we don't need to send this flush: it will
					// get replicated on the next heartbeat. We do it here
					// purely to get things pushed out faster. So it's OK to
					// have "fire and forget" semantics.
					go s.flush(peer, ni)
				}
				t.Err <- nil
				s.logGeneric("replication and commit of command succeeded")
				continue

			case <-failed:
				s.logGeneric("replication of command failed")
				t.Err <- ErrReplicationFailed
				continue

			case <-deposed:
				t.Err <- ErrDeposed
				s.state.Set(Follower)
				s.leader = unknownLeader
				s.logGeneric("during Command, deposed to Follower (leader unknown)")
				return

			case <-timeout:
				s.logGeneric("replication of command timed out")
				t.Err <- ErrTimeout
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
					err := s.flushTimeout(peer0, ni, BroadcastInterval())
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
				s.leader = t.Request.LeaderId
				s.state.Set(Follower)
				s.logGeneric("after an AppendEntries, deposed to Follower (leader=%d)", s.leader)
				return
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.leader = unknownLeader
				s.state.Set(Follower)
				s.logGeneric("after a RequestVote, deposed to Follower (leader unknown)")
				return
			}
		}
	}
}

func (s *Server) handleRequestVote(rv RequestVote) (RequestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if rv.Term < s.term {
		return RequestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if rv.Term > s.term {
		s.term = rv.Term
		s.vote = 0
		stepDown = true
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != rv.CandidateId {
		return RequestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.log.lastIndex() > rv.LastLogIndex || s.log.lastTerm() > rv.LastLogTerm {
		return RequestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason: fmt.Sprintf(
				"our index/term %d/%d > %d/%d",
				s.log.lastIndex(),
				s.log.lastTerm(),
				rv.LastLogIndex,
				rv.LastLogTerm,
			),
		}, stepDown
	}

	// We passed all the tests: cast vote in favor
	s.vote = rv.CandidateId
	s.resetElectionTimeout() // TODO why?
	return RequestVoteResponse{
		Term:        s.term,
		VoteGranted: true,
	}, stepDown
}

func (s *Server) handleAppendEntries(r AppendEntries) (AppendEntriesResponse, bool) {
	// Spec is ambiguous here; basing this on benbjohnson's impl

	// Maybe a nicer way to handle this is to define explicit handler functions
	// for each Server state. Then, we won't try to hide too much logic (i.e.
	// too many protocol rules) in one code path.

	// If the request is from an old term, reject
	if r.Term < s.term {
		return AppendEntriesResponse{
			Term:    s.term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.term {
		s.term = r.Term
		s.vote = 0
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an AppendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if s.State() == Candidate && r.LeaderId != s.leader && r.Term >= s.term {
		s.term = r.Term
		s.vote = 0
		stepDown = true
	}

	// In any case, reset our election timeout
	s.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := s.log.ensureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return AppendEntriesResponse{
			Term:    s.term,
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
		if err := s.log.appendEntry(entry); err != nil {
			return AppendEntriesResponse{
				Term:    s.term,
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
		if err := s.log.commitTo(r.CommitIndex, s.responsesChan); err != nil {
			return AppendEntriesResponse{
				Term:    s.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	// all good
	return AppendEntriesResponse{
		Term:    s.term,
		Success: true,
	}, stepDown
}
