// Package raft is an implementation of the Raft distributed consensus protocol.
package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	follower  = "Follower"
	candidate = "Candidate"
	leader    = "Leader"
)

const (
	unknownLeader = 0
	noVote        = 0
)

var (
	// MinimumElectionTimeoutMS can be set at package initialization. It may be
	// raised to achieve more reliable replication in slow networks, or lowered
	// to achieve faster replication in fast networks. Lowering is not
	// recommended.
	MinimumElectionTimeoutMS int32 = 250

	maximumElectionTimeoutMS = 2 * MinimumElectionTimeoutMS
)

var (
	errNotLeader             = errors.New("not the leader")
	errUnknownLeader         = errors.New("unknown leader")
	errDeposed               = errors.New("deposed during replication")
	errAppendEntriesRejected = errors.New("appendEntries RPC rejected")
	errReplicationFailed     = errors.New("command replication failed (but will keep retrying)")
	errOutOfSync             = errors.New("out of sync")
	errAlreadyRunning        = errors.New("already running")
)

// resetElectionTimeoutMS sets the minimum and maximum election timeouts to the
// passed values, and returns the old values.
func resetElectionTimeoutMS(newMin, newMax int) (int, int) {
	oldMin := atomic.LoadInt32(&MinimumElectionTimeoutMS)
	oldMax := atomic.LoadInt32(&maximumElectionTimeoutMS)
	atomic.StoreInt32(&MinimumElectionTimeoutMS, int32(newMin))
	atomic.StoreInt32(&maximumElectionTimeoutMS, int32(newMax))
	return int(oldMin), int(oldMax)
}

// minimumElectionTimeout returns the current minimum election timeout.
func minimumElectionTimeout() time.Duration {
	return time.Duration(MinimumElectionTimeoutMS) * time.Millisecond
}

// maximumElectionTimeout returns the current maximum election time.
func maximumElectionTimeout() time.Duration {
	return time.Duration(maximumElectionTimeoutMS) * time.Millisecond
}

// electionTimeout returns a variable time.Duration, between the minimum and
// maximum election timeouts.
func electionTimeout() time.Duration {
	n := rand.Intn(int(maximumElectionTimeoutMS - MinimumElectionTimeoutMS))
	d := int(MinimumElectionTimeoutMS) + n
	return time.Duration(d) * time.Millisecond
}

// broadcastInterval returns the interval between heartbeats (AppendEntry RPCs)
// broadcast from the leader. It is the minimum election timeout / 10, as
// dictated by the spec: BroadcastInterval << ElectionTimeout << MTBF.
func broadcastInterval() time.Duration {
	d := MinimumElectionTimeoutMS / 10
	return time.Duration(d) * time.Millisecond
}

// protectedString is just a string protected by a mutex.
type protectedString struct {
	sync.RWMutex
	value string
}

func (s *protectedString) Get() string {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedString) Set(value string) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// protectedBool is just a bool protected by a mutex.
type protectedBool struct {
	sync.RWMutex
	value bool
}

func (s *protectedBool) Get() bool {
	s.RLock()
	defer s.RUnlock()
	return s.value
}

func (s *protectedBool) Set(value bool) {
	s.Lock()
	defer s.Unlock()
	s.value = value
}

// Server is the agent that performs all of the Raft protocol logic.
// In a typical application, each running process that wants to be part of
// the distributed state machine will contain a server component.
type Server struct {
	id      uint64 // id of this server
	state   *protectedString
	running *protectedBool
	leader  uint64 // who we believe is the leader
	term    uint64 // "current term number, which increases monotonically"
	vote    uint64 // who we voted for this term, if applicable
	log     *raftLog
	config  *configuration

	appendEntriesChan chan appendEntriesTuple
	requestVoteChan   chan requestVoteTuple
	commandChan       chan commandTuple
	configurationChan chan configurationTuple

	electionTick <-chan time.Time
	quit         chan chan struct{}
}

// ApplyFunc is a client-provided function that should apply a successfully
// replicated state transition, represented by cmd, to the local state machine,
// and return a response. commitIndex is the sequence number of the state
// transition, which is guaranteed to be gapless and monotonically increasing,
// but not necessarily duplicate-free. ApplyFuncs are not called concurrently.
// Therefore, clients should ensure they return quickly, i.e. <<
// MinimumElectionTimeout.
type ApplyFunc func(commitIndex uint64, cmd []byte) []byte

// NewServer returns an initialized, un-started server. The ID must be unique in
// the Raft network, and greater than 0. The store will be used by the
// distributed log as a persistence layer. It's read-from during creation, in
// case a crashed server is restarted over an already-persisted log. Then, it's
// written-to during normal operations, when log entries are safely replicated.
// ApplyFunc will be called whenever a (user-domain) command has been safely
// replicated and committed to this server's log.
//
// NewServer creates a server, but you'll need to couple it with a transport to
// make it usable. See the example(s) for usage scenarios.
func NewServer(id uint64, store io.ReadWriter, a ApplyFunc) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	// 5.2 Leader election: "the latest term this server has seen is persisted,
	// and is initialized to 0 on first boot."
	log := newRaftLog(store, a)
	latestTerm := log.lastTerm()

	s := &Server{
		id:      id,
		state:   &protectedString{value: follower}, // "when servers start up they begin as followers"
		running: &protectedBool{value: false},
		leader:  unknownLeader, // unknown at startup
		log:     log,
		term:    latestTerm,
		config:  newConfiguration(peerMap{}),

		appendEntriesChan: make(chan appendEntriesTuple),
		requestVoteChan:   make(chan requestVoteTuple),
		commandChan:       make(chan commandTuple),
		configurationChan: make(chan configurationTuple),

		electionTick: nil,
		quit:         make(chan chan struct{}),
	}
	s.resetElectionTimeout()
	return s
}

type configurationTuple struct {
	Peers []Peer
	Err   chan error
}

// SetConfiguration sets the peers that this server will attempt to communicate
// with. The set peers should include a peer that represents this server.
// SetConfiguration must be called before starting the server. Calls to
// SetConfiguration after the server has been started will be replicated
// throughout the Raft network using the joint-consensus mechanism.
//
// TODO we need to refactor how we parse entries: a single code path from any
// source (snapshot, persisted log at startup, or over the network) into the
// log, and as part of that flow, checking if the entry is a configuration and
// emitting it to the configuration structure. This implies an unfortunate
// coupling: whatever processes log entries must have both the configuration
// and the log as data sinks.
func (s *Server) SetConfiguration(peers ...Peer) error {
	if !s.running.Get() {
		s.config.directSet(makePeerMap(peers...))
		return nil
	}

	err := make(chan error)
	s.configurationChan <- configurationTuple{peers, err}
	return <-err
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
	Command         []byte
	CommandResponse chan<- []byte
	Err             chan error
}

// Command appends the passed command to the leader log. If error is nil, the
// command will eventually get replicated throughout the Raft network. When the
// command gets committed to the local server log, it's passed to the apply
// function, and the response from that function is provided on the
// passed response chan.
func (s *Server) Command(cmd []byte, response chan<- []byte) error {
	err := make(chan error)
	s.commandChan <- commandTuple{cmd, response, err}
	return <-err
}

// appendEntries processes the given RPC and returns the response.
func (s *Server) appendEntries(ae appendEntries) appendEntriesResponse {
	t := appendEntriesTuple{
		Request:  ae,
		Response: make(chan appendEntriesResponse),
	}
	s.appendEntriesChan <- t
	return <-t.Response
}

// requestVote processes the given RPC and returns the response.
func (s *Server) requestVote(rv requestVote) requestVoteResponse {
	t := requestVoteTuple{
		Request:  rv,
		Response: make(chan requestVoteResponse),
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
		switch state := s.state.Get(); state {
		case follower:
			s.followerSelect()
		case candidate:
			s.candidateSelect()
		case leader:
			s.leaderSelect()
		default:
			panic(fmt.Sprintf("unknown Server State '%s'", state))
		}
	}
}

func (s *Server) resetElectionTimeout() {
	s.electionTick = time.NewTimer(electionTimeout()).C
}

func (s *Server) logGeneric(format string, args ...interface{}) {
	prefix := fmt.Sprintf("id=%d term=%d state=%s: ", s.id, s.term, s.state.Get())
	log.Printf(prefix+format, args...)
}

func (s *Server) logAppendEntriesResponse(req appendEntries, resp appendEntriesResponse, stepDown bool) {
	s.logGeneric(
		"got appendEntries, sz=%d leader=%d prevIndex/Term=%d/%d commitIndex=%d: responded with success=%v (reason='%s') stepDown=%v",
		len(req.Entries),
		req.LeaderID,
		req.PrevLogIndex,
		req.PrevLogTerm,
		req.CommitIndex,
		resp.Success,
		resp.reason,
		stepDown,
	)
}
func (s *Server) logRequestVoteResponse(req requestVote, resp requestVoteResponse, stepDown bool) {
	s.logGeneric(
		"got RequestVote, candidate=%d: responded with granted=%v (reason='%s') stepDown=%v",
		req.CandidateID,
		resp.VoteGranted,
		resp.reason,
		stepDown,
	)
}

func (s *Server) handleQuit(q chan struct{}) {
	s.logGeneric("got quit signal")
	s.running.Set(false)
	close(q)
}

func (s *Server) forwardCommand(t commandTuple) {
	switch s.leader {
	case unknownLeader:
		s.logGeneric("got command, but don't know leader")
		t.Err <- errUnknownLeader

	case s.id: // I am the leader
		panic("impossible state in forwardCommand")

	default:
		leader, ok := s.config.get(s.leader)
		if !ok {
			panic("invalid state in peers")
		}
		s.logGeneric("got command, forwarding to leader (%d)", s.leader)
		// We're blocking our {follower,candidate}Select function in the
		// receive-command branch. If we continue to block while forwarding
		// the command, the leader won't be able to get a response from us!
		go func() { t.Err <- leader.callCommand(t.Command, t.CommandResponse) }()
	}
}

func (s *Server) forwardConfiguration(t configurationTuple) {
	switch s.leader {
	case unknownLeader:
		s.logGeneric("got configuration, but don't know leader")
		t.Err <- errUnknownLeader

	case s.id: // I am the leader
		panic("impossible state in forwardConfiguration")

	default:
		leader, ok := s.config.get(s.leader)
		if !ok {
			panic("invalid state in peers")
		}
		s.logGeneric("got configuration, forwarding to leader (%d)", s.leader)
		go func() { t.Err <- leader.callSetConfiguration(t.Peers...) }()
	}
}

func (s *Server) followerSelect() {
	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return

		case t := <-s.commandChan:
			s.forwardCommand(t)

		case t := <-s.configurationChan:
			s.forwardConfiguration(t)

		case <-s.electionTick:
			// 5.2 Leader election: "A follower increments its current term and
			// transitions to candidate state."
			if s.config == nil {
				s.logGeneric("election timeout, but no configuration: ignoring")
				s.resetElectionTimeout()
				continue
			}
			s.logGeneric("election timeout, becoming candidate")
			s.term++
			s.vote = noVote
			s.leader = unknownLeader
			s.state.Set(candidate)
			s.resetElectionTimeout()
			return

		case t := <-s.appendEntriesChan:
			if s.leader == unknownLeader {
				s.leader = t.Request.LeaderID
				s.logGeneric("discovered Leader %d", s.leader)
			}
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				// stepDown as a Follower means just to reset the leader
				if s.leader != unknownLeader {
					s.logGeneric("abandoning old leader=%d", s.leader)
				}
				s.logGeneric("following new leader=%d", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				// stepDown as a Follower means just to reset the leader
				if s.leader != unknownLeader {
					s.logGeneric("abandoning old leader=%d", s.leader)
				}
				s.logGeneric("new leader unknown")
				s.leader = unknownLeader
			}
		}
	}
}

func (s *Server) candidateSelect() {
	if s.leader != unknownLeader {
		panic("known leader when entering candidateSelect")
	}
	if s.vote != 0 {
		panic("existing vote when entering candidateSelect")
	}

	// "[A server entering the candidate stage] issues requestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	requestVoteResponses, canceler := s.config.allPeers().except(s.id).requestVotes(requestVote{
		Term:         s.term,
		CandidateID:  s.id,
		LastLogIndex: s.log.lastIndex(),
		LastLogTerm:  s.log.lastTerm(),
	})
	defer canceler.Cancel()

	// Set up vote tallies (plus, vote for myself)
	votes := map[uint64]bool{s.id: true}
	s.vote = s.id
	s.logGeneric("term=%d election started (configuration state %s)", s.term, s.config.state)

	// catch a weird state
	if s.config.pass(votes) {
		s.logGeneric("I immediately won the election")
		s.leader = s.id
		s.state.Set(leader)
		s.vote = noVote
		return
	}

	// "A candidate continues in this state until one of three things happens:
	// (a) it wins the election, (b) another server establishes itself as
	// leader, or (c) a period of time goes by with no winner."
	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return

		case t := <-s.commandChan:
			s.forwardCommand(t)

		case t := <-s.configurationChan:
			s.forwardConfiguration(t)

		case t := <-requestVoteResponses:
			s.logGeneric("got vote: id=%d term=%d granted=%v", t.id, t.response.Term, t.response.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if t.response.Term > s.term {
				s.logGeneric("got vote from future term (%d>%d); abandoning election", t.response.Term, s.term)
				s.leader = unknownLeader
				s.state.Set(follower)
				s.vote = noVote
				return // lose
			}
			if t.response.Term < s.term {
				s.logGeneric("got vote from past term (%d<%d); ignoring", t.response.Term, s.term)
				break
			}
			if t.response.VoteGranted {
				s.logGeneric("%d voted for me", t.id)
				votes[t.id] = true
			}
			// "Once a candidate wins an election, it becomes leader."
			if s.config.pass(votes) {
				s.logGeneric("I won the election")
				s.leader = s.id
				s.state.Set(leader)
				s.vote = noVote
				return // win
			}

		case t := <-s.appendEntriesChan:
			// "While waiting for votes, a candidate may receive an
			// appendEntries RPC from another server claiming to be leader.
			// If the leader's term (included in its RPC) is at least as
			// large as the candidate's current term, then the candidate
			// recognizes the leader as legitimate and steps down, meaning
			// that it returns to follower state."
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after an appendEntries, stepping down to Follower (leader=%d)", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
				s.state.Set(follower)
				return // lose
			}

		case t := <-s.requestVoteChan:
			// We can also be defeated by a more recent candidate
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after a requestVote, stepping down to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(follower)
				return // lose
			}

		case <-s.electionTick:
			// "The third possible outcome is that a candidate neither wins nor
			// loses the election: if many followers become candidates at the
			// same time, votes could be split so that no candidate obtains a
			// majority. When this happens, each candidate will start a new
			// election by incrementing its term and initiating another round of
			// requestVote RPCs."
			s.logGeneric("election ended with no winner; incrementing term and trying again")
			s.resetElectionTimeout()
			s.term++
			s.vote = noVote
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

func newNextIndex(pm peerMap, defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for id := range pm {
		ni.m[id] = defaultNextIndex
	}
	return ni
}

func (ni *nextIndex) bestIndex() uint64 {
	ni.RLock()
	defer ni.RUnlock()

	if len(ni.m) <= 0 {
		return 0
	}

	i := uint64(math.MaxUint64)
	for _, nextIndex := range ni.m {
		if nextIndex < i {
			i = nextIndex
		}
	}
	return i
}

func (ni *nextIndex) prevLogIndex(id uint64) uint64 {
	ni.RLock()
	defer ni.RUnlock()
	if _, ok := ni.m[id]; !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	return ni.m[id]
}

func (ni *nextIndex) decrement(id uint64, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}

	if i != prev {
		return i, errOutOfSync
	}

	if i > 0 {
		ni.m[id]--
	}
	return ni.m[id], nil
}

func (ni *nextIndex) set(id, index, prev uint64) (uint64, error) {
	ni.Lock()
	defer ni.Unlock()

	i, ok := ni.m[id]
	if !ok {
		panic(fmt.Sprintf("peer %d not found", id))
	}
	if i != prev {
		return i, errOutOfSync
	}

	ni.m[id] = index
	return index, nil
}

// flush generates and forwards an appendEntries request that attempts to bring
// the given follower "in sync" with our log. It's idempotent, so it's used for
// both heartbeats and replicating commands.
//
// The appendEntries request we build represents our best attempt at a "delta"
// between our log and the follower's log. The passed nextIndex structure
// manages that state.
//
// flush is synchronous and can block forever if the peer is nonresponsive.
func (s *Server) flush(peer Peer, ni *nextIndex) error {
	peerID := peer.id()
	currentTerm := s.term
	prevLogIndex := ni.prevLogIndex(peerID)
	entries, prevLogTerm := s.log.entriesAfter(prevLogIndex)
	commitIndex := s.log.getCommitIndex()
	s.logGeneric("flush to %d: term=%d leaderId=%d prevLogIndex/Term=%d/%d sz=%d commitIndex=%d", peerID, currentTerm, s.id, prevLogIndex, prevLogTerm, len(entries), commitIndex)
	resp := peer.callAppendEntries(appendEntries{
		Term:         currentTerm,
		LeaderID:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		CommitIndex:  commitIndex,
	})

	if resp.Term > currentTerm {
		s.logGeneric("flush to %d: responseTerm=%d > currentTerm=%d: deposed", peerID, resp.Term, currentTerm)
		return errDeposed
	}

	// It's possible the leader has timed out waiting for us, and moved on.
	// So we should be careful, here, to make only valid state changes to `ni`.

	if !resp.Success {
		newPrevLogIndex, err := ni.decrement(peerID, prevLogIndex)
		if err != nil {
			s.logGeneric("flush to %d: while decrementing prevLogIndex: %s", peerID, err)
			return err
		}
		s.logGeneric("flush to %d: rejected; prevLogIndex(%d) becomes %d", peerID, peerID, newPrevLogIndex)
		return errAppendEntriesRejected
	}

	if len(entries) > 0 {
		newPrevLogIndex, err := ni.set(peer.id(), entries[len(entries)-1].Index, prevLogIndex)
		if err != nil {
			s.logGeneric("flush to %d: while moving prevLogIndex forward: %s", peerID, err)
			return err
		}
		s.logGeneric("flush to %d: accepted; prevLogIndex(%d) becomes %d", peerID, peerID, newPrevLogIndex)
		return nil
	}

	s.logGeneric("flush to %d: accepted; prevLogIndex(%d) remains %d", peerID, peerID, ni.prevLogIndex(peerID))
	return nil
}

// concurrentFlush triggers a concurrent flush to each of the peers. All peers
// must respond (or timeout) before concurrentFlush will return. timeout is per
// peer.
func (s *Server) concurrentFlush(pm peerMap, ni *nextIndex, timeout time.Duration) (int, bool) {
	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(pm))
	for _, peer := range pm {
		go func(peer Peer) {
			errChan := make(chan error, 1)
			go func() { errChan <- s.flush(peer, ni) }()
			go func() { time.Sleep(timeout); errChan <- errTimeout }()
			responses <- tuple{peer.id(), <-errChan} // first responder wins
		}(peer)
	}

	successes, stepDown := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			s.logGeneric("concurrentFlush: peer %d: OK (prevLogIndex(%d)=%d)", t.id, t.id, ni.prevLogIndex(t.id))
			successes++
		case errDeposed:
			s.logGeneric("concurrentFlush: peer %d: deposed!", t.id)
			stepDown = true
		default:
			s.logGeneric("concurrentFlush: peer %d: %s (prevLogIndex(%d)=%d)", t.id, t.err, t.id, ni.prevLogIndex(t.id))
			// nothing to do but log and continue
		}
	}
	return successes, stepDown
}

func (s *Server) leaderSelect() {
	if s.leader != s.id {
		panic(fmt.Sprintf("leader (%d) not me (%d) when entering leaderSelect", s.leader, s.id))
	}
	if s.vote != 0 {
		panic(fmt.Sprintf("vote (%d) not zero when entering leaderSelect", s.leader))
	}

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
	ni := newNextIndex(s.config.allPeers().except(s.id), s.log.lastIndex()) // +1)

	flush := make(chan struct{})
	heartbeat := time.NewTicker(broadcastInterval())
	defer heartbeat.Stop()
	go func() {
		for _ = range heartbeat.C {
			flush <- struct{}{}
		}
	}()

	for {
		select {
		case q := <-s.quit:
			s.handleQuit(q)
			return

		case t := <-s.commandChan:
			// Append the command to our (leader) log
			s.logGeneric("got command, appending")
			currentTerm := s.term
			entry := logEntry{
				Index:           s.log.lastIndex() + 1,
				Term:            currentTerm,
				Command:         t.Command,
				commandResponse: t.CommandResponse,
			}
			if err := s.log.appendEntry(entry); err != nil {
				t.Err <- err
				continue
			}
			s.logGeneric(
				"after append, commitIndex=%d lastIndex=%d lastTerm=%d",
				s.log.getCommitIndex(),
				s.log.lastIndex(),
				s.log.lastTerm(),
			)

			// Now that the entry is in the log, we can fall back to the
			// normal flushing mechanism to attempt to replicate the entry
			// and advance the commit index. We trigger a manual flush as a
			// convenience, so our caller might get a response a bit sooner.
			go func() { flush <- struct{}{} }()
			t.Err <- nil

		case t := <-s.configurationChan:
			// Attempt to change our local configuration
			if err := s.config.changeTo(makePeerMap(t.Peers...)); err != nil {
				t.Err <- err
				continue
			}

			// Serialize the local (C_old,new) configuration
			encodedConfiguration, err := s.config.encode()
			if err != nil {
				t.Err <- err
				continue
			}

			// We're gonna write+replicate that config via log mechanisms.
			// Prepare the on-commit callback.
			entry := logEntry{
				Index:           s.log.lastIndex() + 1,
				Term:            s.term,
				Command:         encodedConfiguration,
				isConfiguration: true,
				committed:       make(chan bool),
			}
			go func() {
				committed := <-entry.committed
				if !committed {
					s.config.changeAborted()
					return
				}
				s.config.changeCommitted()
				if _, ok := s.config.allPeers()[s.id]; !ok {
					s.logGeneric("leader expelled; shutting down")
					q := make(chan struct{})
					s.quit <- q
					<-q
				}
			}()
			if err := s.log.appendEntry(entry); err != nil {
				t.Err <- err
				continue
			}

		case <-flush:
			// Flushes attempt to sync the follower log with ours.
			// That requires per-follower state in the form of nextIndex.
			// After every flush, we check if we can advance our commitIndex.
			// If so, we do it, and trigger another flush ASAP.
			// A flush can cause us to be deposed.
			recipients := s.config.allPeers().except(s.id)

			// Special case: network of 1
			if len(recipients) <= 0 {
				ourLastIndex := s.log.lastIndex()
				if ourLastIndex > 0 {
					if err := s.log.commitTo(ourLastIndex); err != nil {
						s.logGeneric("commitTo(%d): %s", ourLastIndex, err)
						continue
					}
					s.logGeneric("after commitTo(%d), commitIndex=%d", ourLastIndex, s.log.getCommitIndex())
				}
				continue
			}

			// Normal case: network of at-least-2
			successes, stepDown := s.concurrentFlush(recipients, ni, 2*broadcastInterval())
			if stepDown {
				s.logGeneric("deposed during flush")
				s.state.Set(follower)
				s.leader = unknownLeader
				return
			}

			// Only when we know all followers accepted the flush can we
			// consider incrementing commitIndex and pushing out another
			// round of flushes.
			if successes == len(recipients) {
				peersBestIndex := ni.bestIndex()
				ourLastIndex := s.log.lastIndex()
				ourCommitIndex := s.log.getCommitIndex()
				if peersBestIndex > ourLastIndex {
					// safety check: we've probably been deposed
					s.logGeneric("peers' best index %d > our lastIndex %d", peersBestIndex, ourLastIndex)
					s.logGeneric("this is crazy, I'm gonna become a follower")
					s.leader = unknownLeader
					s.vote = noVote
					s.state.Set(follower)
					return
				}
				if peersBestIndex > ourCommitIndex {
					if err := s.log.commitTo(peersBestIndex); err != nil {
						s.logGeneric("commitTo(%d): %s", peersBestIndex, err)
						continue // oh well, next time?
					}
					if s.log.getCommitIndex() > ourCommitIndex {
						s.logGeneric("after commitTo(%d), commitIndex=%d -- queueing another flush", peersBestIndex, s.log.getCommitIndex())
						go func() { flush <- struct{}{} }()
					}
				}
			}

		case t := <-s.appendEntriesChan:
			resp, stepDown := s.handleAppendEntries(t.Request)
			s.logAppendEntriesResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after an appendEntries, deposed to Follower (leader=%d)", t.Request.LeaderID)
				s.leader = t.Request.LeaderID
				s.state.Set(follower)
				return // deposed
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after a requestVote, deposed to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(follower)
				return // deposed
			}
		}
	}
}

// handleRequestVote will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=unknownLeader, s.state.Set(Follower).
func (s *Server) handleRequestVote(rv requestVote) (requestVoteResponse, bool) {
	// Spec is ambiguous here; basing this (loosely!) on benbjohnson's impl

	// If the request is from an old term, reject
	if rv.Term < s.term {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("Term %d < %d", rv.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if rv.Term > s.term {
		s.logGeneric("requestVote from newer term (%d): we defer", rv.Term)
		s.term = rv.Term
		s.vote = noVote
		s.leader = unknownLeader
		stepDown = true
	}

	// Special case: if we're the leader, and we haven't been deposed by a more
	// recent term, then we should always deny the vote
	if s.state.Get() == leader && !stepDown {
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      "already the leader",
		}, stepDown
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != rv.CandidateID {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
		return requestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      fmt.Sprintf("already cast vote for %d", s.vote),
		}, stepDown
	}

	// If the candidate log isn't at least as recent as ours, reject
	if s.log.lastIndex() > rv.LastLogIndex || s.log.lastTerm() > rv.LastLogTerm {
		return requestVoteResponse{
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
	s.vote = rv.CandidateID
	s.resetElectionTimeout()
	return requestVoteResponse{
		Term:        s.term,
		VoteGranted: true,
	}, stepDown
}

// handleAppendEntries will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=r.LeaderID, s.state.Set(Follower).
func (s *Server) handleAppendEntries(r appendEntries) (appendEntriesResponse, bool) {
	// Spec is ambiguous here; basing this on benbjohnson's impl

	// Maybe a nicer way to handle this is to define explicit handler functions
	// for each Server state. Then, we won't try to hide too much logic (i.e.
	// too many protocol rules) in one code path.

	// If the request is from an old term, reject
	if r.Term < s.term {
		return appendEntriesResponse{
			Term:    s.term,
			Success: false,
			reason:  fmt.Sprintf("Term %d < %d", r.Term, s.term),
		}, false
	}

	// If the request is from a newer term, reset our state
	stepDown := false
	if r.Term > s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an appendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if s.state.Get() == candidate && r.LeaderID != s.leader && r.Term >= s.term {
		s.term = r.Term
		s.vote = noVote
		stepDown = true
	}

	// In any case, reset our election timeout
	s.resetElectionTimeout()

	// Reject if log doesn't contain a matching previous entry
	if err := s.log.ensureLastIs(r.PrevLogIndex, r.PrevLogTerm); err != nil {
		return appendEntriesResponse{
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

	// Process the entries
	for i, entry := range r.Entries {
		// Configuration changes requre special preprocessing
		var pm peerMap
		if entry.isConfiguration {
			commandBuf := bytes.NewBuffer(entry.Command)
			if err := gob.NewDecoder(commandBuf).Decode(&pm); err != nil {
				panic("gob decode of peers failed")
			}

			if s.state.Get() == leader {
				// TODO should we instead just ignore this entry?
				return appendEntriesResponse{
					Term:    s.term,
					Success: false,
					reason: fmt.Sprintf(
						"AppendEntry %d/%d failed (configuration): %s",
						i+1,
						len(r.Entries),
						"Leader shouldn't receive configurations via appendEntries",
					),
				}, stepDown
			}

			// Expulsion recognition
			if _, ok := pm[s.id]; !ok {
				entry.committed = make(chan bool)
				go func() {
					if <-entry.committed {
						s.logGeneric("non-leader expelled; shutting down")
						q := make(chan struct{})
						s.quit <- q
						<-q
					}
				}()
			}
		}

		// Append entry to the log
		if err := s.log.appendEntry(entry); err != nil {
			return appendEntriesResponse{
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

		// "Once a given server adds the new configuration entry to its log, it
		// uses that configuration for all future decisions (it does not wait
		// for the entry to become committed)."
		if entry.isConfiguration {
			if err := s.config.directSet(pm); err != nil {
				return appendEntriesResponse{
					Term:    s.term,
					Success: false,
					reason: fmt.Sprintf(
						"AppendEntry %d/%d failed (configuration): %s",
						i+1,
						len(r.Entries),
						err,
					),
				}, stepDown
			}
		}
	}

	// Commit up to the commit index.
	//
	// < ptrb> ongardie: if the new leader sends a 0-entry appendEntries
	//  with lastIndex=5 commitIndex=4, to a follower that has lastIndex=5
	//  commitIndex=5 -- in my impl, this fails, because commitIndex is too
	//  small. shouldn't be?
	// <@ongardie> ptrb: i don't think that should fail
	// <@ongardie> there are 4 ways an appendEntries request can fail: (1)
	//  network drops packet (2) caller has stale term (3) would leave gap in
	//  the recipient's log (4) term of entry preceding the new entries doesn't
	//  match the term at the same index on the recipient
	//
	if r.CommitIndex > 0 && r.CommitIndex > s.log.getCommitIndex() {
		if err := s.log.commitTo(r.CommitIndex); err != nil {
			return appendEntriesResponse{
				Term:    s.term,
				Success: false,
				reason:  fmt.Sprintf("CommitTo(%d) failed: %s", r.CommitIndex, err),
			}, stepDown
		}
	}

	// all good
	return appendEntriesResponse{
		Term:    s.term,
		Success: true,
	}, stepDown
}
