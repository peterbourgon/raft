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
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"
)

const (
	unknownLeader = 0
	noVote        = 0
)

var (
	minimumElectionTimeoutMs int32 = 250
	maximumElectionTimeoutMs       = 2 * minimumElectionTimeoutMs
)

var (
	ErrNotLeader             = errors.New("not the leader")
	ErrUnknownLeader         = errors.New("unknown leader")
	ErrDeposed               = errors.New("deposed during replication")
	ErrAppendEntriesRejected = errors.New("AppendEntries RPC rejected")
	ErrReplicationFailed     = errors.New("command replication failed (but will keep retrying)")
	ErrOutOfSync             = errors.New("out of sync")
	ErrAlreadyRunning        = errors.New("already running")
)

// ResetElectionTimeoutMs sets the minimum and maximum election timeouts to the
// passed values, and returns the old values.
func ResetElectionTimeoutMs(newMin, newMax int) (int, int) {
	oldMin := atomic.LoadInt32(&minimumElectionTimeoutMs)
	oldMax := atomic.LoadInt32(&maximumElectionTimeoutMs)
	atomic.StoreInt32(&minimumElectionTimeoutMs, int32(newMin))
	atomic.StoreInt32(&maximumElectionTimeoutMs, int32(newMax))
	return int(oldMin), int(oldMax)
}

// MinimumElectionTimeout returns the current minimum election timeout.
// This is an exported function primarily to help in testing.
func MinimumElectionTimeout() time.Duration {
	return time.Duration(minimumElectionTimeoutMs) * time.Millisecond
}

// MaximumElectionTimeout returns the current maximum election time.
// This is an exported function primarily to help in testing.
func MaximumElectionTimeout() time.Duration {
	return time.Duration(maximumElectionTimeoutMs) * time.Millisecond
}

// ElectionTimeout returns a variable time.Duration, between the minimum and
// maximum election timeouts.
func ElectionTimeout() time.Duration {
	n := rand.Intn(int(maximumElectionTimeoutMs - minimumElectionTimeoutMs))
	d := int(minimumElectionTimeoutMs) + n
	return time.Duration(d) * time.Millisecond
}

// broadcastInterval returns the interval between heartbeats (AppendEntry RPCs)
// broadcast from the leader. It is the minimum election timeout / 10, as
// dictated by the spec: BroadcastInterval << ElectionTimeout << MTBF.
func broadcastInterval() time.Duration {
	d := minimumElectionTimeoutMs / 10
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

// ApplyFunc is called whenever a new command should be applied to the local
// client's state machine. commitIndex is guaranteed to be gapless and
// monotonically increasing, but duplicates may occur.
type ApplyFunc func(commitIndex uint64, cmd []byte) []byte

// NewServer returns an initialized, un-started server. The ID must be unique in
// the Raft network, and greater than 0. The store will be used by the
// distributed log as a persistence layer. The apply function will be called
// whenever a (user-domain) command has been safely replicated to this server,
// and can be considered committed.
func NewServer(id uint64, store io.ReadWriter, a ApplyFunc) *Server {
	if id <= 0 {
		panic("server id must be > 0")
	}

	// 5.2 Leader election: "the latest term this server has seen is persisted,
	// and is initialized to 0 on first boot.""
	log := newRaftLog(store, a)
	latestTerm := log.lastTerm()

	s := &Server{
		id:      id,
		state:   &protectedString{value: Follower}, // "when servers start up they begin as followers"
		running: &protectedBool{value: false},
		leader:  unknownLeader, // unknown at startup
		log:     log,
		term:    latestTerm,
		config:  newConfiguration(Peers{}),

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

// Id returns the ID of the server.
func (s *Server) Id() uint64 { return s.id }

type configurationTuple struct {
	Peers Peers
	Err   chan error
}

// SetConfiguration sets the peers that this server will attempt to communicate
// with. The set peers should include a peer that represents this server, so
// that quorum is calculated correctly. SetConfiguration must be called before
// starting the server.
func (s *Server) SetConfiguration(peers Peers) error {
	// Pre-start SetConfiguration are special cased to simply set and return
	if !s.running.Get() {
		s.config.directSet(peers)
		return nil
	}

	// Post-start SetConfiguration require communication with the leader
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
	CommandResponse chan []byte
	Err             chan error
}

// Command appends the passed command to the leader log. If error is nil, the
// command will eventually get replicated throughout the Raft network. When the
// command gets committed to the local server log, it's passed to the apply
// function, and the response from that function is provided on the
// passed response chan.
func (s *Server) command(cmd []byte, response chan []byte) error {
	err := make(chan error)
	s.commandChan <- commandTuple{cmd, response, err}
	return <-err
}

// AppendEntries processes the given RPC and returns the response.
func (s *Server) appendEntries(ae AppendEntries) AppendEntriesResponse {
	t := appendEntriesTuple{
		Request:  ae,
		Response: make(chan AppendEntriesResponse),
	}
	s.appendEntriesChan <- t
	return <-t.Response
}

// RequestVote processes the given RPC and returns the response.
func (s *Server) requestVote(rv RequestVote) RequestVoteResponse {
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
		switch state := s.state.Get(); state {
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
	prefix := fmt.Sprintf("id=%d term=%d state=%s: ", s.id, s.term, s.state.Get())
	log.Printf(prefix+format, args...)
}

func (s *Server) logAppendEntriesResponse(req AppendEntries, resp AppendEntriesResponse, stepDown bool) {
	s.logGeneric(
		"got AppendEntries, sz=%d leader=%d prevIndex/Term=%d/%d commitIndex=%d: responded with success=%v (%s) stepDown=%v",
		len(req.Entries),
		req.LeaderId,
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

func (s *Server) handleQuit(q chan struct{}) {
	s.logGeneric("got quit signal")
	s.running.Set(false)
	close(q)
}

func (s *Server) forwardCommand(t commandTuple) {
	switch s.leader {
	case unknownLeader:
		s.logGeneric("got command, but don't know leader")
		t.Err <- ErrUnknownLeader

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
		go func() { t.Err <- leader.Command(t.Command, t.CommandResponse) }()
	}
}

func (s *Server) forwardConfiguration(t configurationTuple) {
	switch s.leader {
	case unknownLeader:
		s.logGeneric("got configuration, but don't know leader")
		t.Err <- ErrUnknownLeader

	case s.id: // I am the leader
		panic("impossible state in forwardConfiguration")

	default:
		leader, ok := s.config.get(s.leader)
		if !ok {
			panic("invalid state in peers")
		}
		s.logGeneric("got configuration, forwarding to leader (%d)", s.leader)
		go func() { t.Err <- leader.SetConfiguration(t.Peers) }()
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
			s.state.Set(Candidate)
			s.resetElectionTimeout()
			return

		case t := <-s.appendEntriesChan:
			if s.leader == unknownLeader {
				s.leader = t.Request.LeaderId
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
				s.logGeneric("following new leader=%d", t.Request.LeaderId)
				s.leader = t.Request.LeaderId
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

	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	tuples, canceler := s.config.allPeers().except(s.id).requestVotes(RequestVote{
		Term:         s.term,
		CandidateId:  s.id,
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
		s.state.Set(Leader)
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

		case t := <-tuples:
			s.logGeneric("got vote: id=%d term=%d granted=%v", t.id, t.rvr.Term, t.rvr.VoteGranted)
			// "A candidate wins the election if it receives votes from a
			// majority of servers in the full cluster for the same term."
			if t.rvr.Term > s.term {
				s.logGeneric("got vote from future term (%d>%d); abandoning election", t.rvr.Term, s.term)
				s.leader = unknownLeader
				s.state.Set(Follower)
				s.vote = noVote
				return // lose
			}
			if t.rvr.Term < s.term {
				s.logGeneric("got vote from past term (%d<%d); ignoring", t.rvr.Term, s.term)
				break
			}
			if t.rvr.VoteGranted {
				s.logGeneric("%d voted for me", t.id)
				votes[t.id] = true
			}
			// "Once a candidate wins an election, it becomes leader."
			if s.config.pass(votes) {
				s.logGeneric("I won the election")
				s.leader = s.id
				s.state.Set(Leader)
				s.vote = noVote
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
				s.logGeneric("after an AppendEntries, stepping down to Follower (leader=%d)", t.Request.LeaderId)
				s.leader = t.Request.LeaderId
				s.state.Set(Follower)
				return // lose
			}

		case t := <-s.requestVoteChan:
			// We can also be defeated by a more recent candidate
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after a RequestVote, stepping down to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(Follower)
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

func newNextIndex(peers Peers, defaultNextIndex uint64) *nextIndex {
	ni := &nextIndex{
		m: map[uint64]uint64{},
	}
	for id, _ := range peers {
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

	var i uint64 = math.MaxUint64
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
		return i, ErrOutOfSync
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
		return i, ErrOutOfSync
	}

	ni.m[id] = index
	return index, nil
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
	prevLogIndex := ni.prevLogIndex(peerId)
	entries, prevLogTerm := s.log.entriesAfter(prevLogIndex)
	commitIndex := s.log.getCommitIndex()
	s.logGeneric("flush to %d: term=%d leaderId=%d prevLogIndex/Term=%d/%d sz=%d commitIndex=%d", peerId, currentTerm, s.id, prevLogIndex, prevLogTerm, len(entries), commitIndex)
	resp := peer.AppendEntries(AppendEntries{
		Term:         currentTerm,
		LeaderId:     s.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		CommitIndex:  commitIndex,
	})

	if resp.Term > currentTerm {
		s.logGeneric("flush to %d: responseTerm=%d > currentTerm=%d: deposed", peerId, resp.Term, currentTerm)
		return ErrDeposed
	}

	// It's possible the leader has timed out waiting for us, and moved on.
	// So we should be careful, here, to make only valid state changes to `ni`.

	if !resp.Success {
		newPrevLogIndex, err := ni.decrement(peerId, prevLogIndex)
		if err != nil {
			s.logGeneric("flush to %d: while decrementing prevLogIndex: %s", peerId, err)
			return err
		}
		s.logGeneric("flush to %d: rejected; prevLogIndex(%d) becomes %d", peerId, peerId, newPrevLogIndex)
		return ErrAppendEntriesRejected
	}

	if len(entries) > 0 {
		newPrevLogIndex, err := ni.set(peer.Id(), entries[len(entries)-1].Index, prevLogIndex)
		if err != nil {
			s.logGeneric("flush to %d: while moving prevLogIndex forward: %s", peerId, err)
			return err
		}
		s.logGeneric("flush to %d: accepted; prevLogIndex(%d) becomes %d", peerId, peerId, newPrevLogIndex)
		return nil
	}

	s.logGeneric("flush to %d: accepted; prevLogIndex(%d) remains %d", peerId, peerId, ni.prevLogIndex(peerId))
	return nil
}

// concurrentFlush triggers a concurrent flush to each of the peers. All peers
// must respond (or timeout) before concurrentFlush will return. timeout is per
// peer.
func (s *Server) concurrentFlush(peers Peers, ni *nextIndex, timeout time.Duration) (int, bool) {
	type tuple struct {
		id  uint64
		err error
	}
	responses := make(chan tuple, len(peers))
	for _, peer := range peers {
		go func(peer0 Peer) {
			err0 := make(chan error, 1)
			go func() { err0 <- s.flush(peer0, ni) }()
			go func() { time.Sleep(timeout); err0 <- ErrTimeout }()
			responses <- tuple{peer0.Id(), <-err0} // first responder wins
		}(peer)
	}

	successes, stepDown := 0, false
	for i := 0; i < cap(responses); i++ {
		switch t := <-responses; t.err {
		case nil:
			s.logGeneric("concurrentFlush: peer %d: OK (prevLogIndex(%d)=%d)", t.id, t.id, ni.prevLogIndex(t.id))
			successes++
		case ErrDeposed:
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
		panic(fmt.Sprintf("vote (%d) not zero when entering leaderSelect", s.leader, s.id))
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
			if err := s.config.changeTo(t.Peers); err != nil {
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
				if _, ok := s.config.allPeers()[s.Id()]; !ok {
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
				s.state.Set(Follower)
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
					s.state.Set(Follower)
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
				s.logGeneric("after an AppendEntries, deposed to Follower (leader=%d)", s.leader)
				s.leader = t.Request.LeaderId
				s.state.Set(Follower)
				return // deposed
			}

		case t := <-s.requestVoteChan:
			resp, stepDown := s.handleRequestVote(t.Request)
			s.logRequestVoteResponse(t.Request, resp, stepDown)
			t.Response <- resp
			if stepDown {
				s.logGeneric("after a RequestVote, deposed to Follower (leader unknown)")
				s.leader = unknownLeader
				s.state.Set(Follower)
				return // deposed
			}
		}
	}
}

// handleRequestVote will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=unknownLeader, s.state.Set(Follower).
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
		s.logGeneric("RequestVote from newer term (%d): we defer", rv.Term)
		s.term = rv.Term
		s.vote = noVote
		s.leader = unknownLeader
		stepDown = true
	}

	// Special case: if we're the leader, and we haven't been deposed by a more
	// recent term, then we should always deny the vote
	if s.state.Get() == Leader && !stepDown {
		return RequestVoteResponse{
			Term:        s.term,
			VoteGranted: false,
			reason:      "already the leader",
		}, stepDown
	}

	// If we've already voted for someone else this term, reject
	if s.vote != 0 && s.vote != rv.CandidateId {
		if stepDown {
			panic("impossible state in handleRequestVote")
		}
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

// handleAppendEntries will modify s.term and s.vote, but nothing else.
// stepDown means you need to: s.leader=r.LeaderId, s.state.Set(Follower).
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
		s.vote = noVote
		stepDown = true
	}

	// Special case for candidates: "While waiting for votes, a candidate may
	// receive an AppendEntries RPC from another server claiming to be leader.
	// If the leader’s term (included in its RPC) is at least as large as the
	// candidate’s current term, then the candidate recognizes the leader as
	// legitimate and steps down, meaning that it returns to follower state."
	if s.state.Get() == Candidate && r.LeaderId != s.leader && r.Term >= s.term {
		s.term = r.Term
		s.vote = noVote
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

	// Process the entries
	for i, entry := range r.Entries {
		// Configuration changes requre special preprocessing
		var peers Peers
		if entry.isConfiguration {
			commandBuf := bytes.NewBuffer(entry.Command)
			if err := gob.NewDecoder(commandBuf).Decode(&peers); err != nil {
				panic("gob decode of peers failed")
			}

			if s.state.Get() == Leader {
				// TODO should we instead just ignore this entry?
				return AppendEntriesResponse{
					Term:    s.term,
					Success: false,
					reason: fmt.Sprintf(
						"AppendEntry %d/%d failed (configuration): %s",
						i+1,
						len(r.Entries),
						"Leader shouldn't receive configurations via AppendEntries",
					),
				}, stepDown
			}

			// Expulsion recognition
			if _, ok := peers[s.Id()]; !ok {
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

		// "Once a given server adds the new configuration entry to its log, it
		// uses that configuration for all future decisions (it does not wait
		// for the entry to become committed)."
		if entry.isConfiguration {
			if err := s.config.directSet(peers); err != nil {
				return AppendEntriesResponse{
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
	// < ptrb> ongardie: if the new leader sends a 0-entry AppendEntries
	// with lastIndex=5 commitIndex=4, to a follower that has lastIndex=5
	// commitIndex=5 -- in my impl, this fails, because commitIndex is too
	// small. shouldn't be?
	// <@ongardie> ptrb: i don't think that should fail
	// <@ongardie> there are 4 ways an AppendEntries request can fail: (1)
	// network drops packet (2) caller has stale term (3) would leave gap in the
	// recipient's log (4) term of entry preceding the new entries doesn't match
	// the term at the same index on the recipient
	//
	if r.CommitIndex > 0 && r.CommitIndex > s.log.getCommitIndex() {
		if err := s.log.commitTo(r.CommitIndex); err != nil {
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
