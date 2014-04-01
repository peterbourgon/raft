package raft

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
	log.SetFlags(log.Lmicroseconds)
}

func TestFollowerToCandidate(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMS(25, 50)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	server := NewServer(1, &bytes.Buffer{}, noop)
	server.SetConfiguration(
		newLocalPeer(server),
		nonresponsivePeer(2),
		nonresponsivePeer(3),
	)

	server.Start()
	defer server.Stop()
	if server.state.Get() != follower {
		t.Fatalf("didn't start as Follower")
	}

	time.Sleep(maximumElectionTimeout())

	cutoff := time.Now().Add(2 * minimumElectionTimeout())
	backoff := minimumElectionTimeout()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Candidate")
		}
		if state := server.state.Get(); state != candidate {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		t.Logf("became Candidate")
		break
	}
}

func TestCandidateToLeader(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMS(25, 50)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	server := NewServer(1, &bytes.Buffer{}, noop)
	server.SetConfiguration(
		newLocalPeer(server),
		approvingPeer(2),
		nonresponsivePeer(3),
	)

	server.Start()
	defer server.Stop()
	time.Sleep(maximumElectionTimeout())

	cutoff := time.Now().Add(2 * maximumElectionTimeout())
	backoff := maximumElectionTimeout()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Leader")
		}
		if state := server.state.Get(); state != leader {
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		t.Logf("became Leader")
		break
	}
}

func TestFailedElection(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMS(25, 50)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	server := NewServer(1, &bytes.Buffer{}, noop)
	server.SetConfiguration(
		newLocalPeer(server),
		disapprovingPeer(2),
		nonresponsivePeer(3),
	)

	server.Start()
	defer server.Stop()
	time.Sleep(2 * electionTimeout())

	if server.state.Get() == leader {
		t.Fatalf("erroneously became Leader")
	}
	t.Logf("remained %s", server.state.Get())
}

func TestLeaderExpulsion(t *testing.T) {
	// a leader
	// receives a configuration that doesn't include itself
	// when that configuration is committed
	// the leader should shut down
}

func TestSimpleConsensus(t *testing.T) {
	logBuffer := &bytes.Buffer{}
	log.SetOutput(logBuffer)
	defer log.SetOutput(os.Stdout)
	defer printOnFailure(t, logBuffer)
	oldMin, oldMax := resetElectionTimeoutMS(25, 50)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	type SetValue struct {
		Value int32 `json:"value"`
	}

	var i1, i2, i3 int32

	applyValue := func(id uint64, i *int32) func(uint64, []byte) []byte {
		return func(index uint64, cmd []byte) []byte {
			var sv SetValue
			if err := json.Unmarshal(cmd, &sv); err != nil {
				var buf bytes.Buffer
				json.NewEncoder(&buf).Encode(map[string]interface{}{"error": err.Error()})
				return buf.Bytes()
			}
			atomic.StoreInt32(i, sv.Value)
			var buf bytes.Buffer
			json.NewEncoder(&buf).Encode(map[string]interface{}{"applied_to_server": id, "applied_value": sv.Value})
			return buf.Bytes()
		}
	}

	s1 := NewServer(1, &bytes.Buffer{}, applyValue(1, &i1))
	s2 := NewServer(2, &bytes.Buffer{}, applyValue(2, &i2))
	s3 := NewServer(3, &bytes.Buffer{}, applyValue(3, &i3))

	s1Responses := &synchronizedBuffer{}
	s2Responses := &synchronizedBuffer{}
	s3Responses := &synchronizedBuffer{}
	defer func(sb *synchronizedBuffer) { t.Logf("s1 responses: %s", sb.String()) }(s1Responses)
	defer func(sb *synchronizedBuffer) { t.Logf("s2 responses: %s", sb.String()) }(s2Responses)
	defer func(sb *synchronizedBuffer) { t.Logf("s3 responses: %s", sb.String()) }(s3Responses)

	p1 := newLocalPeer(s1)
	p2 := newLocalPeer(s2)
	p3 := newLocalPeer(s3)
	s1.SetConfiguration(p1, p2, p3)
	s2.SetConfiguration(p1, p2, p3)
	s3.SetConfiguration(p1, p2, p3)

	s1.Start()
	s2.Start()
	s3.Start()
	defer s1.Stop()
	defer s2.Stop()
	defer s3.Stop()

	var v int32 = 42
	cmd, _ := json.Marshal(SetValue{v})

	response := make(chan []byte, 1)
	func() {
		for {
			switch err := p1.callCommand(cmd, response); err {
			case nil:
				return
			case errUnknownLeader:
				time.Sleep(minimumElectionTimeout())
			default:
				t.Fatal(err)
			}
		}
	}()

	r, ok := <-response
	if ok {
		s1Responses.Write(r)
	} else {
		t.Logf("didn't receive command response")
	}

	ticker := time.Tick(maximumElectionTimeout())
	timeout := time.After(1 * time.Second)
	for {
		select {
		case <-ticker:
			i1l := atomic.LoadInt32(&i1)
			i2l := atomic.LoadInt32(&i2)
			i3l := atomic.LoadInt32(&i3)
			t.Logf("i1=%02d i2=%02d i3=%02d", i1l, i2l, i3l)
			if i1l == v && i2l == v && i3l == v {
				t.Logf("success!")
				return
			}

		case <-timeout:
			t.Fatal("timeout")
		}
	}
}

func TestOrdering_1Server(t *testing.T) {
	testOrderTimeout(t, 1, 5*time.Second)
}

func TestOrdering_2Servers(t *testing.T) {
	testOrderTimeout(t, 2, 5*time.Second)
}

func TestOrdering_3Servers(t *testing.T) {
	testOrderTimeout(t, 3, 5*time.Second)
}

func TestOrdering_4Servers(t *testing.T) {
	testOrderTimeout(t, 4, 5*time.Second)
}

func TestOrdering_5Servers(t *testing.T) {
	testOrderTimeout(t, 5, 5*time.Second)
}

func TestOrdering_6Servers(t *testing.T) {
	testOrderTimeout(t, 6, 5*time.Second)
}

func testOrderTimeout(t *testing.T, nServers int, timeout time.Duration) {
	logBuffer := &bytes.Buffer{}
	log.SetOutput(logBuffer)
	defer log.SetOutput(os.Stdout)
	defer printOnFailure(t, logBuffer)
	oldMin, oldMax := resetElectionTimeoutMS(50, 100)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	done := make(chan struct{})
	go func() { testOrder(t, nServers); close(done) }()
	select {
	case <-done:
		break
	case <-time.After(timeout):
		t.Fatalf("timeout (infinite loop?)")
	}
}

func testOrder(t *testing.T, nServers int) {
	values := rand.Perm(8 + rand.Intn(16))

	// command and response
	type send struct {
		Send int `json:"s"`
	}
	type recv struct {
		Recv int `json:"r"`
	}
	do := func(sb *synchronizedBuffer) func(uint64, []byte) []byte {
		return func(index uint64, cmd []byte) []byte {
			sb.Write(cmd) // write incoming message
			var s send    // decode incoming message
			json.Unmarshal(cmd, &s)
			var buf bytes.Buffer
			json.NewEncoder(&buf).Encode(recv{Recv: s.Send})
			return buf.Bytes() // write outgoing message
		}
	}

	// set up the cluster
	servers := []*Server{}             // server components
	storage := []*bytes.Buffer{}       // persistent log storage
	buffers := []*synchronizedBuffer{} // the "state machine" for each server
	for i := 0; i < nServers; i++ {
		buffers = append(buffers, &synchronizedBuffer{})
		storage = append(storage, &bytes.Buffer{})
		servers = append(servers, NewServer(uint64(i+1), storage[i], do(buffers[i])))
	}
	peers := []Peer{}
	for _, server := range servers {
		peers = append(peers, newLocalPeer(server))
	}
	for _, server := range servers {
		server.SetConfiguration(peers...)
	}

	// define cmds
	cmds := []send{}
	for _, v := range values {
		cmds = append(cmds, send{v})
	}

	// the expected "state-machine" output of applying each command
	expectedBuffer := &synchronizedBuffer{}
	for _, cmd := range cmds {
		buf, _ := json.Marshal(cmd)
		expectedBuffer.Write(buf)
	}

	// boot up the cluster
	for _, server := range servers {
		server.Start()
		defer func(server *Server) {
			log.Printf("issuing stop command to server %d", server.id)
			server.Stop()
		}(server)
	}

	// send commands
	for i, cmd := range cmds {
		index := uint64(rand.Intn(nServers))
		peer := peers[index]
		id := peer.id()
		buf, _ := json.Marshal(cmd)

		for {
			log.Printf("command=%d/%d peer=%d: sending %s", i+1, len(cmds), id, buf)
			response := make(chan []byte, 1)
			err := peer.callCommand(buf, response)

			switch err {
			case nil:
				log.Printf("command=%d/%d peer=%d: OK", i+1, len(cmds), id)
				break

			case errUnknownLeader, errDeposed:
				log.Printf("command=%d/%d peer=%d: failed (%s) -- will retry", i+1, len(cmds), id, err)
				time.Sleep(electionTimeout())
				continue

			case errTimeout:
				log.Printf("command=%d/%d peer=%d: timed out -- assume it went through", i+1, len(cmds), id)
				break

			default:
				t.Fatalf("command=%d/%d peer=%d: failed (%s) -- fatal", i+1, len(cmds), id, err)
			}

			r, ok := <-response
			if !ok {
				log.Printf("command=%d/%d peer=%d: truncated, will retry", i+1, len(cmds), id)
				continue
			}

			log.Printf("command=%d/%d peer=%d: OK, got response %s", i+1, len(cmds), id, string(r))
			break
		}
	}

	// done sending
	log.Printf("testOrder done sending %d command(s) to network", len(cmds))

	// check the buffers (state machines)
	for i, sb := range buffers {
		for {
			expected, got := expectedBuffer.String(), sb.String()
			if len(got) < len(expected) {
				t.Logf("server %d: not yet fully replicated, will check again", i+1)
				time.Sleep(maximumElectionTimeout())
				continue // retry
			}
			if expected != got {
				t.Errorf("server %d: fully replicated, expected\n\t%s, got\n\t%s", i+1, expected, got)
				break
			}
			t.Logf("server %d: %s OK", i+1, got)
			break
		}
	}
}

//
//
//

func printOnFailure(t *testing.T, r io.Reader) {
	if !t.Failed() {
		return
	}
	rd := bufio.NewReader(r)
	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		t.Logf("> %s", line)
	}
}

type synchronizedBuffer struct {
	sync.RWMutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.RLock()
	defer b.RUnlock()
	return b.buf.String()
}

type nonresponsivePeer uint64

func (p nonresponsivePeer) id() uint64 { return uint64(p) }
func (p nonresponsivePeer) callAppendEntries(appendEntries) appendEntriesResponse {
	return appendEntriesResponse{}
}
func (p nonresponsivePeer) callRequestVote(requestVote) requestVoteResponse {
	return requestVoteResponse{}
}
func (p nonresponsivePeer) callCommand([]byte, chan<- []byte) error {
	return fmt.Errorf("not implemented")
}
func (p nonresponsivePeer) callSetConfiguration(...Peer) error {
	return fmt.Errorf("not implemented")
}

type approvingPeer uint64

func (p approvingPeer) id() uint64 { return uint64(p) }
func (p approvingPeer) callAppendEntries(appendEntries) appendEntriesResponse {
	return appendEntriesResponse{}
}
func (p approvingPeer) callRequestVote(rv requestVote) requestVoteResponse {
	return requestVoteResponse{
		Term:        rv.Term,
		VoteGranted: true,
	}
}
func (p approvingPeer) callCommand([]byte, chan<- []byte) error {
	return fmt.Errorf("not implemented")
}
func (p approvingPeer) callSetConfiguration(...Peer) error {
	return fmt.Errorf("not implemented")
}

type disapprovingPeer uint64

func (p disapprovingPeer) id() uint64 { return uint64(p) }
func (p disapprovingPeer) callAppendEntries(appendEntries) appendEntriesResponse {
	return appendEntriesResponse{}
}
func (p disapprovingPeer) callRequestVote(rv requestVote) requestVoteResponse {
	return requestVoteResponse{
		Term:        rv.Term,
		VoteGranted: false,
	}
}
func (p disapprovingPeer) callCommand([]byte, chan<- []byte) error {
	return fmt.Errorf("not implemented")
}
func (p disapprovingPeer) callSetConfiguration(...Peer) error {
	return fmt.Errorf("not implemented")
}
