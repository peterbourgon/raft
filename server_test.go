package raft_test

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/raft"
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

func resetElectionTimeoutMs(newMin, newMax int) (int, int) {
	oldMin, oldMax := raft.MinimumElectionTimeoutMs, raft.MaximumElectionTimeoutMs
	raft.MinimumElectionTimeoutMs, raft.MaximumElectionTimeoutMs = newMin, newMax
	return oldMin, oldMax
}

func TestFollowerToCandidate(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMs(25, 50)
	defer resetElectionTimeoutMs(oldMin, oldMax)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.MakePeers(nonresponsivePeer(2), nonresponsivePeer(3)))
	if server.State() != raft.Follower {
		t.Fatalf("didn't start as Follower")
	}

	server.Start()
	defer func() { server.Stop(); t.Logf("server stopped") }()

	minimum := time.Duration(raft.MaximumElectionTimeoutMs) * time.Millisecond
	time.Sleep(minimum)

	cutoff := time.Now().Add(2 * minimum)
	backoff := raft.BroadcastInterval()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Candidate")
		}
		if state := server.State(); state != raft.Candidate {
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
	oldMin, oldMax := resetElectionTimeoutMs(25, 50)
	defer resetElectionTimeoutMs(oldMin, oldMax)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.MakePeers(nonresponsivePeer(1), approvingPeer(2), nonresponsivePeer(3)))
	server.Start()
	defer func() { server.Stop(); t.Logf("server stopped") }()

	minimum := time.Duration(raft.MaximumElectionTimeoutMs) * time.Millisecond
	time.Sleep(minimum)

	cutoff := time.Now().Add(2 * minimum)
	backoff := raft.BroadcastInterval()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Leader")
		}
		if state := server.State(); state != raft.Leader {
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
	oldMin, oldMax := resetElectionTimeoutMs(25, 50)
	defer resetElectionTimeoutMs(oldMin, oldMax)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.MakePeers(disapprovingPeer(2), nonresponsivePeer(3)))
	server.Start()
	defer func() { server.Stop(); t.Logf("server stopped") }()

	time.Sleep(2 * raft.ElectionTimeout())
	if server.State() == raft.Leader {
		t.Fatalf("erroneously became Leader")
	}
	t.Logf("remained %s", server.State())
}

func TestSimpleConsensus(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMs(25, 50)
	defer resetElectionTimeoutMs(oldMin, oldMax)

	type SetValue struct {
		Value int32 `json:"value"`
	}

	var i1, i2, i3 int32

	applyValue := func(id uint64, i *int32) func([]byte) ([]byte, error) {
		return func(cmd []byte) ([]byte, error) {
			var sv SetValue
			if err := json.Unmarshal(cmd, &sv); err != nil {
				return []byte{}, err
			}
			atomic.StoreInt32(i, sv.Value)
			return json.Marshal(map[string]interface{}{"applied_to_server": id, "applied_value": sv.Value})
		}
	}

	s1 := raft.NewServer(1, &bytes.Buffer{}, applyValue(1, &i1))
	s2 := raft.NewServer(2, &bytes.Buffer{}, applyValue(2, &i2))
	s3 := raft.NewServer(3, &bytes.Buffer{}, applyValue(3, &i3))

	appendTo := func(dst *synchronizedBuffer) chan []byte {
		c := make(chan []byte)
		go func() {
			for p := range c {
				dst.Write(p)
			}
		}()
		return c
	}
	s1Responses := &synchronizedBuffer{}
	s2Responses := &synchronizedBuffer{}
	s3Responses := &synchronizedBuffer{}

	peers := raft.MakePeers(
		raft.NewLocalPeer(s1),
		raft.NewLocalPeer(s2),
		raft.NewLocalPeer(s3),
	)

	s1.SetPeers(peers)
	s2.SetPeers(peers)
	s3.SetPeers(peers)

	s1.Start()
	s2.Start()
	s3.Start()
	defer func() { s1.Stop(); t.Logf("s1 stopped") }()
	defer func() { s2.Stop(); t.Logf("s2 stopped") }()
	defer func() { s3.Stop(); t.Logf("s3 stopped") }()

	time.Sleep(2 * raft.ElectionTimeout())

	cmd := SetValue{42}
	cmdBuf, err := json.Marshal(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Command(cmdBuf, appendTo(s1Responses)); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		d := raft.BroadcastInterval()
		for {
			i1l := atomic.LoadInt32(&i1)
			i2l := atomic.LoadInt32(&i2)
			i3l := atomic.LoadInt32(&i3)
			t.Logf("i1=%02d i2=%02d i3=%02d", i1l, i2l, i3l)
			if i1l == cmd.Value && i2l == cmd.Value && i3l == cmd.Value {
				close(done)
				return
			}
			time.Sleep(d)
			d *= 2
		}
	}()

	select {
	case <-done:
		t.Logf("success")
	case <-time.After(1 * time.Second):
		t.Errorf("timeout")
	}

	t.Logf("s1 responses: %s", s1Responses.String())
	t.Logf("s2 responses: %s", s2Responses.String())
	t.Logf("s3 responses: %s", s3Responses.String())
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
	oldMin, oldMax := resetElectionTimeoutMs(50, 100)
	defer resetElectionTimeoutMs(oldMin, oldMax)

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
		Send int `json:"send"`
	}
	type recv struct {
		Recv int `json:"recv"`
	}
	do := func(sb *synchronizedBuffer) func(buf []byte) ([]byte, error) {
		return func(buf []byte) ([]byte, error) {
			sb.Write(buf)                           // write incoming message
			var s send                              // decode incoming message
			json.Unmarshal(buf, &s)                 // ...
			return json.Marshal(recv{Recv: s.Send}) // write outgoing message
		}
	}

	// set up the cluster
	servers := []*raft.Server{}        // server components
	storage := []*bytes.Buffer{}       // persistent log storage
	buffers := []*synchronizedBuffer{} // the "state machine" for each server
	for i := 0; i < nServers; i++ {
		buffers = append(buffers, &synchronizedBuffer{})
		storage = append(storage, &bytes.Buffer{})
		servers = append(servers, raft.NewServer(uint64(i+1), storage[i], do(buffers[i])))
	}
	peers := raft.Peers{}
	for _, server := range servers {
		peers[server.Id] = raft.NewLocalPeer(server)
	}
	for _, server := range servers {
		server.SetPeers(peers)
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

	// function-scope this so servers get stopped deterministically
	func() {
		// boot up the cluster
		for _, server := range servers {
			server.Start()
			defer func(server0 *raft.Server) {
				log.Printf("issuing stop command to server %d", server0.Id)
				server0.Stop()
			}(server)
		}

		// send commands
		for i, cmd := range cmds {
			id := uint64(rand.Intn(nServers)) + 1
			peer := peers[id]
			buf, _ := json.Marshal(cmd)
		retry:
			for {
				log.Printf("testOrder sending command %d/%d: %s", i+1, len(cmds), buf)
				switch err := peer.Command(buf, make(chan []byte, 1)); err {
				case nil:
					log.Printf("command=%d/%d peer=%d: OK", i+1, len(cmds), id)
					break retry
				case raft.ErrUnknownLeader, raft.ErrDeposed, raft.ErrTimeout:
					log.Printf("command=%d/%d peer=%d: failed (%s) -- will retry", i+1, len(cmds), id, err)
					time.Sleep(raft.ElectionTimeout())
				default:
					t.Fatalf("command=%d/%d peer=%d: failed (%s) -- fatal", i+1, len(cmds), id, err)
				}
			}
		}

		// done sending
		log.Printf("testOrder done sending %d command(s) to network", len(cmds))
		time.Sleep(4 * raft.BroadcastInterval()) // time to replicate
		log.Printf("testOrder shutting servers down")
	}()

	// check the buffers (state machines)
	for i, sb := range buffers {
		expected, got := expectedBuffer.String(), sb.String()
		t.Logf("server %d: state machine: %s", i+1, got)
		if expected != got {
			t.Fatalf("server %d: expected \n\t'%s', got \n\t'%s'", i+1, expected, got)
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

func (p nonresponsivePeer) Id() uint64 { return uint64(p) }
func (p nonresponsivePeer) AppendEntries(raft.AppendEntries) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{}
}
func (p nonresponsivePeer) RequestVote(raft.RequestVote) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{}
}
func (p nonresponsivePeer) Command([]byte, chan []byte) error {
	return fmt.Errorf("not implemented")
}

type approvingPeer uint64

func (p approvingPeer) Id() uint64 { return uint64(p) }
func (p approvingPeer) AppendEntries(raft.AppendEntries) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{}
}
func (p approvingPeer) RequestVote(rv raft.RequestVote) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{
		Term:        rv.Term,
		VoteGranted: true,
	}
}
func (p approvingPeer) Command([]byte, chan []byte) error {
	return fmt.Errorf("not implemented")
}

type disapprovingPeer uint64

func (p disapprovingPeer) Id() uint64 { return uint64(p) }
func (p disapprovingPeer) AppendEntries(raft.AppendEntries) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{}
}
func (p disapprovingPeer) RequestVote(rv raft.RequestVote) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{
		Term:        rv.Term,
		VoteGranted: false,
	}
}
func (p disapprovingPeer) Command([]byte, chan []byte) error {
	return fmt.Errorf("not implemented")
}
