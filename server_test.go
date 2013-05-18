package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/raft"
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
	server.SetPeers(raft.Peers{
		2: nonresponsivePeer(2),
		3: nonresponsivePeer(3),
	})
	go drainTo(&synchronizedBuffer{}, server.CommandResponses())
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
	server.SetPeers(raft.Peers{
		1: nonresponsivePeer(1),
		2: approvingPeer(2),
		3: nonresponsivePeer(3),
	})
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
	server.SetPeers(raft.Peers{
		2: disapprovingPeer(2),
		3: nonresponsivePeer(3),
	})
	go drainTo(&synchronizedBuffer{}, server.CommandResponses())
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
			return json.Marshal(map[string]interface{}{"id": id, "ok": true})
		}
	}

	s1 := raft.NewServer(1, &bytes.Buffer{}, applyValue(1, &i1))
	s2 := raft.NewServer(2, &bytes.Buffer{}, applyValue(2, &i2))
	s3 := raft.NewServer(3, &bytes.Buffer{}, applyValue(3, &i3))

	s1Responses := &synchronizedBuffer{}
	go drainTo(s1Responses, s1.CommandResponses())
	s2Responses := &synchronizedBuffer{}
	go drainTo(s2Responses, s2.CommandResponses())
	s3Responses := &synchronizedBuffer{}
	go drainTo(s3Responses, s3.CommandResponses())

	peers := map[uint64]raft.Peer{
		s1.Id: raft.NewLocalPeer(s1),
		s2.Id: raft.NewLocalPeer(s2),
		s3.Id: raft.NewLocalPeer(s3),
	}

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
	if err := s1.Command(cmdBuf); err != nil {
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

func TestOrdering(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	oldMin, oldMax := resetElectionTimeoutMs(25, 50)
	defer resetElectionTimeoutMs(oldMin, oldMax)

	values := rand.Perm(8 + rand.Intn(16))
	for nServers := 1; nServers <= 6; nServers++ {
		t.Logf("nServers=%d", nServers)
		done := make(chan struct{})
		go func() {
			testOrder(t, nServers, values...)
			close(done)
		}()
		select {
		case <-done:
			break
		case <-time.After(5 * time.Second):
			t.Fatalf("nServers=%d values=%v timeout (infinite loop?)", nServers, values)
		}
	}
}

func testOrder(t *testing.T, nServers int, values ...int) {
	// command and response
	type send struct {
		Send int `json:"send"`
	}
	type recv struct {
		Recv int `json:"recv"`
	}
	do := func(buf []byte) ([]byte, error) {
		var s send
		json.Unmarshal(buf, &s)
		return json.Marshal(recv{Recv: s.Send})
	}

	// set up the cluster
	servers := []*raft.Server{}
	buffers := []*bytes.Buffer{}
	for i := 0; i < nServers; i++ {
		buffers = append(buffers, &bytes.Buffer{})
		servers = append(servers, raft.NewServer(uint64(i+1), buffers[i], do))
	}
	peers := raft.Peers{}
	for _, server := range servers {
		peers[server.Id] = raft.NewLocalPeer(server)
	}
	for _, server := range servers {
		server.SetPeers(peers)
	}

	// record command responses somewhere
	//began := time.Now()
	responses := make([]synchronizedBuffer, len(servers))
	for i, server := range servers {
		go func(server0 *raft.Server, i0 int) {
			for buf := range server0.CommandResponses() {
				//t.Logf("+%-15s %d: recv: %s", time.Since(began), server0.Id, buf)
				var r recv
				json.Unmarshal(buf, &r)
				responses[i0].Write(append([]byte(fmt.Sprint(r.Recv)), ' '))
			}
		}(server, i)
	}

	// define cmds
	cmds := []send{}
	for _, v := range values {
		cmds = append(cmds, send{v})
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
				//t.Logf("+%-15s %d: send: %s", time.Since(began), id, buf)
				switch err := peer.Command(buf); err {
				case nil:
					//t.Logf("+%-15s %d: send OK: %s", time.Since(began), id, buf)
					break retry
				case raft.ErrUnknownLeader:
					//t.Logf("+%-15s %d: send failed, no leader, will retry", time.Since(began), id)
					time.Sleep(raft.ElectionTimeout())
				default:
					t.Fatalf("i=%d peer=%d send failed: %s", i, id, err)
				}
			}
		}

		// done sending
		log.Printf("testOrder done sending %d command(s) to network", len(cmds))
	}()

	// check the command responses
	expected := []byte{}
	for _, cmd := range cmds {
		expected = append(expected, append([]byte(fmt.Sprint(cmd.Send)), ' ')...)
	}
	for i := 0; i < len(servers); i++ {
		got := responses[i].String()
		t.Logf("%d: %v", i, got)
		if string(expected) != got {
			t.Errorf("%d: %s != %s", i, string(expected), got)
		}
	}
}

//
//
//

type synchronizedBuffer struct {
	sync.RWMutex
	buf bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) {
	b.Lock()
	defer b.Unlock()
	b.buf.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.RLock()
	defer b.RUnlock()
	return b.buf.String()
}

func drainTo(b *synchronizedBuffer, c <-chan []byte) {
	for buf := range c {
		b.Write(buf)
	}
}

type nonresponsivePeer uint64

func (p nonresponsivePeer) Id() uint64 { return uint64(p) }
func (p nonresponsivePeer) AppendEntries(raft.AppendEntries) raft.AppendEntriesResponse {
	return raft.AppendEntriesResponse{}
}
func (p nonresponsivePeer) RequestVote(raft.RequestVote) raft.RequestVoteResponse {
	return raft.RequestVoteResponse{}
}
func (p nonresponsivePeer) Command([]byte) error {
	return fmt.Errorf("not implemented")
}
func (p nonresponsivePeer) CommandResponses() <-chan []byte {
	return make(chan []byte)
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
func (p approvingPeer) Command([]byte) error {
	return fmt.Errorf("not implemented")
}
func (p approvingPeer) CommandResponses() <-chan []byte {
	return make(chan []byte)
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
func (p disapprovingPeer) Command([]byte) error {
	return fmt.Errorf("not implemented")
}
func (p disapprovingPeer) CommandResponses() <-chan []byte {
	return make(chan []byte)
}
