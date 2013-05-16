package raft_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/peterbourgon/raft"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func init() {
	log.SetFlags(log.Lmicroseconds)
}

func TestFollowerToCandidate(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)

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

	began := time.Now()
	cutoff := began.Add(2 * raft.ElectionTimeout())
	backoff := raft.BroadcastInterval()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Candidate")
		}
		if state := server.State(); state != raft.Candidate {
			//t.Logf("after %15s, %s; retry", time.Since(began), state)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		t.Logf("became Candidate after %s", time.Since(began))
		break
	}

	d := 2 * raft.ElectionTimeout()
	time.Sleep(d)

	if server.State() != raft.Candidate {
		t.Fatalf("after %s, not Candidate", d.String())
	}
}

func TestCandidateToLeader(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)

	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	server := raft.NewServer(1, &bytes.Buffer{}, noop)
	server.SetPeers(raft.Peers{
		1: nonresponsivePeer(1),
		2: approvingPeer(2),
		3: nonresponsivePeer(3),
	})
	server.Start()
	defer func() { server.Stop(); t.Logf("server stopped") }()

	began := time.Now()
	cutoff := began.Add(2 * raft.ElectionTimeout())
	backoff := raft.BroadcastInterval()
	for {
		if time.Now().After(cutoff) {
			t.Fatal("failed to become Leader")
		}
		if state := server.State(); state != raft.Leader {
			//t.Logf("after %15s, %s; retry", time.Since(began), state)
			time.Sleep(backoff)
			backoff *= 2
			continue
		}
		t.Logf("became Leader after %s", time.Since(began))
		break
	}
}

func TestFailedElection(t *testing.T) {
	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)

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
