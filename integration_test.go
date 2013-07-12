package raft_test

import (
	"bytes"
	"fmt"
	"github.com/peterbourgon/raft"
	"log"
	"net/http"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

func noop(uint64, []byte) []byte { return []byte{} }

type protectedSlice struct {
	sync.RWMutex
	slice [][]byte
}

func (ps *protectedSlice) Get() [][]byte {
	ps.RLock()
	defer ps.RUnlock()
	return ps.slice
}

func (ps *protectedSlice) Add(buf []byte) {
	ps.Lock()
	defer ps.Unlock()
	ps.slice = append(ps.slice, buf)
}

func appender(ps *protectedSlice) raft.ApplyFunc {
	return func(commitIndex uint64, cmd []byte) []byte {
		ps.Add(cmd)
		return []byte{}
	}
}

var (
	listenHost = "127.0.0.1"
	basePort   = 8900
)

func Test3ServersOverHTTP(t *testing.T) {
	testNServersOverHTTP(t, 3)
}

// func Test8ServersOverHTTP(t *testing.T) {
// 	testNServersOverHTTP(t, 8)
// }

func testNServersOverHTTP(t *testing.T, n int) {
	if n <= 0 {
		t.Fatalf("n <= 0")
	}

	log.SetOutput(&bytes.Buffer{})
	defer log.SetOutput(os.Stdout)
	log.SetFlags(log.Lmicroseconds)
	oldMin, oldMax := raft.ResetElectionTimeoutMs(50, 100)
	defer raft.ResetElectionTimeoutMs(oldMin, oldMax)

	// node = Raft protocol server + a HTTP server + a transport bridge
	stateMachines := make([]*protectedSlice, n)
	raftServers := make([]*raft.Server, n)
	transports := make([]*raft.HTTPTransport, n)
	httpServers := make([]*http.Server, n)

	// create them individually
	for i := 0; i < n; i++ {
		// create a state machine
		stateMachines[i] = &protectedSlice{}

		// create a Raft protocol server
		raftServers[i] = raft.NewServer(uint64(i+1), &bytes.Buffer{}, appender(stateMachines[i]))

		// expose that server with a HTTP transport
		mux := http.NewServeMux()
		transports[i].Register(mux, raftServers[i])

		// bind the HTTP transport to a concrete HTTP server
		httpServers[i] = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", listenHost, basePort+i+1),
			Handler: mux,
		}

		// we have to start the HTTP server, so the NewHTTPPeer ID check works
		// (it can work without starting the actual Raft protocol server)
		go httpServers[i].ListenAndServe()
		t.Logf("Server id=%d @ %s", raftServers[i].ID(), httpServers[i].Addr)
	}

	// build the common set of peers in the network
	peers := raft.Peers{}
	for i := 0; i < n; i++ {
		u, err := url.Parse(fmt.Sprintf("http://%s:%d", listenHost, basePort+i+1))
		if err != nil {
			t.Fatal(err)
		}
		peer, err := raft.NewHTTPPeer(*u)
		if err != nil {
			t.Fatal(err)
		}
		peers[peer.ID()] = peer
	}

	// inject each Raft protocol server with its peers
	for _, raftServer := range raftServers {
		raftServer.SetConfiguration(peers)
	}

	// start each Raft protocol server
	for _, raftServer := range raftServers {
		raftServer.Start()
		defer raftServer.Stop()
	}

	// wait for them to organize
	time.Sleep(2 * time.Duration(n) * raft.MaximumElectionTimeout())

	// send a command into the network
	cmd := []byte(`{"do_something":true}`)
	response := make(chan []byte, 1)
	if err := raftServers[0].Command(cmd, response); err != nil {
		t.Fatal(err)
	}
	select {
	case resp := <-response:
		t.Logf("got %d-byte command response ('%s')", len(resp), resp)
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for command response")
	}

	// ensure it was replicated
	wg := sync.WaitGroup{}
	wg.Add(len(stateMachines))
	for i := 0; i < len(stateMachines); i++ {
		go func(i int) {
			defer wg.Done()
			backoff := 10 * time.Millisecond
			for {
				slice := stateMachines[i].Get()
				if len(slice) != 1 {
					t.Logf("stateMachines[%d] not yet replicated", i)
					time.Sleep(backoff)
					backoff *= 2
					continue
				}
				if bytes.Compare(slice[0], cmd) != 0 {
					t.Fatalf("stateMachines[%d]: expected '%s' (%d-byte), got '%s' (%d-byte)", i, string(cmd), len(cmd), string(slice[0]), len(slice[0]))
					return
				}
				t.Logf("stateMachines[%d] replicated OK", i)
				return
			}
		}(i)
	}
	done := make(chan struct{}, 1)
	go func() { wg.Wait(); done <- struct{}{} }()
	select {
	case <-done:
		t.Logf("all state machines successfully replicated")
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for state machines to replicate")
	}
}
