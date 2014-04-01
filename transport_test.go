package raft

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
)

func Test3ServersOverHTTP(t *testing.T) {
	testNServersOverHTTP(t, 3)
}

func Test8ServersOverHTTP(t *testing.T) {
	testNServersOverHTTP(t, 8)
}

func testNServersOverHTTP(t *testing.T, n int) {
	if n <= 0 {
		t.Fatalf("n <= 0")
	}

	logBuffer := &bytes.Buffer{}
	log.SetOutput(logBuffer)
	defer log.SetOutput(os.Stdout)
	defer printOnFailure(t, logBuffer)
	oldMin, oldMax := resetElectionTimeoutMS(100, 200)
	defer resetElectionTimeoutMS(oldMin, oldMax)

	// node = Raft protocol server + a HTTP server + a transport bridge
	stateMachines := make([]*protectedSlice, n)
	raftServers := make([]*Server, n)
	httpServers := make([]*httptest.Server, n)

	// create them individually
	for i := 0; i < n; i++ {
		// create a state machine
		stateMachines[i] = &protectedSlice{}

		// create a Raft protocol server
		raftServers[i] = NewServer(uint64(i+1), &bytes.Buffer{}, appender(stateMachines[i]))

		// expose that server with a HTTP transport
		mux := http.NewServeMux()
		HTTPTransport(mux, raftServers[i])

		// bind the HTTP transport to a concrete HTTP server
		httpServers[i] = httptest.NewServer(mux)
		// TODO this sometimes hangs, presuambly because we still have open
		// (active?) connections. Need to debug that.
		//defer httpServers[i].Close()

		// we have to start the HTTP server, so the NewHTTPPeer ID check works
		// (it can work without starting the actual Raft protocol server)
		t.Logf("Server id=%d @ %s", raftServers[i].id, httpServers[i].URL)
	}

	// build the common set of peers in the network
	peers := []Peer{}
	for i := 0; i < n; i++ {
		u, err := url.Parse(httpServers[i].URL)
		if err != nil {
			t.Fatal(err)
		}
		peer, err := NewHTTPPeer(u)
		if err != nil {
			t.Fatal(err)
		}
		peers = append(peers, peer)
	}

	// inject each Raft protocol server with its peers
	for _, raftServer := range raftServers {
		raftServer.SetConfiguration(peers...)
	}

	// start each Raft protocol server
	for _, raftServer := range raftServers {
		raftServer.Start()
		defer raftServer.Stop()
	}

	// wait for them to organize
	time.Sleep(time.Duration(n) * maximumElectionTimeout())

	// send a command into the network
	cmd := []byte(`{"do_something":true}`)
	response := make(chan []byte, 1)
	if err := raftServers[0].Command(cmd, response); err != nil {
		t.Fatal(err)
	}
	select {
	case resp := <-response:
		t.Logf("got %d-byte command response ('%s')", len(resp), resp)
	case <-time.After(2 * maximumElectionTimeout()):
		t.Fatal("timeout waiting for command response")
	}

	// ensure it was replicated
	wg := sync.WaitGroup{}
	wg.Add(len(stateMachines))
	for i := 0; i < len(stateMachines); i++ {
		go func(i int) {
			defer wg.Done()
			backoff := 5 * time.Millisecond
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
	case <-time.After(2 * maximumElectionTimeout()):
		t.Fatalf("timeout waiting for state machines to replicate")
	}
}

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

func appender(ps *protectedSlice) ApplyFunc {
	return func(commitIndex uint64, cmd []byte) []byte {
		ps.Add(cmd)
		return []byte(`{"ok":true}`)
	}
}
