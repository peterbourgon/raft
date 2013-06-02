package rafthttp_test

import (
	"bytes"
	"fmt"
	"github.com/peterbourgon/raft"
	"github.com/peterbourgon/raft/http"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func noop(uint64, []byte) []byte { return []byte{} }

var (
	listenHost = "127.0.0.1"
	basePort   = 8900
)

func Test3Servers(t *testing.T) {
	testServers(t, 3)
}

func Test8Servers(t *testing.T) {
	testServers(t, 8)
}

func testServers(t *testing.T, n int) {
	log.SetFlags(log.Lmicroseconds)
	oldMin, oldMax := raft.ResetElectionTimeoutMs(50, 100)
	defer raft.ResetElectionTimeoutMs(oldMin, oldMax)

	// node = Raft protocol server + a HTTP server + a transport bridge
	raftServers := make([]*raft.Server, n)
	httpServers := make([]*http.Server, n)
	raftHttpServers := make([]*rafthttp.Server, n)

	// create them individually
	for i := 0; i < n; i++ {
		// create a Raft protocol server
		raftServers[i] = raft.NewServer(uint64(i+1), &bytes.Buffer{}, noop)

		// wrap that server in a HTTP transport
		raftHttpServers[i] = rafthttp.NewServer(raftServers[i])

		// connect that HTTP transport to a unique HTTP server
		mux := http.NewServeMux()
		httpServers[i] = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", listenHost, basePort+i+1),
			Handler: mux,
		}
		raftHttpServers[i].Install(mux)

		// we have to start the HTTP server, so the NewHTTPPeer ID check works
		// (it can work without starting the actual Raft protocol server)
		go httpServers[i].ListenAndServe()
		t.Logf("Server id=%d @ %s", raftServers[i].Id(), httpServers[i].Addr)
	}

	// build the common set of peers in the network
	peers := raft.Peers{}
	for i := 0; i < n; i++ {
		u, err := url.Parse(fmt.Sprintf("http://%s:%d", listenHost, basePort+i+1))
		if err != nil {
			t.Fatal(err)
		}
		peer, err := rafthttp.NewPeer(*u)
		if err != nil {
			t.Fatal(err)
		}
		peers[peer.Id()] = peer
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

	time.Sleep(2 * raft.MaximumElectionTimeout())
}
