package raft_test

import (
	"bytes"
	"github.com/peterbourgon/raft"
	"net/http"
	"net/url"
)

func ExampleNewServer_http() {
	// A no-op ApplyFunc
	a := func(uint64, []byte) []byte { return []byte{} }

	// Helper function to parse URLs
	mustParseURL := func(rawurl string) url.URL {
		u, err := url.Parse(rawurl)
		if err != nil {
			panic(err)
		}
		u.Path = ""
		return *u
	}

	// Helper function to construct HTTPPeers
	mustNewHTTPPeer := func(u url.URL) raft.Peer {
		p, err := raft.NewHTTPPeer(u)
		if err != nil {
			panic(err)
		}
		return p
	}

	// Construct the server
	s := raft.NewServer(1, &bytes.Buffer{}, a)

	// Expose the server using a HTTP transport
	m := http.NewServeMux()
	raft.HTTPTransport(m, s)
	go func() { http.ListenAndServe(":8080", m) }()

	// Set the initial server configuration
	s.SetConfiguration(
		mustNewHTTPPeer(mustParseURL("http://127.0.0.1:8080")), // this server
		mustNewHTTPPeer(mustParseURL("http://10.1.1.11:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.12:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.13:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.14:8080")),
	)

	// Start the server
	s.Start()

	// Issue a command into the network
	response := make(chan []byte)
	if err := s.Command([]byte(`PING`), response); err != nil {
		panic(err) // command not accepted
	}

	// After the command is replicated, we'll receive the (empty) response
	<-response
}
