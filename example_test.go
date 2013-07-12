package raft

import (
	"bytes"
	"net/http"
	"net/url"
)

func ExampleHTTPNode() {
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
	mustNewHTTPPeer := func(u url.URL) *HTTPPeer {
		p, err := NewHTTPPeer(u)
		if err != nil {
			panic(err)
		}
		return p
	}

	// Construct the server
	s := NewServer(1, &bytes.Buffer{}, a)

	// Expose the server using a HTTP transport
	m := http.NewServeMux()
	t := HTTPTransport{}
	t.Register(m, s)
	go func() { http.ListenAndServe(":8080", m) }()

	// Set the initial server configuration, and start the server
	s.SetConfiguration(MakePeers(
		mustNewHTTPPeer(mustParseURL("http://localhost:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.11:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.12:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.13:8080")),
		mustNewHTTPPeer(mustParseURL("http://10.1.1.14:8080")),
	))
	s.Start()
}
