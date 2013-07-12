package raft

import (
	"errors"
	"time"
)

var (
	errTimeout = errors.New("timeout")
)

// Peer is the local representation of a remote node. It may be backed by any
// concrete transport: HTTP, net/rpc, etc. It provides a Raft-domain interface
// to a server. Peers must be encoding/gob encodable.
type Peer interface {
	ID() uint64
	AppendEntries(AppendEntries) AppendEntriesResponse
	RequestVote(RequestVote) RequestVoteResponse
	Command([]byte, chan []byte) error
	SetConfiguration(Peers) error
}

// localPeer is the simplest kind of peer, mapped to a server in the
// same process-space. Useful for testing and demonstration; not so
// useful for networks of independent processes.
type localPeer struct {
	server *Server
}

func newLocalPeer(server *Server) *localPeer { return &localPeer{server} }

func (p *localPeer) ID() uint64 { return p.server.ID() }

func (p *localPeer) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	return p.server.appendEntries(ae)
}

func (p *localPeer) RequestVote(rv RequestVote) RequestVoteResponse {
	return p.server.requestVote(rv)
}

func (p *localPeer) Command(cmd []byte, response chan []byte) error {
	return p.server.Command(cmd, response)
}

func (p *localPeer) SetConfiguration(peers Peers) error {
	return p.server.SetConfiguration(peers)
}

// requestVoteTimeout issues the RequestVote to the given peer.
// If no response is received before timeout, an error is returned.
func requestVoteTimeout(p Peer, rv RequestVote, timeout time.Duration) (RequestVoteResponse, error) {
	c := make(chan RequestVoteResponse, 1)
	go func() { c <- p.RequestVote(rv) }()

	select {
	case resp := <-c:
		return resp, nil
	case <-time.After(timeout):
		return RequestVoteResponse{}, errTimeout
	}
}

// Peers is a collection of Peer interfaces. It provides some convenience
// functions for actions that should apply to multiple Peers.
type Peers map[uint64]Peer

// MakePeers is a simple helper function to construct a Peers structure from the
// passed list of peers.
func MakePeers(peers ...Peer) Peers {
	p := Peers{}
	for _, peer := range peers {
		p[peer.ID()] = peer
	}
	return p
}

func (p Peers) except(id uint64) Peers {
	except := Peers{}
	for id0, peer := range p {
		if id0 == id {
			continue
		}
		except[id0] = peer
	}
	return except
}

func (p Peers) count() int { return len(p) }

func (p Peers) quorum() int {
	switch n := len(p); n {
	case 0, 1:
		return 1
	default:
		return (n / 2) + 1
	}
}

// requestVotes sends the passed RequestVote RPC to every peer in Peers. It
// forwards responses along the returned RequestVoteResponse channel. It makes
// the RPCs with a timeout of BroadcastInterval * 2 (chosen arbitrarily). Peers
// that don't respond within the timeout are retried forever. The retry loop
// stops only when all peers have responded, or a Cancel signal is sent via the
// returned Canceler.
func (p Peers) requestVotes(r RequestVote) (chan voteResponseTuple, canceler) {
	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	// construct the channels we'll return
	abortChan := make(chan struct{})
	tupleChan := make(chan voteResponseTuple)

	go func() {
		// We loop until all Peers have given us a response.
		// Track which Peers have responded.
		respondedAlready := Peers{} // none yet

		for {
			notYetResponded := disjoint(p, respondedAlready)
			if len(notYetResponded) <= 0 {
				return // done
			}

			// scatter
			tupleChan0 := make(chan voteResponseTuple, len(notYetResponded))
			for id, peer := range notYetResponded {
				go func(id0 uint64, peer0 Peer) {
					resp, err := requestVoteTimeout(peer0, r, 2*MaximumElectionTimeout())
					tupleChan0 <- voteResponseTuple{id0, resp, err}
				}(id, peer)
			}

			// gather
			for i := 0; i < cap(tupleChan0); i++ {
				select {
				case t := <-tupleChan0:
					if t.err != nil {
						continue // will need to retry
					}
					respondedAlready[t.id] = nil // set membership semantics
					tupleChan <- t

				case <-abortChan:
					return // give up
				}
			}
		}
	}()

	return tupleChan, cancel(abortChan)
}

type voteResponseTuple struct {
	id  uint64
	rvr RequestVoteResponse
	err error
}

type canceler interface {
	Cancel()
}

type cancel chan struct{}

func (c cancel) Cancel() { close(c) }

func disjoint(all, except Peers) Peers {
	d := Peers{}
	for id, peer := range all {
		if _, ok := except[id]; ok {
			continue
		}
		d[id] = peer
	}
	return d
}
