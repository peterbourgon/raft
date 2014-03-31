package raft

import (
	"errors"
	"time"
)

var (
	errTimeout = errors.New("timeout")
)

// Peer is the local representation of a remote node. It's an interface that may
// be backed by any concrete transport: local, HTTP, net/rpc, etc. Peers must be
// encoding/gob encodable.
type Peer interface {
	id() uint64
	callAppendEntries(appendEntries) appendEntriesResponse
	callRequestVote(requestVote) requestVoteResponse
	callCommand([]byte, chan<- []byte) error
	callSetConfiguration(...Peer) error
}

// localPeer is the simplest kind of peer, mapped to a server in the
// same process-space. Useful for testing and demonstration; not so
// useful for networks of independent processes.
type localPeer struct {
	server *Server
}

func newLocalPeer(server *Server) *localPeer { return &localPeer{server} }

func (p *localPeer) id() uint64 { return p.server.id }

func (p *localPeer) callAppendEntries(ae appendEntries) appendEntriesResponse {
	return p.server.appendEntries(ae)
}

func (p *localPeer) callRequestVote(rv requestVote) requestVoteResponse {
	return p.server.requestVote(rv)
}

func (p *localPeer) callCommand(cmd []byte, response chan<- []byte) error {
	return p.server.Command(cmd, response)
}

func (p *localPeer) callSetConfiguration(peers ...Peer) error {
	return p.server.SetConfiguration(peers...)
}

// requestVoteTimeout issues the requestVote to the given peer.
// If no response is received before timeout, an error is returned.
func requestVoteTimeout(p Peer, rv requestVote, timeout time.Duration) (requestVoteResponse, error) {
	c := make(chan requestVoteResponse, 1)
	go func() { c <- p.callRequestVote(rv) }()

	select {
	case resp := <-c:
		return resp, nil
	case <-time.After(timeout):
		return requestVoteResponse{}, errTimeout
	}
}

// peerMap is a collection of Peer interfaces. It provides some convenience
// functions for actions that should apply to multiple Peers.
type peerMap map[uint64]Peer

// makePeerMap constructs a peerMap from a list of peers.
func makePeerMap(peers ...Peer) peerMap {
	pm := peerMap{}
	for _, peer := range peers {
		pm[peer.id()] = peer
	}
	return pm
}

// explodePeerMap converts a peerMap into a slice of peers.
func explodePeerMap(pm peerMap) []Peer {
	a := []Peer{}
	for _, peer := range pm {
		a = append(a, peer)
	}
	return a
}

func (pm peerMap) except(id uint64) peerMap {
	except := peerMap{}
	for id0, peer := range pm {
		if id0 == id {
			continue
		}
		except[id0] = peer
	}
	return except
}

func (pm peerMap) count() int { return len(pm) }

func (pm peerMap) quorum() int {
	switch n := len(pm); n {
	case 0, 1:
		return 1
	default:
		return (n / 2) + 1
	}
}

// requestVotes sends the passed requestVote RPC to every peer in Peers. It
// forwards responses along the returned requestVoteResponse channel. It makes
// the RPCs with a timeout of BroadcastInterval * 2 (chosen arbitrarily). Peers
// that don't respond within the timeout are retried forever. The retry loop
// stops only when all peers have responded, or a Cancel signal is sent via the
// returned canceler.
func (pm peerMap) requestVotes(r requestVote) (chan voteResponseTuple, canceler) {
	// "[A server entering the candidate stage] issues requestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	// construct the channels we'll return
	abortChan := make(chan struct{})
	tupleChan := make(chan voteResponseTuple)

	go func() {
		// We loop until all Peers have given us a response.
		// Track which Peers have responded.
		respondedAlready := peerMap{} // none yet

		for {
			notYetResponded := disjoint(pm, respondedAlready)
			if len(notYetResponded) <= 0 {
				return // done
			}

			// scatter
			tupleChan0 := make(chan voteResponseTuple, len(notYetResponded))
			for id, peer := range notYetResponded {
				go func(id uint64, peer Peer) {
					resp, err := requestVoteTimeout(peer, r, 2*maximumElectionTimeout())
					tupleChan0 <- voteResponseTuple{id, resp, err}
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
	id       uint64
	response requestVoteResponse
	err      error
}

type canceler interface {
	Cancel()
}

type cancel chan struct{}

func (c cancel) Cancel() { close(c) }

func disjoint(all, except peerMap) peerMap {
	d := peerMap{}
	for id, peer := range all {
		if _, ok := except[id]; ok {
			continue
		}
		d[id] = peer
	}
	return d
}
