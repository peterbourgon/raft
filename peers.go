package raft

import (
	"errors"
	"time"
)

var (
	ErrTimeout        = errors.New("timeout")
	ErrInvalidRequest = errors.New("invalid request")
)

// Peer is anything which provides a Raft-domain interface to a Server. Peer is
// an interface to facilitate making Servers available over different transport
// mechanisms (e.g. pure local, net/rpc, Protobufs, HTTP...). All Peers should
// be 1:1 with a Server.
type Peer interface {
	Id() uint64
	AppendEntries(AppendEntries) AppendEntriesResponse
	RequestVote(RequestVote) RequestVoteResponse
}

// LocalPeer is the simplest kind of Peer, mapped to a Server in the
// same process-space. Useful for testing and demonstration; not so
// useful for networks of independent processes.
type LocalPeer struct {
	server *Server
}

func NewLocalPeer(server *Server) *LocalPeer { return &LocalPeer{server} }

func (p *LocalPeer) Id() uint64 { return p.server.Id }

func (p *LocalPeer) AppendEntries(ae AppendEntries) AppendEntriesResponse {
	return p.server.AppendEntries(ae)
}

func (p *LocalPeer) RequestVote(rv RequestVote) RequestVoteResponse {
	return p.server.RequestVote(rv)
}

func RequestVoteTimeout(p Peer, rv RequestVote, timeout time.Duration) (RequestVoteResponse, error) {
	c := make(chan RequestVoteResponse)
	go func() { c <- p.RequestVote(rv) }()

	select {
	case resp := <-c:
		return resp, nil
	case <-time.After(timeout):
		return RequestVoteResponse{}, ErrTimeout
	}
}

// Peers is a collection of Peer interfaces. It provides some convenience
// functions for actions that should apply to multiple Peers.
type Peers map[uint64]Peer

func (p Peers) Except(id uint64) Peers {
	except := Peers{}
	for id0, peer := range p {
		if id0 == id {
			continue
		}
		except[id0] = peer
	}
	return except
}

func (p Peers) Count() int { return len(p) }

func (p Peers) Quorum() int {
	switch n := len(p); n {
	case 0, 1:
		return 1
	default:
		return (n / 2) + 1
	}
}

// RequestVotes sends the passed RequestVote RPC to every peer in Peers. It
// forwards responses along the returned RequestVoteResponse channel. It calls
// RequestVoteTimeout with a timeout of BroadcastInterval * 2 (chosen
// arbitrarily). Peers that don't respond within the timeout are retried
// forever. The retry loop stops only when all peers have responded, or a Cancel
// signal is sent via the returned Canceler.
func (p Peers) RequestVotes(r RequestVote) (chan RequestVoteResponse, Canceler) {
	// "[A server entering the candidate stage] issues RequestVote RPCs in
	// parallel to each of the other servers in the cluster. If the candidate
	// receives no response for an RPC, it reissues the RPC repeatedly until a
	// response arrives or the election concludes."

	// construct the channels we'll return
	abortChan := make(chan struct{})
	responsesChan := make(chan RequestVoteResponse)

	// compact a peer.RequestVote response to a single struct
	type tuple struct {
		Id                  uint64
		RequestVoteResponse RequestVoteResponse
		Err                 error
	}

	go func() {
		// we loop until all Peers have given us a response
		// track which Peers have responded
		respondedAlready := Peers{} // none yet

		for {
			notYetResponded := disjoint(p, respondedAlready)
			if len(notYetResponded) <= 0 {
				return // done
			}

			// scatter
			tupleChan := make(chan tuple, len(notYetResponded))
			for id, peer := range notYetResponded {
				go func(id0 uint64, peer0 Peer) {
					resp, err := RequestVoteTimeout(peer0, r, 2*BroadcastInterval())
					tupleChan <- tuple{id0, resp, err}
				}(id, peer)
			}

			// gather
			for i := 0; i < len(notYetResponded); i++ {
				select {
				case t := <-tupleChan:
					if t.Err != nil {
						continue // will need to retry
					}
					respondedAlready[t.Id] = nil           // value irrelevant
					responsesChan <- t.RequestVoteResponse // forward the vote
				case <-abortChan:
					return // give up
				}
			}
		}
	}()

	return responsesChan, cancel(abortChan)
}

type Canceler interface {
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
