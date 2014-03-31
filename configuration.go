package raft

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

var (
	errConfigurationAlreadyChanging = errors.New("configuration already changing")
)

const (
	cOld    = "C_old"
	cOldNew = "C_old,new"
)

// configuration represents the sets of peers and behaviors required to
// implement joint-consensus.
type configuration struct {
	sync.RWMutex
	state     string
	cOldPeers peerMap
	cNewPeers peerMap
}

// newConfiguration returns a new configuration in stable (C_old) state based
// on the passed peers.
func newConfiguration(pm peerMap) *configuration {
	return &configuration{
		state:     cOld, // start in a stable state,
		cOldPeers: pm,   // with only C_old
	}
}

// directSet is used when bootstrapping, and when receiving a replicated
// configuration from a leader. It directly sets the configuration to the
// passed peers. It's assumed this is called on a non-leader, and therefore
// requires no consistency dance.
func (c *configuration) directSet(pm peerMap) error {
	c.Lock()
	defer c.Unlock()

	c.cOldPeers = pm
	c.cNewPeers = peerMap{}
	c.state = cOld
	return nil
}

func (c *configuration) get(id uint64) (Peer, bool) {
	c.RLock()
	defer c.RUnlock()

	if peer, ok := c.cOldPeers[id]; ok {
		return peer, true
	}
	if peer, ok := c.cNewPeers[id]; ok {
		return peer, true
	}
	return nil, false
}

func (c *configuration) encode() ([]byte, error) {
	buf := &bytes.Buffer{}
	if err := gob.NewEncoder(buf).Encode(c.allPeers()); err != nil {
		return []byte{}, err
	}
	return buf.Bytes(), nil
}

// allPeers returns the union set of all peers in the configuration.
func (c *configuration) allPeers() peerMap {
	c.RLock()
	defer c.RUnlock()

	union := peerMap{}
	for id, peer := range c.cOldPeers {
		union[id] = peer
	}
	for id, peer := range c.cNewPeers {
		union[id] = peer
	}
	return union
}

// pass returns true if the votes represented by the votes map are sufficient
// to constitute a quorum. pass respects C_old,new requirements, which dictate
// that any request must receive a majority from both C_old and C_new to pass.
func (c *configuration) pass(votes map[uint64]bool) bool {
	c.RLock()
	defer c.RUnlock()

	// Count the votes
	cOldHave, cOldRequired := 0, c.cOldPeers.quorum()
	for id := range c.cOldPeers {
		if votes[id] {
			cOldHave++
		}
		if cOldHave >= cOldRequired {
			break
		}
	}

	// If we've already failed, we can stop here
	if cOldHave < cOldRequired {
		return false
	}

	// C_old passes: if we're in C_old, we pass
	if c.state == cOld {
		return true
	}

	// Not in C_old, so make sure we have some peers in C_new
	if len(c.cNewPeers) <= 0 {
		panic(fmt.Sprintf("configuration state '%s', but no C_new peers", c.state))
	}

	// Since we're in C_old,new, we need to also pass C_new to pass overall.
	// It's important that we range through C_new and check our votes map, and
	// not the other way around: if a server casts a vote but doesn't exist in
	// a particular configuration, that vote should not be counted.
	cNewHave, cNewRequired := 0, c.cNewPeers.quorum()
	for id := range c.cNewPeers {
		if votes[id] {
			cNewHave++
		}
		if cNewHave >= cNewRequired {
			break
		}
	}

	return cNewHave >= cNewRequired
}

// changeTo signals a request to change to the configuration represented by the
// passed peers. changeTo puts the configuration in the C_old,new state.
// changeTo should be eventually followed by ChangeCommitted or ChangeAborted.
func (c *configuration) changeTo(pm peerMap) error {
	c.Lock()
	defer c.Unlock()

	if c.state != cOld {
		return errConfigurationAlreadyChanging
	}

	if len(c.cNewPeers) > 0 {
		panic(fmt.Sprintf("configuration ChangeTo in state '%s', but have C_new peers already", c.state))
	}

	c.cNewPeers = pm
	c.state = cOldNew
	return nil
}

// changeCommitted moves a configuration from C_old,new to C_new.
func (c *configuration) changeCommitted() {
	c.Lock()
	defer c.Unlock()

	if c.state != cOldNew {
		panic("configuration ChangeCommitted, but not in C_old,new")
	}

	if len(c.cNewPeers) <= 0 {
		panic("configuration ChangeCommitted, but C_new peers are empty")
	}

	c.cOldPeers = c.cNewPeers
	c.cNewPeers = peerMap{}
	c.state = cOld
}

// changeAborted moves a configuration from C_old,new to C_old.
func (c *configuration) changeAborted() {
	c.Lock()
	defer c.Unlock()

	if c.state != cOldNew {
		panic("configuration ChangeAborted, but not in C_old,new")
	}

	c.cNewPeers = peerMap{}
	c.state = cOld
}
