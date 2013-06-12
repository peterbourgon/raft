package raft

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrConfigurationAlreadyChanging = errors.New("Configuration already changing")
	ErrConfigurationNotInOldNew     = errors.New("Configuration not in C_old,new")
)

const (
	Old    = "C_old"
	OldNew = "C_old,new"
)

type Configuration struct {
	sync.RWMutex
	state string
	c_old Peers
	c_new Peers
}

func NewConfiguration(peers Peers) *Configuration {
	return &Configuration{
		state: Old,   // start in a stable state,
		c_old: peers, // with only C_old
	}
}

// AllPeers returns the union set of all peers in the Configuration.
func (c *Configuration) AllPeers() Peers {
	c.RLock()
	defer c.RUnlock()

	union := Peers{}
	for id, peer := range c.c_old {
		union[id] = peer
	}
	for id, peer := range c.c_new {
		union[id] = peer
	}
	return union
}

// Pass returns true if the votes represented by the votes map are sufficient
// to constitute a quorum. Pass respects C_old,new requirements, which dictate
// that any request must receive a majority from both C_old and C_new to pass.
func (c *Configuration) Pass(votes map[uint64]bool) bool {
	c.RLock()
	defer c.RUnlock()

	// Count the votes
	c_oldHave, c_oldRequired := 0, c.c_old.Quorum()
	for id := range c.c_old {
		if votes[id] {
			c_oldHave++
		}
		if c_oldHave >= c_oldRequired {
			break
		}
	}

	// If we've already failed, we can stop here
	if c_oldHave < c_oldRequired {
		return false
	}

	// C_old passes: if we're in C_old, we pass
	if c.state == Old {
		return true
	}

	// Not in C_old, so make sure we have some peers in C_new
	if len(c.c_new) <= 0 {
		panic(fmt.Sprintf("Configuration state '%s', but no C_new peers", c.state))
	}

	// Since we're in C_old,new, we need to also pass C_new to pass overall
	c_newHave, c_newRequired := 0, c.c_new.Quorum()
	for id := range c.c_new {
		if votes[id] {
			c_newHave++
		}
		if c_newHave >= c_newRequired {
			break
		}
	}

	return c_newHave >= c_newRequired
}

// ChangeTo signals a request to change to the configuration represented by the
// passed peers. ChangeTo puts the Configuration in the C_old,new state.
// ChangeTo should be eventually followed by ChangeCommitted or ChangeAborted.
func (c *Configuration) ChangeTo(peers Peers) error {
	c.Lock()
	defer c.Unlock()

	if c.state != Old {
		return ErrConfigurationAlreadyChanging
	}

	if len(c.c_new) > 0 {
		panic(fmt.Sprintf("Configuration ChangeTo in state '%s', but have C_new peers already", c.state))
	}

	c.c_new = peers
	c.state = OldNew
	return nil
}

// ChangeCommitted moves a Configuration from C_old,new to C_new.
func (c *Configuration) ChangeCommitted() {
	c.Lock()
	defer c.Unlock()

	if c.state != OldNew {
		panic("Configuration ChangeCommitted, but not in C_old,new")
	}

	if len(c.c_new) <= 0 {
		panic("Configuration ChangeCommitted, but C_new peers are empty")
	}

	c.c_old = c.c_new
	c.c_new = Peers{}
	c.state = Old
}

// ChangeAborted moves a Configuration from C_old,ew to C_old.
func (c *Configuration) ChangeAborted() {
	c.Lock()
	defer c.Unlock()

	if c.state != OldNew {
		panic("Configuration ChangeAborted, but not in C_old,new")
	}

	c.c_new = Peers{}
	c.state = Old
}
