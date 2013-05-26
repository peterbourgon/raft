package raft

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"time"
)

var (
	ErrTermTooSmall    = errors.New("term too small")
	ErrIndexTooSmall   = errors.New("index too small")
	ErrIndexTooBig     = errors.New("commit index too big")
	ErrInvalidChecksum = errors.New("invalid checksum")
	ErrInvalidLogLine  = errors.New("invalid log line")
	ErrNoCommand       = errors.New("no command")
	ErrBadIndex        = errors.New("bad index")
	ErrBadTerm         = errors.New("bad term")
)

type Log struct {
	sync.RWMutex
	store       io.Writer
	entries     []LogEntry
	commitIndex uint64
	apply       func([]byte) ([]byte, error)
}

func NewLog(store io.ReadWriter, apply func([]byte) ([]byte, error)) *Log {
	l := &Log{
		store:       store,
		entries:     []LogEntry{},
		commitIndex: 0,
		apply:       apply,
	}
	l.recover(store)
	return l
}

// recover reads from the log's store, to populate the log with log entries
// from persistent storage. It should be called once, at log instantiation.
func (l *Log) recover(r io.Reader) error {
	for {
		var entry LogEntry
		switch err := entry.decode(r); err {
		case io.EOF:
			return nil // successful completion
		default:
			return err // unsuccessful completion
		case nil:
			if err = l.appendEntry(entry); err != nil {
				return err
			}
		}
	}
}

// entriesAfter returns a slice of log entries after (i.e. not including) the
// passed index, and the term of the log entry specified by index, as a
// convenience to the caller. (This function is only used by a leader attempting
// to flush log entries to its followers.)
//
// This function is called to populate an AppendEntries RPC. That implies they
// are destined for a follower, which implies the application of the commands
// should have the response thrown away, which implies we shouldn't pass a
// commandResponse channel (see: commitTo implementation). In the normal case,
// the LogEntries we return here will get serialized as they pass thru their
// transport, and lose their commandResponse channel anyway. But in the case of
// a LocalPeer (or equivalent) this doesn't happen. So, we must make sure to
// proactively strip commandResponse channels.
func (l *Log) entriesAfter(index uint64) ([]LogEntry, uint64) {
	l.RLock()
	defer l.RUnlock()

	i := 0
	lastTerm := uint64(0)
	for ; i < len(l.entries); i++ {
		if l.entries[i].Index > index {
			break
		}
		lastTerm = l.entries[i].Term
	}

	a := l.entries[i:]
	if len(a) == 0 {
		return []LogEntry{}, lastTerm
	}

	return stripResponseChannels(a), lastTerm
}

func stripResponseChannels(a []LogEntry) []LogEntry {
	stripped := make([]LogEntry, len(a))
	for i, entry := range a {
		stripped[i] = LogEntry{
			Index:   entry.Index,
			Term:    entry.Term,
			Command: entry.Command,
		}
	}
	return stripped
}

// contains returns true if a log entry with the given index and term exists in
// the log.
func (l *Log) contains(index, term uint64) bool {
	l.RLock()
	defer l.RUnlock()

	// It's not necessarily true that l.entries[i] has index == i.
	for _, entry := range l.entries {
		if entry.Index == index && entry.Term == term {
			return true
		}
		if entry.Index > index || entry.Term > term {
			break
		}
	}
	return false
}

// ensureLastIs deletes all non-committed log entries after the given index and
// term. It will fail if the given index doesn't exist, has already been
// committed, or doesn't match the given term.
//
// This method satisfies the requirement that a log entry in an AppendEntries
// call precisely follows the accompanying LastLogTerm and LastLogIndex.
func (l *Log) ensureLastIs(index, term uint64) error {
	l.Lock()
	defer l.Unlock()

	// Taken loosely from benbjohnson's impl

	if index < l.commitIndex {
		return ErrIndexTooSmall
	}

	if index > uint64(len(l.entries)) {
		return ErrIndexTooBig
	}

	// It's possible that the passed index is 0. It means the leader has come to
	// decide we need a complete log rebuild. Of course, that's only valid if we
	// haven't committed anything, so this check comes after that one.
	if index == 0 {
		l.entries = []LogEntry{}
		return nil
	}

	entry := l.entries[index-1]
	if entry.Term != term {
		return ErrBadTerm
	}

	// If we blow away log entries that haven't yet sent responses to clients,
	// signal the clients to stop waiting, by closing the channel without a
	// response value.
	for _, deletedEntry := range l.entries[index:] {
		if deletedEntry.commandResponse != nil {
			close(deletedEntry.commandResponse)
			deletedEntry.commandResponse = nil
		}
	}

	l.entries = l.entries[:index]
	return nil
}

// getCommitIndex returns the commit index of the log. That is, the index of the
// last log entry which can be considered committed.
func (l *Log) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.commitIndex
}

// lastIndex returns the index of the most recent log entry.
func (l *Log) lastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndexWithLock()
}

func (l *Log) lastIndexWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// lastTerm returns the term of the most recent log entry.
func (l *Log) lastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTermWithLock()
}

func (l *Log) lastTermWithLock() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// appendEntry appends the passed log entry to the log.
// It will return an error if any condition is violated.
func (l *Log) appendEntry(entry LogEntry) error {
	l.Lock()
	defer l.Unlock()
	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithLock()
		if entry.Term < lastTerm {
			return ErrTermTooSmall
		}
		if entry.Term == lastTerm && entry.Index <= l.lastIndexWithLock() {
			return ErrIndexTooSmall
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// commitTo commits all log entries up to the passed commitIndex. Commit means:
// synchronize the log entry to persistent storage, and call the state machine
// execute function for the log entry's command.
func (l *Log) commitTo(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()

	// Reject old commit indexes
	if commitIndex < l.commitIndex {
		return ErrIndexTooSmall
	}

	// Reject new commit indexes
	if commitIndex > uint64(len(l.entries)) {
		return ErrIndexTooBig
	}

	// Sync entries between our commit index and the passed commit index
	for i := l.commitIndex; i < commitIndex; i++ {
		entry := l.entries[i]
		if err := entry.encode(l.store); err != nil {
			return err
		}
		resp, err := l.apply(entry.Command)
		if err != nil {
			return err
		}
		if entry.commandResponse != nil {
			select {
			case entry.commandResponse <- resp: // TODO might could `go` this
				break
			case <-time.After(BroadcastInterval()): // << ElectionInterval
				panic("uncoöperative command response receiver")
			}
			close(entry.commandResponse)
			entry.commandResponse = nil
		}
		l.commitIndex = entry.Index
	}

	return nil
}

// LogEntry is the atomic unit being managed by the distributed log. A log entry
// always has an index (monotonically increasing), a term in which the Raft
// network leader first sees the entry, and a command. The command is what gets
// executed against the node state machine when the log entry is successfully
// replicated.
type LogEntry struct {
	Index           uint64      `json:"index"`
	Term            uint64      `json:"term"` // when received by leader
	Command         []byte      `json:"command,omitempty"`
	commandResponse chan []byte `json:"-"` // only present on receiver's log
}

// encode serializes the log entry to the passed io.Writer.
func (e *LogEntry) encode(w io.Writer) error {
	if len(e.Command) <= 0 {
		return ErrNoCommand
	}
	if e.Index <= 0 {
		return ErrBadIndex
	}
	if e.Term <= 0 {
		return ErrBadTerm
	}

	buf := &bytes.Buffer{}
	if _, err := fmt.Fprintf(buf, "%016x %016x %s\n", e.Index, e.Term, e.Command); err != nil {
		return err
	}

	checksum := crc32.ChecksumIEEE(buf.Bytes())
	_, err := fmt.Fprintf(w, "%08x %s", checksum, buf.String())
	return err
}

// decode deserializes one log entry from the passed io.Reader.
func (e *LogEntry) decode(r io.Reader) error {
	var readChecksum uint32
	if _, err := fmt.Fscanf(r, "%08x ", &readChecksum); err != nil {
		return err
	}

	if _, err := fmt.Fscanf(r, "%016x ", &e.Index); err != nil {
		return err
	}

	if _, err := fmt.Fscanf(r, "%016x ", &e.Term); err != nil {
		return err
	}

	if err := consumeUntil(r, '\n', &e.Command); err != nil {
		return err
	}

	b := fmt.Sprintf("%016x %016x %s\n", e.Index, e.Term, e.Command)
	computedChecksum := crc32.ChecksumIEEE([]byte(b))
	if computedChecksum != readChecksum {
		return ErrInvalidChecksum
	}

	return nil
}

// consumeUntil does a series of 1-byte Reads from the passed io.Reader
// until it reaches delim, or EOF. This is pretty inefficient.
func consumeUntil(r io.Reader, delim byte, dst *[]byte) error {
	p := make([]byte, 1)
	for {
		n, err := r.Read(p)
		if err != nil {
			return err
		}
		if n != 1 {
			return io.ErrUnexpectedEOF
		}
		if p[0] == delim {
			return nil
		}
		*dst = append(*dst, p[0])
	}
}
