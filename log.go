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
	store     io.Writer
	entries   []LogEntry
	commitPos int
	apply     func([]byte) ([]byte, error)
}

func NewLog(store io.ReadWriter, apply func([]byte) ([]byte, error)) *Log {
	l := &Log{
		store:     store,
		entries:   []LogEntry{},
		commitPos: -1, // no commits to begin with
		apply:     apply,
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

	pos := 0
	lastTerm := uint64(0)
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index > index {
			break
		}
		lastTerm = l.entries[pos].Term
	}

	a := l.entries[pos:]
	if len(a) == 0 {
		return []LogEntry{}, lastTerm
	}

	return stripResponseChannels(a), lastTerm
}

func stripResponseChannels(a []LogEntry) []LogEntry {
	stripped := make([]LogEntry, len(a))
	for i, entry := range a {
		stripped[i] = LogEntry{
			Index:           entry.Index,
			Term:            entry.Term,
			Command:         entry.Command,
			commandResponse: nil,
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

	if index < l.getCommitIndexWithLock() {
		return ErrIndexTooSmall
	}

	if index > l.lastIndexWithLock() {
		return ErrIndexTooBig
	}

	// It's possible that the passed index is 0. It means the leader has come to
	// decide we need a complete log rebuild. Of course, that's only valid if we
	// haven't committed anything, so this check comes after that one.
	if index == 0 {
		for pos := 0; pos < len(l.entries); pos++ {
			if l.entries[pos].commandResponse != nil {
				close(l.entries[pos].commandResponse)
				l.entries[pos].commandResponse = nil
			}
		}
		l.entries = []LogEntry{}
		return nil
	}

	// Normal case: find the position of the matching log entry.
	pos := 0
	for ; pos < len(l.entries); pos++ {
		if l.entries[pos].Index < index {
			continue // didn't find it yet
		}
		if l.entries[pos].Index > index {
			return ErrBadIndex // somehow went past it
		}
		if l.entries[pos].Index != index {
			panic("not <, not >, but somehow !=")
		}
		if l.entries[pos].Term != term {
			return ErrBadTerm
		}
		break // good
	}

	// Sanity check.
	if pos < l.commitPos {
		panic("index >= commitIndex, but pos < commitPos")
	}

	// `pos` is the position of log entry matching index and term.
	// We want to truncate everything after that.
	truncateFrom := pos + 1
	if truncateFrom >= len(l.entries) {
		return nil // nothing to truncate
	}

	// If we blow away log entries that haven't yet sent responses to clients,
	// signal the clients to stop waiting, by closing the channel without a
	// response value.
	for pos = truncateFrom; pos < len(l.entries); pos++ {
		if l.entries[pos].commandResponse != nil {
			close(l.entries[pos].commandResponse)
			l.entries[pos].commandResponse = nil
		}
	}

	// Truncate the log.
	l.entries = l.entries[:truncateFrom]

	// Done.
	return nil
}

// getCommitIndex returns the commit index of the log. That is, the index of the
// last log entry which can be considered committed.
func (l *Log) getCommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.getCommitIndexWithLock()
}

func (l *Log) getCommitIndexWithLock() uint64 {
	if l.commitPos < 0 {
		return 0
	}
	if l.commitPos >= len(l.entries) {
		panic(fmt.Sprintf("commitPos %d > len(l.entries) %d; bad bookkeeping in Log", l.commitPos, len(l.entries)))
	}
	return l.entries[l.commitPos].Index
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

// appendEntry appends the passed log entry to the log. It will return an error
// if the entry's term is smaller than the log's most recent term, or if the
// entry's index is too small relative to the log's most recent entry.
func (l *Log) appendEntry(entry LogEntry) error {
	l.Lock()
	defer l.Unlock()

	if len(l.entries) > 0 {
		lastTerm := l.lastTermWithLock()
		if entry.Term < lastTerm {
			return ErrTermTooSmall
		}
		lastIndex := l.lastIndexWithLock()
		if entry.Term == lastTerm && entry.Index <= lastIndex {
			return ErrIndexTooSmall
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// commitTo commits all log entries up to and including the passed commitIndex.
// Commit means: synchronize the log entry to persistent storage, and call the
// state machine apply function for the log entry's command.
func (l *Log) commitTo(commitIndex uint64) error {
	if commitIndex == 0 {
		panic("commitTo(0)")
	}

	l.Lock()
	defer l.Unlock()

	// Reject old commit indexes
	if commitIndex < l.getCommitIndexWithLock() {
		return ErrIndexTooSmall
	}

	// Reject new commit indexes
	if commitIndex > l.lastIndexWithLock() {
		return ErrIndexTooBig
	}

	// If we've already committed to the commitIndex, great!
	if commitIndex == l.getCommitIndexWithLock() {
		return nil
	}

	// We should start committing at precisely the last commitPos + 1
	pos := l.commitPos + 1
	if pos < 0 {
		panic("pending commit pos < 0")
	}

	// Commit entries between our existing commit index and the passed index.
	// Remember to include the passed index.
	for {
		// Sanity checks. TODO replace with plain `for` when this is stable.
		if pos >= len(l.entries) {
			panic(fmt.Sprintf("commitTo pos=%d advanced past all log entries (%d)", pos, len(l.entries)))
		}
		if l.entries[pos].Index > commitIndex {
			panic("commitTo advanced past the desired commitIndex")
		}

		// Encode the entry to persistent storage.
		if err := l.entries[pos].encode(l.store); err != nil {
			return err
		}

		// Apply the entry's command to our state machine.
		resp, err := l.apply(l.entries[pos].Command)
		if err != nil {
			return err
		}

		// Transmit the response to waiting client, if applicable.
		if l.entries[pos].commandResponse != nil {
			select {
			case l.entries[pos].commandResponse <- resp: // TODO could `go` this
				//
			case <-time.After(BroadcastInterval()): // << ElectionInterval
				panic("uncoöperative command response receiver")
			}
			close(l.entries[pos].commandResponse)
			l.entries[pos].commandResponse = nil
		}

		// Mark our commit position cursor.
		l.commitPos = pos

		// If that was the last one, we're done.
		if l.entries[pos].Index == commitIndex {
			break
		}
		if l.entries[pos].Index > commitIndex {
			panic(fmt.Sprintf("current entry Index %d is beyond our desired commitIndex %d", l.entries[pos].Index, commitIndex))
		}

		// Otherwise, advance!
		pos++
	}

	// Done.
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
