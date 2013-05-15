package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
)

var (
	ErrTermTooSmall    = errors.New("term too small")
	ErrIndexTooSmall   = errors.New("index too small")
	ErrIndexTooBig     = errors.New("commit index too big")
	ErrInvalidChecksum = errors.New("invalid checksum")
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

func NewLog(store io.Writer, apply func([]byte) ([]byte, error)) *Log {
	return &Log{
		store:       store,
		entries:     []LogEntry{},
		commitIndex: 0,
		apply:       apply,
	}
}

func (l *Log) EntriesAfter(index, defaultTerm uint64) ([]LogEntry, uint64) {
	l.RLock()
	defer l.RUnlock()
	return l.entriesAfter(index, defaultTerm)
}

func (l *Log) entriesAfter(index, defaultTerm uint64) ([]LogEntry, uint64) {
	i := 0
	for ; i < len(l.entries); i++ {
		if l.entries[i].Index > index {
			break
		}
	}
	a := l.entries[i:]
	if len(a) == 0 {
		return a, defaultTerm
	}
	return a, a[len(a)-1].Term
}

// Contains returns true if a LogEntry with the given index and term exists in
// the log.
func (l *Log) Contains(index, term uint64) bool {
	l.RLock()
	defer l.RUnlock()
	return l.contains(index, term)
}

func (l *Log) contains(index, term uint64) bool {
	if index == 0 || uint64(len(l.entries)) < index {
		return false
	}
	return l.entries[index-1].Term == term
}

// EnsureLastIs deletes all non-committed LogEntries after the given index and
// term. It will fail if the given index doesn't exist, has already been
// committed, or doesn't match the given term.
//
// This method satisfies the requirement that a LogEntry in an AppendEntries
// call precisely follows the accompanying LastLogTerm and LastLogIndex.
func (l *Log) EnsureLastIs(index, term uint64) error {
	l.Lock()
	defer l.Unlock()
	return l.ensureLastIs(index, term)
}

func (l *Log) ensureLastIs(index, term uint64) error {
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

	l.entries = l.entries[:index]
	return nil
}

func (l *Log) CommitIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.commitIndex
}

// LastIndex returns the index of the most recent log entry.
func (l *Log) LastIndex() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastIndex()
}

func (l *Log) lastIndex() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// LastTerm returns the term of the most recent log entry.
func (l *Log) LastTerm() uint64 {
	l.RLock()
	defer l.RUnlock()
	return l.lastTerm()
}

func (l *Log) lastTerm() uint64 {
	if len(l.entries) <= 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Term
}

// AppendEntry appends the passed log entry to the log.
// It will return an error if any condition is violated.
func (l *Log) AppendEntry(entry LogEntry) error {
	l.Lock()
	defer l.Unlock()
	return l.appendEntry(entry)
}

func (l *Log) appendEntry(entry LogEntry) error {
	if len(l.entries) > 0 {
		lastTerm := l.lastTerm()
		if entry.Term < lastTerm {
			return ErrTermTooSmall
		}
		if entry.Term == lastTerm && entry.Index <= l.lastIndex() {
			return ErrIndexTooSmall
		}
	}

	l.entries = append(l.entries, entry)
	return nil
}

// CommitTo commits all log entries up to the passed commitIndex. Commit means:
// synchronize the log entry to persistent storage, and call the state machine
// execute function for the log entry's command.
func (l *Log) CommitTo(commitIndex uint64) error {
	l.Lock()
	defer l.Unlock()
	return l.commitTo(commitIndex)
}

func (l *Log) commitTo(commitIndex uint64) error {
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
		if err := entry.Encode(l.store); err != nil {
			return err
		}
		if _, err := l.apply(entry.Command); err != nil {
			return err
		}
		l.commitIndex = entry.Index
	}

	return nil
}

// LogEntry is the atomic unit being managed by the distributed log.
// A LogEntry always has an index (monotonically increasing), a Term in which
// the Raft network leader first sees the entry, and a Command. The Command is
// what gets executed against the Raft node's state machine when the LogEntry
// is replicated throughout the Raft network.
type LogEntry struct {
	Index   uint64 `json:"index"`
	Term    uint64 `json:"term"` // when received by leader
	Command []byte `json:"command,omitempty"`
}

// Encode serializes the log entry to the passed io.Writer.
func (e *LogEntry) Encode(w io.Writer) error {
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

// Decode deserializes the log entry from the passed io.Reader.
// Decode returns the number of bytes read.
func (e *LogEntry) Decode(r io.Reader) (int, error) {
	pos := 0

	// Read the expected checksum first.
	var readChecksum uint32
	if _, err := fmt.Fscanf(r, "%08x", &readChecksum); err != nil {
		return pos, err
	}
	pos += 8

	// Read the rest of the line.
	rd := bufio.NewReader(r)
	if c, _ := rd.ReadByte(); c != ' ' {
		return pos, fmt.Errorf("LogEntry: Decode: expected space, got %02x", c)
	}
	pos += 1

	line, err := rd.ReadString('\n')
	pos += len(line)
	if err == io.EOF {
		return pos, err
	} else if err != nil {
		return pos, err
	}
	b := bytes.NewBufferString(line)

	computedChecksum := crc32.ChecksumIEEE(b.Bytes())
	if readChecksum != computedChecksum {
		return pos, ErrInvalidChecksum
	}

	if _, err = fmt.Fscanf(b, "%016x %016x ", &e.Index, &e.Term); err != nil {
		return pos, fmt.Errorf("LogEntry: Decode: scan: %s", err)
	}
	e.Command, err = b.ReadBytes('\n')
	if err != nil {
		return pos, err
	}
	bytes.TrimSpace(e.Command)

	// Make sure there's only an EOF remaining.
	c, err := b.ReadByte()
	if err != io.EOF {
		return pos, fmt.Errorf("LogEntry: Decode: expected EOF, got %02x", c)
	}

	return pos, nil
}
