package raft

import (
	"bytes"
	"math"
	"strings"
	"testing"
)

func TestLogEntriesAfter(t *testing.T) {
	c := []byte(`{}`)
	buf := &bytes.Buffer{}
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	log := NewLog(buf, noop)
	defaultTerm := uint64(5)

	type tuple struct {
		After           uint64
		ExpectedEntries int
		ExpectedTerm    uint64
	}

	for _, tu := range []tuple{
		{0, 0, defaultTerm},
		{1, 0, defaultTerm},
		{2, 0, defaultTerm},
		{3, 0, defaultTerm},
		{4, 0, defaultTerm},
	} {
		entries, term := log.entriesAfter(tu.After, defaultTerm)
		if expected, got := tu.ExpectedEntries, len(entries); expected != got {
			t.Errorf("with %d, After(%d): entries: expected %d got %d", 0, tu.After, expected, got)
		}
		if expected, got := tu.ExpectedTerm, term; expected != got {
			t.Errorf("with %d, After(%d): expected %d got %d", 0, tu.After, expected, got)
		}
	}

	log.appendEntry(LogEntry{1, 1, c})
	for _, tu := range []tuple{
		{0, 1, 1},
		{1, 0, defaultTerm},
		{2, 0, defaultTerm},
		{3, 0, defaultTerm},
		{4, 0, defaultTerm},
	} {
		entries, term := log.entriesAfter(tu.After, defaultTerm)
		if expected, got := tu.ExpectedEntries, len(entries); expected != got {
			t.Errorf("with %d, After(%d): entries: expected %d got %d", 1, tu.After, expected, got)
		}
		if expected, got := tu.ExpectedTerm, term; expected != got {
			t.Errorf("with %d, After(%d): term: expected %d got %d", 1, tu.After, expected, got)
		}
	}

	log.appendEntry(LogEntry{2, 1, c})
	for _, tu := range []tuple{
		{0, 2, 1},
		{1, 1, 1},
		{2, 0, defaultTerm},
		{3, 0, defaultTerm},
		{4, 0, defaultTerm},
	} {
		entries, term := log.entriesAfter(tu.After, defaultTerm)
		if expected, got := tu.ExpectedEntries, len(entries); expected != got {
			t.Errorf("with %d, After(%d): entries: expected %d got %d", 2, tu.After, expected, got)
		}
		if expected, got := tu.ExpectedTerm, term; expected != got {
			t.Errorf("with %d, After(%d): term: expected %d got %d", 2, tu.After, expected, got)
		}
	}

	log.appendEntry(LogEntry{3, 2, c})
	for _, tu := range []tuple{
		{0, 3, 2},
		{1, 2, 2},
		{2, 1, 2},
		{3, 0, defaultTerm},
		{4, 0, defaultTerm},
	} {
		entries, term := log.entriesAfter(tu.After, defaultTerm)
		if expected, got := tu.ExpectedEntries, len(entries); expected != got {
			t.Errorf("with %d, After(%d): entries: expected %d got %d", 3, tu.After, expected, got)
		}
		if expected, got := tu.ExpectedTerm, term; expected != got {
			t.Errorf("with %d, After(%d): term: expected %d got %d", 3, tu.After, expected, got)
		}
	}
}

func TestLogEntryEncodeDecode(t *testing.T) {
	for _, logEntry := range []LogEntry{
		LogEntry{1, 1, []byte(`{}`)},
		LogEntry{1, 2, []byte(`{}`)},
		LogEntry{1, 2, []byte(`{}`)},
		LogEntry{2, 2, []byte(`{}`)},
		LogEntry{255, 3, []byte(`{"cmd": 123}`)},
		LogEntry{math.MaxUint64 - 1, math.MaxUint64, []byte(`{}`)},
	} {
		b := &bytes.Buffer{}
		if err := logEntry.Encode(b); err != nil {
			t.Errorf("%v: Encode: %s", logEntry, err)
			continue
		}
		t.Logf("%v: Encode: %s", logEntry, strings.TrimSpace(b.String()))
		sz := b.Len()

		var e LogEntry
		if n, err := e.Decode(b); err != nil {
			t.Errorf("%v: Decode: %s", logEntry, err)
		} else if n != sz {
			t.Errorf("%v: Decode: expected %d, decoded %d", logEntry, sz, n)
		}

	}
}

func TestLogAppend(t *testing.T) {
	c := []byte(`{}`)
	rc := make(chan []byte, 100)
	buf := &bytes.Buffer{}
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	log := NewLog(buf, noop)

	// Append 3 valid LogEntries
	if err := log.appendEntry(LogEntry{1, 1, c}); err != nil {
		t.Errorf("Append: %s", err)
	}
	if err := log.appendEntry(LogEntry{2, 1, c}); err != nil {
		t.Errorf("Append: %s", err)
	}
	if err := log.appendEntry(LogEntry{3, 2, c}); err != nil {
		t.Errorf("Append: %s", err)
	}

	// Append some invalid LogEntries
	if err := log.appendEntry(LogEntry{4, 1, c}); err != ErrTermTooSmall {
		t.Errorf("Append: expected ErrTermTooSmall, got %v", err)
	}
	if err := log.appendEntry(LogEntry{2, 2, c}); err != ErrIndexTooSmall {
		t.Errorf("Append: expected ErrIndexTooSmall, got %v", nil)
	}

	// Check our flush buffer, before doing any commits
	precommit := ``
	if expected, got := precommit, buf.String(); expected != got {
		t.Errorf("before commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}

	// Commit the first two, only
	if err := log.commitTo(2, rc); err != nil {
		t.Errorf("commitTo: %s", err)
	}

	// Check our flush buffer
	lines := []string{
		`02869b0c 0000000000000001 0000000000000001 {}`,
		`3bfe364c 0000000000000002 0000000000000001 {}`,
	}
	if expected, got := strings.Join(lines, "\n")+"\n", buf.String(); expected != got {
		t.Errorf("after commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}

	// Make some invalid commits
	if err := log.commitTo(1, rc); err != ErrIndexTooSmall {
		t.Errorf("Commit: expected ErrIndexTooSmall, got %v", err)
	}
	if err := log.commitTo(4, rc); err != ErrIndexTooBig {
		t.Errorf("Commit: expected ErrIndexTooBig, got %v", err)
	}

	// Commit every LogEntry
	if err := log.commitTo(3, rc); err != nil {
		t.Errorf("commitTo: %s", err)
	}

	// Check our flush buffer again
	lines = append(
		lines,
		`6b76285c 0000000000000003 0000000000000002 {}`,
	)
	if expected, got := strings.Join(lines, "\n")+"\n", buf.String(); expected != got {
		t.Errorf("after commit, expected:\n%s\ngot:\n%s\n", expected, got)
	}
}

func TestLogContains(t *testing.T) {
	c := []byte(`{}`)
	buf := &bytes.Buffer{}
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	log := NewLog(buf, noop)

	for _, tuple := range []struct {
		Index uint64
		Term  uint64
	}{
		{1, 1},
		{2, 1},
		{3, 2},
	} {
		e := LogEntry{tuple.Index, tuple.Term, c}
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("appendEntry(%v): %s", e, err)
		}
	}

	for _, tuple := range []struct {
		Index    uint64
		Term     uint64
		Expected bool
	}{
		{0, 1, false},
		{1, 0, false},
		{1, 1, true},
		{2, 1, true},
		{1, 2, false},
		{3, 2, true},
		{3, 3, false},
		{3, 4, false},
		{4, 3, false},
		{4, 4, false},
		{4, 1, false},
		{1, 4, false},
	} {
		index, term, expected := tuple.Index, tuple.Term, tuple.Expected
		if got := log.contains(index, term); expected != got {
			t.Errorf("Contains(%d, %d): expected %v, got %v", index, term, expected, got)
		}
	}
}

func TestLogTruncation(t *testing.T) {
	c := []byte(`{}`)
	rc := make(chan []byte, 100)
	buf := &bytes.Buffer{}
	noop := func([]byte) ([]byte, error) { return []byte{}, nil }
	log := NewLog(buf, noop)

	for _, tuple := range []struct {
		Index uint64
		Term  uint64
	}{
		{1, 1},
		{2, 1},
		{3, 2},
	} {
		e := LogEntry{tuple.Index, tuple.Term, c}
		if err := log.appendEntry(e); err != nil {
			t.Fatalf("appendEntry(%v): %s", e, err)
		}
	}

	if err := log.commitTo(2, rc); err != nil {
		t.Fatal(err)
	}

	if expected, got := ErrIndexTooBig, log.ensureLastIs(4, 3); expected != got {
		t.Errorf("expected %s, got %s", expected, got)
	}
	if expected, got := ErrIndexTooSmall, log.ensureLastIs(1, 1); expected != got {
		t.Errorf("expected %s, got %s", expected, got) // before commitIndex
	}
	if expected, got := ErrBadTerm, log.ensureLastIs(3, 4); expected != got {
		t.Errorf("expected %s, got %s", expected, got)
	}

	if err := log.ensureLastIs(3, 2); err != nil {
		t.Fatal(err)
	}
	if err := log.ensureLastIs(2, 1); err != nil {
		t.Fatal(err)
	}
	if log.contains(3, 2) {
		t.Fatal("should have truncated (3,2) but it still exists")
	}
	if !log.contains(2, 1) {
		t.Fatal("(2,1) should still exist but it seems to be missing")
	}
}
