package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Persister owns the on-disk representation of a single Raft node:
//
//	data/<node-id>/state.json   — currentTerm + votedFor (fsync'd on change)
//	data/<node-id>/wal.log      — append-only log of entries (length-prefixed JSON lines)
//	data/<node-id>/snapshot.json — periodic snapshot of KV state + last included index/term
//
// The WAL only ever contains entries with Index > SnapshotLastIndex. When a
// snapshot is taken we atomically rewrite snapshot.json and truncate the WAL.
type Persister struct {
	dir     string
	walFile *os.File
}

type persistedState struct {
	CurrentTerm int    `json:"current_term"`
	VotedFor    string `json:"voted_for"`
	// CommitIndex is persisted so apply-state survives a crash. Strictly this
	// isn't required by the Raft paper — commitIndex can be recomputed — but
	// persisting it lets a node replay WAL on its own without waiting for a
	// fresh majority to re-commit the tail, which is what our M2 kill-and-
	// restart test needs.
	CommitIndex int `json:"commit_index"`
}

// Snapshot is the serialized KV state plus the log index/term it covers.
type Snapshot struct {
	LastIncludedIndex int               `json:"last_included_index"`
	LastIncludedTerm  int               `json:"last_included_term"`
	Data              map[string]string `json:"data"`
}

// LoadedState is the result of replaying all persistent files at startup.
type LoadedState struct {
	CurrentTerm int
	VotedFor    string
	CommitIndex int
	Snapshot    *Snapshot // nil if none
	Entries     []LogEntry
}

func NewPersister(dir string) (*Persister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "wal.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	return &Persister{dir: dir, walFile: f}, nil
}

func (p *Persister) Close() error {
	if p.walFile != nil {
		return p.walFile.Close()
	}
	return nil
}

// SaveState fsyncs currentTerm, votedFor, and commitIndex to state.json atomically.
func (p *Persister) SaveState(term int, votedFor string, commitIndex int) error {
	path := filepath.Join(p.dir, "state.json")
	tmp := path + ".tmp"
	buf, err := json.Marshal(persistedState{CurrentTerm: term, VotedFor: votedFor, CommitIndex: commitIndex})
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// AppendEntry appends a single log entry to the WAL and fsyncs.
// Each record on disk looks like: <json-bytes>\n
// Using JSON-per-line keeps recovery trivial and human-debuggable.
func (p *Persister) AppendEntry(e LogEntry) error {
	buf, err := json.Marshal(e)
	if err != nil {
		return err
	}
	buf = append(buf, '\n')
	if _, err := p.walFile.Write(buf); err != nil {
		return err
	}
	return p.walFile.Sync()
}

// RewriteWAL replaces the current WAL with the given entries (used when
// HandleAppendEntries truncates a conflicting suffix). Done atomically via
// rename + reopen of the file handle.
func (p *Persister) RewriteWAL(entries []LogEntry) error {
	path := filepath.Join(p.dir, "wal.log")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, e := range entries {
		b, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return err
		}
		w.Write(b)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Reopen the append handle.
	if p.walFile != nil {
		p.walFile.Close()
	}
	p.walFile, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	return err
}

// SaveSnapshot atomically writes the snapshot and truncates the WAL to empty,
// since after snapshotting all entries <= LastIncludedIndex are covered.
func (p *Persister) SaveSnapshot(s Snapshot) error {
	path := filepath.Join(p.dir, "snapshot.json")
	tmp := path + ".tmp"
	buf, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// Truncate WAL — caller is responsible for passing only entries
	// with Index > LastIncludedIndex going forward.
	return p.RewriteWAL(nil)
}

// LoadAll reads state.json, snapshot.json (if any), and replays wal.log.
// Entries returned are those with Index > snapshot.LastIncludedIndex.
func (p *Persister) LoadAll() (*LoadedState, error) {
	out := &LoadedState{}

	// state.json
	if buf, err := os.ReadFile(filepath.Join(p.dir, "state.json")); err == nil {
		var ps persistedState
		if err := json.Unmarshal(buf, &ps); err != nil {
			return nil, fmt.Errorf("decode state.json: %w", err)
		}
		out.CurrentTerm = ps.CurrentTerm
		out.VotedFor = ps.VotedFor
		out.CommitIndex = ps.CommitIndex
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// snapshot.json
	if buf, err := os.ReadFile(filepath.Join(p.dir, "snapshot.json")); err == nil {
		var s Snapshot
		if err := json.Unmarshal(buf, &s); err != nil {
			return nil, fmt.Errorf("decode snapshot.json: %w", err)
		}
		out.Snapshot = &s
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	// wal.log
	walPath := filepath.Join(p.dir, "wal.log")
	f, err := os.Open(walPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return out, nil
		}
		return nil, err
	}
	defer f.Close()

	r := bufio.NewReader(f)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			// Strip trailing newline.
			if line[len(line)-1] == '\n' {
				line = line[:len(line)-1]
			}
			if len(line) == 0 {
				if err == io.EOF {
					break
				}
				continue
			}
			var e LogEntry
			if jerr := json.Unmarshal(line, &e); jerr != nil {
				// Truncated/partial trailing record — stop here, it never fsync'd.
				break
			}
			out.Entries = append(out.Entries, e)
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return out, nil
}
