// Package twopc implements a minimal two-phase-commit protocol across the
// shard.Router's local shards. Each shard is treated as an independent
// participant (as if it were its own Raft group); the coordinator persists
// the global commit decision, each participant persists its own prepare
// record, and recovery replays any in-flight transaction to an atomic
// outcome on restart.
//
// Design:
//
//   - PREPARE: each participant writes one line to <shard>/prepare.log with
//     the txn_id + intended writes and fsyncs before acknowledging.
//   - COMMIT/ABORT: the coordinator writes one line to <data-root>/decision.log
//     with txn_id + outcome and fsyncs before instructing participants to
//     apply. This log is the authoritative source of truth.
//   - APPLY: each participant applies the writes to its LSM and then writes
//     a "done" line to its prepare.log. A prepare without a matching done
//     + a decision=commit in the coordinator log = "must apply on recovery".
//     Absent a decision, the prepare is aborted on recovery (participants
//     cannot commit without coordinator blessing).
//
// This is not a real distributed 2PC (it runs inside one process), but the
// durability-and-recovery semantics are the same: we can crash at any moment
// and the transaction either fully applies on every shard or does not appear
// on any shard.
package twopc

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Op is a single write inside a 2PC transaction.
type Op struct {
	Kind  string `json:"kind"` // "put" or "delete"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// ShardWrites is a bucket of operations for one participant shard.
type ShardWrites struct {
	ShardID string `json:"shard_id"`
	Ops     []Op   `json:"ops"`
}

// prepareRecord is what we persist to each participant's prepare.log.
// Each line is one JSON record.
type prepareRecord struct {
	TxnID    string `json:"txn_id"`
	CommitTS uint64 `json:"commit_ts"`
	Ops      []Op   `json:"ops"`
	// Phase advances PREPARE → APPLIED. "aborted" marks an ABORT outcome
	// we observed via the coordinator decision log.
	Phase string `json:"phase"` // "prepared" | "applied" | "aborted"
}

// decisionRecord is what we persist to the coordinator decision log. Writing
// this line is the "commit point" of the distributed txn.
type decisionRecord struct {
	TxnID    string   `json:"txn_id"`
	CommitTS uint64   `json:"commit_ts"`
	Outcome  string   `json:"outcome"` // "commit" | "abort"
	Shards   []string `json:"shards"`  // participants
}

// ParticipantLog owns one shard's prepare log on disk.
type ParticipantLog struct {
	mu   sync.Mutex
	path string
	// pending maps txn_id → latest-phase record for that txn.
	pending map[string]prepareRecord
}

// OpenParticipantLog opens or creates path, replays it into memory.
func OpenParticipantLog(path string) (*ParticipantLog, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	l := &ParticipantLog{path: path, pending: make(map[string]prepareRecord)}
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	for _, line := range splitLines(buf) {
		if len(line) == 0 {
			continue
		}
		var rec prepareRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return nil, fmt.Errorf("parse prepare log line: %w", err)
		}
		l.pending[rec.TxnID] = rec
	}
	return l, nil
}

// Prepare appends a "prepared" record for txn. Fsyncs before returning.
func (l *ParticipantLog) Prepare(txn string, commitTS uint64, ops []Op) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := prepareRecord{TxnID: txn, CommitTS: commitTS, Ops: ops, Phase: "prepared"}
	if err := l.appendLocked(rec); err != nil {
		return err
	}
	l.pending[txn] = rec
	return nil
}

// MarkApplied appends an "applied" line for txn; called after the writes
// have been applied to the shard. Safe to call multiple times.
func (l *ParticipantLog) MarkApplied(txn string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev, ok := l.pending[txn]
	if !ok {
		return fmt.Errorf("MarkApplied: unknown txn %q", txn)
	}
	prev.Phase = "applied"
	if err := l.appendLocked(prev); err != nil {
		return err
	}
	l.pending[txn] = prev
	return nil
}

// MarkAborted appends an "aborted" line for txn.
func (l *ParticipantLog) MarkAborted(txn string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	prev, ok := l.pending[txn]
	if !ok {
		// Nothing to do — we never prepared it or it was already cleaned.
		return nil
	}
	prev.Phase = "aborted"
	if err := l.appendLocked(prev); err != nil {
		return err
	}
	l.pending[txn] = prev
	return nil
}

// Pending returns every prepare record whose phase is still "prepared".
// Used at recovery time to find txns that need a resolution.
func (l *ParticipantLog) Pending() []prepareRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]prepareRecord, 0)
	for _, r := range l.pending {
		if r.Phase == "prepared" {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CommitTS < out[j].CommitTS })
	return out
}

// Lookup returns the record for txn (or false if absent).
func (l *ParticipantLog) Lookup(txn string) (prepareRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.pending[txn]
	return r, ok
}

func (l *ParticipantLog) appendLocked(rec prepareRecord) error {
	buf, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(append(buf, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

func splitLines(buf []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range buf {
		if b == '\n' {
			lines = append(lines, buf[start:i])
			start = i + 1
		}
	}
	if start < len(buf) {
		lines = append(lines, buf[start:])
	}
	return lines
}
