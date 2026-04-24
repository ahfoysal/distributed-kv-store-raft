// Package kv is the Raft state machine. As of M3 it delegates all durable
// storage to an LSM (see internal/lsm): Put/Delete operations go through the
// memtable and eventually flush to SSTables; Get walks memtable → L0 → L1.
//
// The public surface is unchanged so Raft can continue to call Apply /
// Snapshot / Restore the same way it did in M2.
package kv

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/foysal/distkv/internal/lsm"
)

type Op struct {
	Kind  string `json:"kind"` // "set" | "delete" | "txn_commit"
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
	// TxnPayload carries a JSON-encoded mvcc.CommitBatch when Kind == "txn_commit".
	// We keep it opaque here to avoid a dependency on internal/mvcc from the
	// base kv package (the applier in main.go decodes and routes it).
	TxnPayload json.RawMessage `json:"txn,omitempty"`
}

// Store is the KV state machine backed by an LSM.
type Store struct {
	db *lsm.DB

	// TxnApplier, if set, is invoked for Op{Kind:"txn_commit"} commands. It
	// receives the opaque TxnPayload (an mvcc.CommitBatch). Set by main.go when
	// MVCC is wired in.
	TxnApplier func(payload []byte)
}

// New opens (or creates) an LSM-backed KV store at dir. Passing "" returns an
// error; callers that want an ephemeral store should pass a t.TempDir().
func New(dir string) (*Store, error) {
	return NewWithOptions(dir, lsm.Options{})
}

// NewWithOptions is like New but allows overriding the LSM thresholds.
func NewWithOptions(dir string, opts lsm.Options) (*Store, error) {
	if dir == "" {
		return nil, fmt.Errorf("kv.New: dir is required")
	}
	db, err := lsm.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close flushes and releases underlying files.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Get(key string) (string, bool) {
	return s.db.Get(key)
}

// Apply decodes a command and applies it to the LSM. Errors are logged but do
// not propagate — Raft's applyLoop has no channel for per-entry errors, and a
// malformed command is treated as a no-op (same behaviour as M2).
func (s *Store) Apply(command string) {
	var op Op
	if err := json.Unmarshal([]byte(command), &op); err != nil {
		return
	}
	switch op.Kind {
	case "set":
		if err := s.db.Put(op.Key, op.Value); err != nil {
			log.Printf("kv.Apply put %q: %v", op.Key, err)
		}
	case "delete":
		if err := s.db.Delete(op.Key); err != nil {
			log.Printf("kv.Apply delete %q: %v", op.Key, err)
		}
	case "txn_commit":
		if s.TxnApplier != nil {
			s.TxnApplier([]byte(op.TxnPayload))
		}
	}
}

func EncodeSet(key, value string) string {
	b, _ := json.Marshal(Op{Kind: "set", Key: key, Value: value})
	return string(b)
}

func EncodeDelete(key string) string {
	b, _ := json.Marshal(Op{Kind: "delete", Key: key})
	return string(b)
}

// EncodeTxnCommit wraps a serialized mvcc.CommitBatch as a Raft command.
func EncodeTxnCommit(payload []byte) string {
	b, _ := json.Marshal(Op{Kind: "txn_commit", TxnPayload: json.RawMessage(payload)})
	return string(b)
}

// Snapshot returns a point-in-time copy of every live key so Raft can
// serialize it to snapshot.json. We still dump a full map here (M3 retains the
// M2 snapshot format) — a future milestone can switch to shipping SSTables.
func (s *Store) Snapshot() map[string]string {
	return s.db.SnapshotAll()
}

// Restore replaces the LSM state with the given map. Called at startup after
// snapshot.json is loaded, and after any future InstallSnapshot RPC.
func (s *Store) Restore(data map[string]string) {
	if err := s.db.Restore(data); err != nil {
		log.Printf("kv.Restore: %v", err)
	}
}

// Flush forces any buffered memtable contents out to an L0 SSTable, so a
// subsequent process kill cannot lose them. Called before taking a Raft
// snapshot so SnapshotAll → Restore is a fixed point.
func (s *Store) Flush() error { return s.db.Flush() }

// Len returns the number of live keys (best-effort).
func (s *Store) Len() int { return s.db.Len() }

// Stats exposes LSM-level counters for debugging / tests.
func (s *Store) Stats() lsm.Stats { return s.db.Stats() }
