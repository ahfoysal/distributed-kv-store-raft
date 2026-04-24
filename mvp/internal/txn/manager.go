// Package txn implements optimistic MVCC transactions with serializable
// snapshot isolation (SSI) on top of the mvcc.Store.
//
// Protocol (per the M4 plan):
//
//	BEGIN  -> allocate read_ts = HLC.Now(), return txn_id
//	GET    -> record key in read set, return store.GetAt(key, read_ts)
//	PUT    -> buffer in write set (never visible to other txns)
//	COMMIT -> validate: for every key in the read set, no committed write must
//	          exist in (read_ts, commit_ts]. If validation passes, apply the
//	          write set at commit_ts. Otherwise abort the transaction.
//	ABORT  -> drop the buffered state.
//
// Optimistic OCC with snapshot-time reads gives serializable execution: a
// transaction either sees a consistent snapshot at read_ts AND wrote to a
// world where no conflicting write landed in its window, or it aborts.
package txn

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"sync"

	"github.com/foysal/distkv/internal/mvcc"
)

// Txn holds the per-transaction state.
type Txn struct {
	ID       string
	ReadTS   uint64
	ReadSet  map[string]struct{}
	WriteSet map[string]mvcc.WriteOp // keyed by key; later PUTs overwrite earlier ones
	Aborted  bool
	Done     bool
}

// Manager owns all open transactions.
type Manager struct {
	mu    sync.Mutex
	hlc   *HLC
	store *mvcc.Store
	txns  map[string]*Txn
}

func NewManager(store *mvcc.Store, hlc *HLC) *Manager {
	return &Manager{
		hlc:   hlc,
		store: store,
		txns:  make(map[string]*Txn),
	}
}

var (
	ErrTxnNotFound = errors.New("txn not found")
	ErrTxnDone     = errors.New("txn already committed or aborted")
	ErrAborted     = errors.New("txn aborted: serialization conflict (SSI)")
)

// Begin allocates a fresh transaction and returns its id + read timestamp.
func (m *Manager) Begin() *Txn {
	m.mu.Lock()
	defer m.mu.Unlock()
	id := newTxnID()
	t := &Txn{
		ID:       id,
		ReadTS:   m.hlc.Now(),
		ReadSet:  make(map[string]struct{}),
		WriteSet: make(map[string]mvcc.WriteOp),
	}
	m.txns[id] = t
	return t
}

// Get reads a key from the transaction's snapshot. Writes buffered earlier in
// the same transaction shadow the snapshot (read-your-own-writes).
func (m *Manager) Get(id, key string) (string, bool, error) {
	m.mu.Lock()
	t, ok := m.txns[id]
	m.mu.Unlock()
	if !ok {
		return "", false, ErrTxnNotFound
	}
	if t.Done {
		return "", false, ErrTxnDone
	}
	// Record in read set BEFORE returning, even if the key does not exist —
	// phantom writes to a key this txn thought was absent are still SSI
	// conflicts.
	t.ReadSet[key] = struct{}{}

	// Read-your-own-writes.
	if w, ok := t.WriteSet[key]; ok {
		if w.Tombstone {
			return "", false, nil
		}
		return w.Value, true, nil
	}
	v, found := m.store.GetAt(key, t.ReadTS)
	return v, found, nil
}

// Put buffers a write in the transaction's write set.
func (m *Manager) Put(id, key, value string) error {
	m.mu.Lock()
	t, ok := m.txns[id]
	m.mu.Unlock()
	if !ok {
		return ErrTxnNotFound
	}
	if t.Done {
		return ErrTxnDone
	}
	t.WriteSet[key] = mvcc.WriteOp{Key: key, Value: value}
	return nil
}

// Delete buffers a tombstone in the write set.
func (m *Manager) Delete(id, key string) error {
	m.mu.Lock()
	t, ok := m.txns[id]
	m.mu.Unlock()
	if !ok {
		return ErrTxnNotFound
	}
	if t.Done {
		return ErrTxnDone
	}
	t.WriteSet[key] = mvcc.WriteOp{Key: key, Tombstone: true}
	return nil
}

// Abort drops the transaction's buffered state.
func (m *Manager) Abort(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.txns[id]
	if !ok {
		return ErrTxnNotFound
	}
	t.Aborted = true
	t.Done = true
	delete(m.txns, id)
	return nil
}

// Commit runs the SSI validator, and if it passes, builds a CommitBatch at a
// fresh commit timestamp. The caller is responsible for durably replicating
// the batch (through Raft) before calling ApplyCommitted to install it.
//
// Returns (batch, committed, err). If committed == false the transaction was
// aborted due to an SSI conflict.
func (m *Manager) Commit(id string) (mvcc.CommitBatch, bool, error) {
	// We serialize the validate+allocate step under m.mu so that two concurrent
	// commits cannot both pass validation against the same pre-commit snapshot
	// and then both install their writes. Conceptually this is the "commit
	// queue" every serializable OCC database needs.
	m.mu.Lock()
	defer m.mu.Unlock()

	t, ok := m.txns[id]
	if !ok {
		return mvcc.CommitBatch{}, false, ErrTxnNotFound
	}
	if t.Done {
		return mvcc.CommitBatch{}, false, ErrTxnDone
	}

	commitTS := m.hlc.Now()

	// Read-only transactions skip validation entirely — their snapshot read
	// was consistent by construction.
	if len(t.WriteSet) > 0 {
		for k := range t.ReadSet {
			if m.store.HasWriteInRange(k, t.ReadTS, commitTS) {
				t.Aborted = true
				t.Done = true
				delete(m.txns, id)
				return mvcc.CommitBatch{}, false, ErrAborted
			}
		}
		// Also check write-write conflicts: every key we are about to write,
		// nobody else may have written in (read_ts, commit_ts].
		for k := range t.WriteSet {
			if _, inReadSet := t.ReadSet[k]; inReadSet {
				continue // already checked above
			}
			if m.store.HasWriteInRange(k, t.ReadTS, commitTS) {
				t.Aborted = true
				t.Done = true
				delete(m.txns, id)
				return mvcc.CommitBatch{}, false, ErrAborted
			}
		}
	}

	// Validation passed. Build the batch.
	writes := make([]mvcc.WriteOp, 0, len(t.WriteSet))
	for _, w := range t.WriteSet {
		writes = append(writes, w)
	}
	batch := mvcc.CommitBatch{CommitTS: commitTS, Writes: writes}

	t.Done = true
	delete(m.txns, id)
	return batch, true, nil
}

func newTxnID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return "t-" + hex.EncodeToString(b[:])
}
