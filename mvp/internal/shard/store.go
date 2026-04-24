package shard

import (
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/foysal/distkv/internal/kv"
	"github.com/foysal/distkv/internal/lsm"
)

// Shard wraps an LSM-backed KV store and tracks the approximate number of
// live keys for the rebalancer. It is safe for concurrent use.
type Shard struct {
	ID    string
	dir   string
	store *kv.Store

	mu       sync.Mutex
	keyCount int

	// keys keeps a sorted set of the live keys in the shard. The LSM itself
	// does not expose an ordered scan, so we maintain this alongside writes
	// to support midpoint-split and range scans cheaply. For the M5 demo
	// scale (≲ 1M keys per shard) this is fine; a real implementation would
	// use the LSM's existing ordered merge iterator.
	keys map[string]struct{}
}

// OpenShard opens or creates an LSM-backed shard whose data files live under
// dataRoot/<id>.
func OpenShard(dataRoot, id string) (*Shard, error) {
	dir := filepath.Join(dataRoot, id)
	st, err := kv.NewWithOptions(dir, lsm.Options{})
	if err != nil {
		return nil, fmt.Errorf("open shard %s: %w", id, err)
	}
	sh := &Shard{
		ID:    id,
		dir:   dir,
		store: st,
		keys:  make(map[string]struct{}),
	}
	// Best-effort rehydrate key set from the LSM snapshot (used on restart).
	for k := range st.Snapshot() {
		sh.keys[k] = struct{}{}
	}
	sh.keyCount = len(sh.keys)
	return sh, nil
}

// Close flushes and releases the underlying KV store.
func (s *Shard) Close() error { return s.store.Close() }

// Flush pushes the LSM memtable to an L0 SSTable. Used by the backup
// endpoint so the on-disk state is complete before archiving.
func (s *Shard) Flush() error { return s.store.Flush() }

// KeyCount returns the current approximate live-key count.
func (s *Shard) KeyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keyCount
}

// Put writes key=value into the shard.
func (s *Shard) Put(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.Apply(kv.EncodeSet(key, value))
	if _, existed := s.keys[key]; !existed {
		s.keys[key] = struct{}{}
		s.keyCount++
	}
	return nil
}

// Get reads key.
func (s *Shard) Get(key string) (string, bool) {
	return s.store.Get(key)
}

// Delete removes key.
func (s *Shard) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store.Apply(kv.EncodeDelete(key))
	if _, existed := s.keys[key]; existed {
		delete(s.keys, key)
		s.keyCount--
	}
	return nil
}

// Scan returns every (key, value) pair whose key falls inside [start, end),
// sorted ascending by key. Empty start = -inf; empty end = +inf.
func (s *Shard) Scan(start, end string) []KV {
	s.mu.Lock()
	sorted := make([]string, 0, len(s.keys))
	for k := range s.keys {
		if start != "" && k < start {
			continue
		}
		if end != "" && k >= end {
			continue
		}
		sorted = append(sorted, k)
	}
	s.mu.Unlock()
	sort.Strings(sorted)
	out := make([]KV, 0, len(sorted))
	for _, k := range sorted {
		if v, ok := s.store.Get(k); ok {
			out = append(out, KV{Key: k, Value: v})
		}
	}
	return out
}

// sortedKeys returns a sorted snapshot of the current key set.
func (s *Shard) sortedKeys() []string {
	s.mu.Lock()
	ks := make([]string, 0, len(s.keys))
	for k := range s.keys {
		ks = append(ks, k)
	}
	s.mu.Unlock()
	sort.Strings(ks)
	return ks
}

// KV is a single key/value tuple returned by Scan.
type KV struct {
	Key   string
	Value string
}
