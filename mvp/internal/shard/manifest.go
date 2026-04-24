// Package shard implements M5 range sharding on top of the KV layer.
//
// The key space is partitioned into half-open ranges [start, end). Each range
// is owned by a Shard, which wraps an LSM-backed KV store and tracks its key
// count. A Manifest maps keys → shards via binary search on the range bounds.
//
// M5-lite policy: when a shard's key count exceeds a configurable threshold
// the Coordinator runs a midpoint split — it picks the median key in the
// shard, creates a new shard whose data lives on disk next to the old one,
// copies the upper half of the keys into it, deletes them from the original,
// and atomically swaps the updated range map into the Manifest. The full
// per-shard-Raft-group design from the spec is future work; see
// "Known limitations" in the README.
package shard

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Range is the half-open key interval [Start, End). An empty Start means
// "-infinity"; an empty End means "+infinity". End > Start in lexicographic
// order whenever both are non-empty.
type Range struct {
	Start string `json:"start"`
	End   string `json:"end"`
}

// Contains reports whether key k falls inside r.
func (r Range) Contains(k string) bool {
	if r.Start != "" && k < r.Start {
		return false
	}
	if r.End != "" && k >= r.End {
		return false
	}
	return true
}

func (r Range) String() string {
	s := r.Start
	if s == "" {
		s = "-inf"
	}
	e := r.End
	if e == "" {
		e = "+inf"
	}
	return fmt.Sprintf("[%s, %s)", s, e)
}

// ShardInfo is the manifest entry for one shard. The actual Shard object
// lives in the Store; the manifest only carries metadata.
type ShardInfo struct {
	ID    string `json:"id"`
	Range Range  `json:"range"`
}

// Manifest is the set of shards ordered by Range.Start. It is persisted to
// disk as JSON and atomically rewritten (tmp + rename) on every change.
type Manifest struct {
	mu     sync.RWMutex
	path   string
	Shards []ShardInfo `json:"shards"`
}

// LoadManifest opens (or initializes) the manifest file at path. If the file
// does not exist, the manifest is seeded with a single shard covering the
// entire key space (id = "s0").
func LoadManifest(path string) (*Manifest, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	m := &Manifest{path: path}
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		m.Shards = []ShardInfo{{ID: "s0", Range: Range{}}}
		if err := m.flushLocked(); err != nil {
			return nil, err
		}
		return m, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(buf, m); err != nil {
		return nil, fmt.Errorf("manifest parse: %w", err)
	}
	return m, nil
}

// Snapshot returns a copy of the shard list, safe for iteration.
func (m *Manifest) Snapshot() []ShardInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]ShardInfo, len(m.Shards))
	copy(out, m.Shards)
	return out
}

// Find returns the ShardInfo that owns key k. Panics if the manifest is
// malformed and no shard covers k (shouldn't happen for a well-formed
// manifest that includes an open-ended final range).
func (m *Manifest) Find(k string) ShardInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	// Shards are sorted by Start ascending; do a binary search on End.
	n := len(m.Shards)
	idx := sort.Search(n, func(i int) bool {
		end := m.Shards[i].Range.End
		return end == "" || k < end
	})
	if idx == n {
		// Should be impossible if the last shard has End="".
		return m.Shards[n-1]
	}
	return m.Shards[idx]
}

// ReplaceWithSplit replaces the shard with id oldID by two shards that
// together cover the same range, split at splitKey. The new shard list is
// persisted before being swapped in.
//
// Returns (leftInfo, rightInfo). The left shard keeps the old id; the right
// shard gets the caller-supplied newID. splitKey must be strictly greater
// than the old range's Start and strictly less than its End (or End="").
func (m *Manifest) ReplaceWithSplit(oldID, newID, splitKey string) (ShardInfo, ShardInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, s := range m.Shards {
		if s.ID == oldID {
			idx = i
			break
		}
	}
	if idx < 0 {
		return ShardInfo{}, ShardInfo{}, fmt.Errorf("shard %q not found", oldID)
	}
	old := m.Shards[idx]
	if splitKey <= old.Range.Start {
		return ShardInfo{}, ShardInfo{}, fmt.Errorf("split key %q <= range start %q", splitKey, old.Range.Start)
	}
	if old.Range.End != "" && splitKey >= old.Range.End {
		return ShardInfo{}, ShardInfo{}, fmt.Errorf("split key %q >= range end %q", splitKey, old.Range.End)
	}
	left := ShardInfo{ID: old.ID, Range: Range{Start: old.Range.Start, End: splitKey}}
	right := ShardInfo{ID: newID, Range: Range{Start: splitKey, End: old.Range.End}}
	next := make([]ShardInfo, 0, len(m.Shards)+1)
	next = append(next, m.Shards[:idx]...)
	next = append(next, left, right)
	next = append(next, m.Shards[idx+1:]...)
	m.Shards = next
	if err := m.flushLocked(); err != nil {
		return ShardInfo{}, ShardInfo{}, err
	}
	return left, right, nil
}

// flushLocked writes the current shard list to disk atomically (tmp+rename).
// Caller must hold m.mu for write.
func (m *Manifest) flushLocked() error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + ".tmp"
	if err := os.WriteFile(tmp, buf, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, m.path)
}
