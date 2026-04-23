// Package lsm is a small log-structured merge-tree storage engine:
//
//   - writes go into an in-memory sorted Memtable;
//   - when the memtable crosses a size threshold it is flushed to disk as an
//     immutable SSTable at level 0;
//   - reads check the memtable first, then L0 tables newest-to-oldest, then
//     L1 tables; a tombstone short-circuits the search;
//   - when L0 exceeds a table-count threshold, the compactor merges L0 (and
//     any overlapping L1 tables) into a single new L1 run, dropping
//     tombstones.
//
// It is intentionally minimal — no WAL (Raft provides durability above us),
// no block index, no compression, no level count beyond L1. The goal is to
// demonstrate the shape of an LSM inside a Raft state machine.
package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	defaultFlushThresholdBytes = 1 << 20 // 1 MiB
	defaultL0Trigger           = 4       // compact when L0 has this many tables
)

// Options controls thresholds. Zero values mean "use default".
type Options struct {
	FlushThresholdBytes int
	L0Trigger           int
}

// DB is the top-level handle. It is safe for concurrent Get/Put/Delete.
type DB struct {
	dir string

	mu          sync.RWMutex
	mem         *Memtable
	l0          []*SSTable // flushed L0 tables, oldest first
	l1          []*SSTable // compacted L1 tables, sorted by MinKey
	flushThresh int
	l0Trigger   int
	nextID      uint64
}

// Open creates or reopens an LSM database rooted at dir. Existing .sst files
// in the directory are loaded and slotted into their levels based on the
// L<N>- filename prefix.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	db := &DB{
		dir:         dir,
		mem:         NewMemtable(),
		flushThresh: opts.FlushThresholdBytes,
		l0Trigger:   opts.L0Trigger,
	}
	if db.flushThresh <= 0 {
		db.flushThresh = defaultFlushThresholdBytes
	}
	if db.l0Trigger <= 0 {
		db.l0Trigger = defaultL0Trigger
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if filepath.Ext(name) != ".sst" {
			// Clean up any stray tmp files from a crashed build.
			if filepath.Ext(name) == ".tmp" {
				_ = os.Remove(filepath.Join(dir, name))
			}
			continue
		}
		level, id, ok := parseSSTableFilename(name)
		if !ok {
			continue
		}
		t, err := LoadSSTable(filepath.Join(dir, name), id, level)
		if err != nil {
			return nil, fmt.Errorf("load %s: %w", name, err)
		}
		switch level {
		case 0:
			db.l0 = append(db.l0, t)
		case 1:
			db.l1 = append(db.l1, t)
		default:
			_ = t.Close()
		}
		if id >= db.nextID {
			db.nextID = id + 1
		}
	}
	// L0 ordering is oldest→newest, by ID (IDs are monotonic).
	sort.Slice(db.l0, func(i, j int) bool { return db.l0[i].ID < db.l0[j].ID })
	sort.Slice(db.l1, func(i, j int) bool { return db.l1[i].MinKey < db.l1[j].MinKey })

	return db, nil
}

// Close flushes the memtable (best effort) and closes all SSTables. Safe to
// call multiple times.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.mem != nil && db.mem.Len() > 0 {
		if err := db.flushLocked(); err != nil {
			return err
		}
	}
	for _, t := range db.l0 {
		_ = t.Close()
	}
	for _, t := range db.l1 {
		_ = t.Close()
	}
	db.l0 = nil
	db.l1 = nil
	return nil
}

func (db *DB) nextTableID() uint64 {
	id := db.nextID
	db.nextID++
	return id
}

// Put inserts or overwrites a key.
func (db *DB) Put(key, value string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mem.Put(key, value)
	return db.maybeFlushLocked()
}

// Delete writes a tombstone.
func (db *DB) Delete(key string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.mem.Delete(key)
	return db.maybeFlushLocked()
}

// Get follows the standard LSM read path:
//   memtable → L0 newest→oldest → L1 (one table at most covers the key).
// Tombstones short-circuit.
func (db *DB) Get(key string) (string, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if v, tomb, found := db.mem.Get(key); found {
		if tomb {
			return "", false
		}
		return v, true
	}

	// L0 newest first (db.l0 is oldest→newest, so iterate backwards).
	for i := len(db.l0) - 1; i >= 0; i-- {
		t := db.l0[i]
		if v, tomb, found := t.Get(key); found {
			if tomb {
				return "", false
			}
			return v, true
		}
	}

	// L1 tables don't overlap; binary-search by key range.
	if idx := findL1(db.l1, key); idx >= 0 {
		if v, tomb, found := db.l1[idx].Get(key); found {
			if tomb {
				return "", false
			}
			return v, true
		}
	}
	return "", false
}

func findL1(tables []*SSTable, key string) int {
	// Linear is fine for small L1; binary search with sort.Search for O(log n).
	i := sort.Search(len(tables), func(i int) bool { return tables[i].MaxKey >= key })
	if i < len(tables) && tables[i].MinKey <= key && key <= tables[i].MaxKey {
		return i
	}
	return -1
}

// maybeFlushLocked flushes the memtable if it has outgrown the threshold, and
// triggers compaction afterward if L0 is full.
func (db *DB) maybeFlushLocked() error {
	if db.mem.SizeBytes() < db.flushThresh {
		return nil
	}
	if err := db.flushLocked(); err != nil {
		return err
	}
	return db.compactLevelsLocked()
}

// flushLocked turns the current memtable into a new L0 SSTable and swaps in a
// fresh empty memtable. Called with db.mu held.
func (db *DB) flushLocked() error {
	if db.mem.Len() == 0 {
		return nil
	}
	records := db.mem.Flush()
	id := db.nextTableID()
	t, err := writeThenRename(db.dir, 0, id, records)
	if err != nil {
		return err
	}
	db.l0 = append(db.l0, t)
	db.mem = NewMemtable()
	return nil
}

// Flush forces a memtable → L0 flush. Used by snapshot paths that want the
// durable state to reflect everything written so far.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := db.flushLocked(); err != nil {
		return err
	}
	return db.compactLevelsLocked()
}

// SnapshotAll returns a map of every live (non-tombstoned) key→value. It is
// used by the Raft layer for snapshot.json: at this milestone we still dump
// the full KV state, just now sourced from the LSM.
func (db *DB) SnapshotAll() map[string]string {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// Priority order, newest first: memtable, L0 newest→oldest, L1.
	// We iterate oldest→newest and let newer writes overwrite, which is simpler
	// and gives the same result.

	out := make(map[string]string)
	// L1 first (oldest).
	for _, t := range db.l1 {
		recs, err := t.ScanAll()
		if err != nil {
			continue
		}
		for _, r := range recs {
			if r.Tombstone {
				delete(out, r.Key)
			} else {
				out[r.Key] = r.Value
			}
		}
	}
	// L0 oldest→newest.
	for _, t := range db.l0 {
		recs, err := t.ScanAll()
		if err != nil {
			continue
		}
		for _, r := range recs {
			if r.Tombstone {
				delete(out, r.Key)
			} else {
				out[r.Key] = r.Value
			}
		}
	}
	// Memtable last (newest).
	for _, r := range db.mem.Flush() {
		if r.Tombstone {
			delete(out, r.Key)
		} else {
			out[r.Key] = r.Value
		}
	}
	return out
}

// Restore wipes all on-disk SSTables and the memtable, then bulk-loads the
// given map. Used when a Raft snapshot is installed.
func (db *DB) Restore(data map[string]string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	for _, t := range db.l0 {
		_ = t.Close()
		_ = os.Remove(t.Path)
	}
	for _, t := range db.l1 {
		_ = t.Close()
		_ = os.Remove(t.Path)
	}
	db.l0 = nil
	db.l1 = nil
	db.mem = NewMemtable()

	for k, v := range data {
		db.mem.Put(k, v)
	}
	// Force a flush so the restored state is durable without waiting for the
	// threshold to be hit.
	if err := db.flushLocked(); err != nil {
		return err
	}
	return nil
}

// Len returns the approximate number of live keys. Exact when there are no
// tombstones pending GC; otherwise an upper bound.
func (db *DB) Len() int {
	return len(db.SnapshotAll())
}

// Stats is a small debugging helper.
type Stats struct {
	MemKeys  int
	L0Tables int
	L1Tables int
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{
		MemKeys:  db.mem.Len(),
		L0Tables: len(db.l0),
		L1Tables: len(db.l1),
	}
}
