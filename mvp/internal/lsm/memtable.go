package lsm

import (
	"sort"
	"sync"
)

// entry is what we store in the memtable. Tombstone=true means the key was
// deleted — we have to remember the deletion so that reads don't fall through
// to an older SSTable that still has the key.
type entry struct {
	value     string
	tombstone bool
}

// Memtable is an in-memory map with sorted-iteration support. On flush it
// produces a sorted slice of records for SSTable construction.
//
// We use a plain map + sort-on-flush rather than a skiplist to keep the
// implementation compact; writes are O(1) and flushes are O(n log n). For the
// sizes we deal with in this milestone (≤ a few MB before flush) this is fine.
type Memtable struct {
	mu      sync.RWMutex
	data    map[string]entry
	sizeEst int // rough byte count, used to decide when to flush
}

func NewMemtable() *Memtable {
	return &Memtable{data: make(map[string]entry)}
}

func (m *Memtable) Put(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, ok := m.data[key]
	m.data[key] = entry{value: value}
	if ok {
		m.sizeEst -= len(prev.value)
	} else {
		m.sizeEst += len(key)
	}
	m.sizeEst += len(value)
}

// Delete writes a tombstone. Readers that see a tombstone treat the key as
// absent and STOP — they do not look in older SSTables.
func (m *Memtable) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	prev, ok := m.data[key]
	m.data[key] = entry{tombstone: true}
	if ok {
		m.sizeEst -= len(prev.value)
	} else {
		m.sizeEst += len(key)
	}
}

// Get returns (value, tombstone, found). If found==false the key was never
// written; if tombstone==true the key was explicitly deleted and the caller
// MUST NOT fall through to SSTables.
func (m *Memtable) Get(key string) (string, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.data[key]
	if !ok {
		return "", false, false
	}
	return e.value, e.tombstone, true
}

func (m *Memtable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sizeEst
}

func (m *Memtable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.data)
}

// Record is one key/value/tombstone observation. Flush returns a sorted slice.
type Record struct {
	Key       string
	Value     string
	Tombstone bool
}

// Flush returns the memtable contents sorted by key, suitable for writing an
// SSTable. The memtable is NOT cleared; the caller replaces it with a fresh
// memtable after the SSTable is durable on disk.
func (m *Memtable) Flush() []Record {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.data))
	for k := range m.data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]Record, 0, len(keys))
	for _, k := range keys {
		e := m.data[k]
		out = append(out, Record{Key: k, Value: e.value, Tombstone: e.tombstone})
	}
	return out
}
