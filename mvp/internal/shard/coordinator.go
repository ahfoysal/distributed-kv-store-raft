package shard

import (
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Router is the client-visible facade: given a key, find the shard and
// execute a Get/Put/Delete against it. It also owns the split policy and
// the background rebalancer goroutine.
type Router struct {
	dataRoot string
	manifest *Manifest

	mu     sync.RWMutex
	shards map[string]*Shard // id → shard

	splitThreshold int
	nextShardID    atomic.Uint64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// Config tunes the coordinator.
type Config struct {
	// DataRoot is the directory under which each shard gets its own subdir.
	DataRoot string
	// ManifestPath is the file holding the persisted shard manifest.
	ManifestPath string
	// SplitThreshold is the key-count at which a shard will be split by the
	// background rebalancer. Zero disables auto-splitting.
	SplitThreshold int
	// RebalanceInterval is how often the rebalancer wakes up. Zero disables
	// the background goroutine entirely (callers can still call Rebalance()
	// by hand).
	RebalanceInterval time.Duration
}

// NewRouter loads or initializes the manifest at cfg.ManifestPath, opens
// every referenced shard under cfg.DataRoot, and kicks off the rebalancer.
func NewRouter(cfg Config) (*Router, error) {
	mf, err := LoadManifest(cfg.ManifestPath)
	if err != nil {
		return nil, err
	}
	r := &Router{
		dataRoot:       cfg.DataRoot,
		manifest:       mf,
		shards:         make(map[string]*Shard),
		splitThreshold: cfg.SplitThreshold,
		stopCh:         make(chan struct{}),
	}
	// Seed nextShardID to one past the highest "sN" id we've seen.
	var maxN uint64
	for _, s := range mf.Snapshot() {
		var n uint64
		if _, err := fmt.Sscanf(s.ID, "s%d", &n); err == nil && n > maxN {
			maxN = n
		}
	}
	r.nextShardID.Store(maxN + 1)

	for _, info := range mf.Snapshot() {
		sh, err := OpenShard(cfg.DataRoot, info.ID)
		if err != nil {
			return nil, err
		}
		r.shards[info.ID] = sh
	}

	if cfg.RebalanceInterval > 0 {
		r.wg.Add(1)
		go r.rebalanceLoop(cfg.RebalanceInterval)
	}
	return r, nil
}

// Manifest returns the router's manifest. Exposed so higher layers (2PC,
// backup) can introspect the current shard list and look up owners for a
// key without going through one of the Put/Get/Scan entry points.
func (r *Router) Manifest() *Manifest { return r.manifest }

// Shard returns the live Shard with id, or nil if no such shard is open.
// Used by the 2PC coordinator to issue direct writes during apply.
func (r *Router) Shard(id string) *Shard {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shards[id]
}

// DataRoot returns the on-disk directory every shard's data lives under.
func (r *Router) DataRoot() string { return r.dataRoot }

// Close stops the rebalancer and closes every shard.
func (r *Router) Close() error {
	close(r.stopCh)
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for _, sh := range r.shards {
		if err := sh.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// shardForKey returns the open Shard that currently owns k.
func (r *Router) shardForKey(k string) (*Shard, ShardInfo) {
	info := r.manifest.Find(k)
	r.mu.RLock()
	sh := r.shards[info.ID]
	r.mu.RUnlock()
	return sh, info
}

// Put routes a write to the shard owning key.
func (r *Router) Put(key, value string) error {
	sh, _ := r.shardForKey(key)
	if sh == nil {
		return fmt.Errorf("no shard for key %q", key)
	}
	return sh.Put(key, value)
}

// Get routes a read to the shard owning key.
func (r *Router) Get(key string) (string, bool) {
	sh, _ := r.shardForKey(key)
	if sh == nil {
		return "", false
	}
	return sh.Get(key)
}

// Delete routes a delete to the shard owning key.
func (r *Router) Delete(key string) error {
	sh, _ := r.shardForKey(key)
	if sh == nil {
		return fmt.Errorf("no shard for key %q", key)
	}
	return sh.Delete(key)
}

// Scan walks every shard whose range overlaps [start, end) and returns the
// union of their scan results, in key order.
func (r *Router) Scan(start, end string) []KV {
	infos := r.manifest.Snapshot()
	var out []KV
	for _, info := range infos {
		// Skip shards that cannot overlap.
		if end != "" && info.Range.Start != "" && info.Range.Start >= end {
			continue
		}
		if start != "" && info.Range.End != "" && info.Range.End <= start {
			continue
		}
		r.mu.RLock()
		sh := r.shards[info.ID]
		r.mu.RUnlock()
		if sh == nil {
			continue
		}
		// Intersect the shard's range with [start, end).
		s := info.Range.Start
		if start > s {
			s = start
		}
		e := info.Range.End
		if end != "" && (e == "" || end < e) {
			e = end
		}
		out = append(out, sh.Scan(s, e)...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Shards returns the current manifest snapshot (id + range + key count).
func (r *Router) Shards() []ShardStatus {
	infos := r.manifest.Snapshot()
	out := make([]ShardStatus, 0, len(infos))
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, info := range infos {
		sh := r.shards[info.ID]
		cnt := 0
		if sh != nil {
			cnt = sh.KeyCount()
		}
		out = append(out, ShardStatus{ShardInfo: info, KeyCount: cnt})
	}
	return out
}

// ShardStatus is ShardInfo + the current key count. Used by /shards API.
type ShardStatus struct {
	ShardInfo
	KeyCount int `json:"key_count"`
}

// Split manually splits shard id at splitKey. Used by the admin API and by
// the rebalancer (which picks the median key as splitKey).
func (r *Router) Split(id, splitKey string) (ShardInfo, ShardInfo, error) {
	// 1. Allocate a new shard id and open a new empty shard.
	newID := fmt.Sprintf("s%d", r.nextShardID.Add(1)-1)
	newShard, err := OpenShard(r.dataRoot, newID)
	if err != nil {
		return ShardInfo{}, ShardInfo{}, err
	}

	// 2. Find the old shard and move every key >= splitKey into the new one.
	r.mu.RLock()
	old := r.shards[id]
	r.mu.RUnlock()
	if old == nil {
		_ = newShard.Close()
		return ShardInfo{}, ShardInfo{}, fmt.Errorf("shard %q not open", id)
	}

	for _, k := range old.sortedKeys() {
		if k < splitKey {
			continue
		}
		v, ok := old.Get(k)
		if !ok {
			continue
		}
		if err := newShard.Put(k, v); err != nil {
			_ = newShard.Close()
			return ShardInfo{}, ShardInfo{}, err
		}
		if err := old.Delete(k); err != nil {
			_ = newShard.Close()
			return ShardInfo{}, ShardInfo{}, err
		}
	}

	// 3. Swap the manifest.
	left, right, err := r.manifest.ReplaceWithSplit(id, newID, splitKey)
	if err != nil {
		_ = newShard.Close()
		return ShardInfo{}, ShardInfo{}, err
	}

	// 4. Register the new shard in the router.
	r.mu.Lock()
	r.shards[newID] = newShard
	r.mu.Unlock()
	return left, right, nil
}

// Rebalance performs one pass of the M5-lite policy: any shard whose key
// count exceeds the threshold is split at its median key. Returns the list
// of splits performed.
func (r *Router) Rebalance() []SplitEvent {
	if r.splitThreshold <= 0 {
		return nil
	}
	var events []SplitEvent
	for _, st := range r.Shards() {
		if st.KeyCount <= r.splitThreshold {
			continue
		}
		r.mu.RLock()
		sh := r.shards[st.ID]
		r.mu.RUnlock()
		if sh == nil {
			continue
		}
		keys := sh.sortedKeys()
		if len(keys) < 2 {
			continue
		}
		splitKey := keys[len(keys)/2]
		if splitKey == st.Range.Start {
			// Degenerate: every key equals the start boundary. Skip.
			continue
		}
		left, right, err := r.Split(st.ID, splitKey)
		if err != nil {
			log.Printf("rebalance: split %s at %q failed: %v", st.ID, splitKey, err)
			continue
		}
		events = append(events, SplitEvent{SplitKey: splitKey, Left: left, Right: right})
	}
	return events
}

// SplitEvent is one shard split performed by Rebalance.
type SplitEvent struct {
	SplitKey string
	Left     ShardInfo
	Right    ShardInfo
}

func (r *Router) rebalanceLoop(interval time.Duration) {
	defer r.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-r.stopCh:
			return
		case <-t.C:
			_ = r.Rebalance()
		}
	}
}
