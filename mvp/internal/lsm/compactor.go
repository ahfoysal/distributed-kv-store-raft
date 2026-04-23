package lsm

import (
	"os"
	"sort"
)

// Simple leveled compaction:
//   L0: tables from flushes. May overlap each other (SSTables at L0 can share
//       key ranges). When L0 grows beyond l0Trigger, compact ALL L0 tables
//       together with any overlapping L1 tables into one or more L1 tables.
//   L1: single run of non-overlapping tables by key range.
//
// That's the minimum needed to demonstrate compaction + tombstone GC.

// compactLevels runs at most one compaction cycle if L0 exceeds the trigger,
// and returns the new (possibly changed) table slice. Caller holds db.mu.
func (db *DB) compactLevelsLocked() error {
	if len(db.l0) < db.l0Trigger {
		return nil
	}

	l0 := append([]*SSTable(nil), db.l0...)
	l1 := append([]*SSTable(nil), db.l1...)

	// Collect L1 tables that overlap the combined L0 key range.
	l0Min, l0Max := keyRange(l0)
	var overlapL1 []*SSTable
	var untouchedL1 []*SSTable
	for _, t := range l1 {
		if t.MaxKey < l0Min || t.MinKey > l0Max {
			untouchedL1 = append(untouchedL1, t)
		} else {
			overlapL1 = append(overlapL1, t)
		}
	}

	// Merge: newer tables override older. Priority order from most-recent to
	// oldest: l0 (newest first), then overlapL1 (arbitrary order — L1 tables
	// don't overlap each other so conflicts within L1 can't happen; but L0
	// can have keys that overwrite L1 and that's handled by the priority).
	sources := make([][]Record, 0, len(l0)+len(overlapL1))
	// l0 is stored oldest→newest in db.l0 (appended on flush), so reverse it
	// for priority (newest first).
	for i := len(l0) - 1; i >= 0; i-- {
		recs, err := l0[i].ScanAll()
		if err != nil {
			return err
		}
		sources = append(sources, recs)
	}
	for _, t := range overlapL1 {
		recs, err := t.ScanAll()
		if err != nil {
			return err
		}
		sources = append(sources, recs)
	}

	// k-way merge by earliest key, first occurrence wins (sources[0] is newest).
	merged := kwayMerge(sources)

	// Drop tombstones at L1 (the deepest level in this simple 2-level scheme):
	// once a tombstone has no older SSTable beneath it, the key is truly gone.
	filtered := merged[:0]
	for _, r := range merged {
		if r.Tombstone {
			continue
		}
		filtered = append(filtered, r)
	}

	// Write new L1 table(s). To keep things simple we emit a single L1 table;
	// a production compactor would chunk by target file size.
	var newL1 []*SSTable
	if len(filtered) > 0 {
		id := db.nextTableID()
		t, err := writeThenRename(db.dir, 1, id, filtered)
		if err != nil {
			return err
		}
		newL1 = append(untouchedL1, t)
	} else {
		newL1 = untouchedL1
	}
	// Keep L1 sorted by MinKey for predictable scans.
	sort.Slice(newL1, func(i, j int) bool { return newL1[i].MinKey < newL1[j].MinKey })

	// Delete old files.
	oldTables := append([]*SSTable(nil), l0...)
	oldTables = append(oldTables, overlapL1...)
	for _, t := range oldTables {
		_ = t.Close()
		_ = os.Remove(t.Path)
	}

	db.l0 = nil
	db.l1 = newL1
	return nil
}

func keyRange(tables []*SSTable) (string, string) {
	if len(tables) == 0 {
		return "", ""
	}
	mn, mx := tables[0].MinKey, tables[0].MaxKey
	for _, t := range tables[1:] {
		if t.MinKey < mn {
			mn = t.MinKey
		}
		if t.MaxKey > mx {
			mx = t.MaxKey
		}
	}
	return mn, mx
}

// kwayMerge takes pre-sorted record slices (sources[0] is highest priority)
// and returns a sorted slice of records where the highest-priority occurrence
// of each duplicate key wins.
func kwayMerge(sources [][]Record) []Record {
	// Small N (≤ l0Trigger + a few L1 tables), so a simple min-pick loop is
	// clearer than a heap.
	cursors := make([]int, len(sources))
	var out []Record
	for {
		// Find the smallest key across all cursors.
		bestKey := ""
		bestFound := false
		for i, src := range sources {
			if cursors[i] >= len(src) {
				continue
			}
			k := src[cursors[i]].Key
			if !bestFound || k < bestKey {
				bestKey = k
				bestFound = true
			}
		}
		if !bestFound {
			break
		}
		// Among all sources whose current record == bestKey, the lowest-indexed
		// (highest priority) wins. Advance every matching cursor.
		var winner Record
		winnerFound := false
		for i, src := range sources {
			if cursors[i] >= len(src) {
				continue
			}
			if src[cursors[i]].Key == bestKey {
				if !winnerFound {
					winner = src[cursors[i]]
					winnerFound = true
				}
				cursors[i]++
			}
		}
		out = append(out, winner)
	}
	return out
}
