package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestBasicPutGetDelete covers the three core operations without forcing any
// flushes: everything stays in the memtable.
func TestBasicPutGetDelete(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put("alpha", "1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := db.Put("beta", "2"); err != nil {
		t.Fatalf("put: %v", err)
	}

	if v, ok := db.Get("alpha"); !ok || v != "1" {
		t.Fatalf("alpha = (%q, %v), want (1, true)", v, ok)
	}
	if v, ok := db.Get("beta"); !ok || v != "2" {
		t.Fatalf("beta = (%q, %v), want (2, true)", v, ok)
	}
	if _, ok := db.Get("missing"); ok {
		t.Fatal("missing should not exist")
	}

	if err := db.Delete("alpha"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, ok := db.Get("alpha"); ok {
		t.Fatal("alpha should be gone after delete")
	}
}

// TestFlushAndRead inserts enough records to force several memtable flushes,
// then reads them all back, then reopens the DB and reads them again.
func TestFlushAndRead(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 4 * 1024}) // tiny to force flushes
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const N = 1000
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%05d", i)
		v := fmt.Sprintf("value-%05d-%s", i, strings.Repeat("x", 32))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	stats := db.Stats()
	if stats.L0Tables == 0 && stats.L1Tables == 0 {
		t.Fatalf("expected some SSTables after %d writes, got %+v", N, stats)
	}

	// Every key reads back.
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%05d", i)
		wantV := fmt.Sprintf("value-%05d-%s", i, strings.Repeat("x", 32))
		if v, ok := db.Get(k); !ok || v != wantV {
			t.Fatalf("get %s = (%q, %v), want (%q, true)", k, v, ok, wantV)
		}
	}

	// Close and reopen — persistence check.
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	db2, err := Open(dir, Options{FlushThresholdBytes: 4 * 1024})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%05d", i)
		wantV := fmt.Sprintf("value-%05d-%s", i, strings.Repeat("x", 32))
		if v, ok := db2.Get(k); !ok || v != wantV {
			t.Fatalf("reopen get %s = (%q, %v), want (%q, true)", k, v, ok, wantV)
		}
	}

	// There should be at least one .sst file on disk.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	ssts := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			ssts++
		}
	}
	if ssts == 0 {
		t.Fatal("expected at least one .sst file on disk after reopen")
	}
}

// TestTombstoneAcrossLevels verifies that a delete in a newer memtable/SSTable
// masks a value in an older SSTable.
func TestTombstoneAcrossLevels(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 1}) // flush on every put
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.Put("foo", "bar"); err != nil {
		t.Fatal(err)
	}
	// First put flushed into an L0 SSTable.
	if v, ok := db.Get("foo"); !ok || v != "bar" {
		t.Fatalf("foo = (%q, %v), want (bar, true)", v, ok)
	}
	if err := db.Delete("foo"); err != nil {
		t.Fatal(err)
	}
	// Delete produced a newer SSTable with a tombstone — read must NOT fall
	// through to the older table.
	if v, ok := db.Get("foo"); ok {
		t.Fatalf("foo should be deleted, got %q", v)
	}
}

// TestCompaction forces enough L0 tables to trigger compaction and verifies
// that L0 shrinks, L1 grows, and reads still return the right values.
func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 1, L0Trigger: 3})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Each Put forces a flush (threshold=1), so after N puts L0 has N tables
	// until compaction merges them into L1.
	const N = 12
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%02d", i)
		if err := db.Put(k, fmt.Sprintf("v%02d", i)); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	stats := db.Stats()
	if stats.L1Tables == 0 {
		t.Fatalf("expected L1 to be populated after %d flushes with trigger=3, got %+v", N, stats)
	}
	if stats.L0Tables >= N {
		t.Fatalf("expected L0 to have been compacted, got %d tables", stats.L0Tables)
	}

	// Every key still readable.
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("k%02d", i)
		wantV := fmt.Sprintf("v%02d", i)
		if v, ok := db.Get(k); !ok || v != wantV {
			t.Fatalf("get %s = (%q, %v), want (%q, true)", k, v, ok, wantV)
		}
	}
}

// TestOverwriteSemantics checks that an overwrite returns the latest value no
// matter whether the old version lives in a different SSTable/level.
func TestOverwriteSemantics(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 1, L0Trigger: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := db.Put("x", "old"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("y", "y-old"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("x", "mid"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("z", "z-old"); err != nil {
		t.Fatal(err)
	}
	if err := db.Put("x", "new"); err != nil {
		t.Fatal(err)
	}

	if v, ok := db.Get("x"); !ok || v != "new" {
		t.Fatalf("x = (%q, %v), want (new, true)", v, ok)
	}
	if v, ok := db.Get("y"); !ok || v != "y-old" {
		t.Fatalf("y = (%q, %v), want (y-old, true)", v, ok)
	}
}

// Test10kKeysPersist is the integration test called for by M3:
// insert 10,000 keys, verify reads, close-and-reopen, verify again, and
// assert SSTables exist on disk.
func Test10kKeysPersist(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 64 * 1024, L0Trigger: 4})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const N = 10000
	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%07d", i)
		v := fmt.Sprintf("v-%d", i*7+3)
		if err := db.Put(k, v); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%07d", i)
		wantV := fmt.Sprintf("v-%d", i*7+3)
		if v, ok := db.Get(k); !ok || v != wantV {
			t.Fatalf("pre-restart get %s = (%q, %v)", k, v, ok)
		}
	}

	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// On-disk SSTables must exist.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir: %v", err)
	}
	ssts := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".sst" {
			ssts++
		}
	}
	if ssts == 0 {
		t.Fatal("expected at least one .sst file on disk")
	}
	t.Logf("persisted to %d SSTable file(s)", ssts)

	// Reopen and re-verify everything.
	db2, err := Open(dir, Options{FlushThresholdBytes: 64 * 1024, L0Trigger: 4})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()

	for i := 0; i < N; i++ {
		k := fmt.Sprintf("key-%07d", i)
		wantV := fmt.Sprintf("v-%d", i*7+3)
		if v, ok := db2.Get(k); !ok || v != wantV {
			t.Fatalf("post-restart get %s = (%q, %v)", k, v, ok)
		}
	}

	t.Logf("10k keys verified before and after restart, stats=%+v", db2.Stats())
}

// TestRestore simulates Raft installing a snapshot: Restore wipes the LSM and
// bulk-loads the given map.
func TestRestore(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, Options{FlushThresholdBytes: 4 * 1024})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 200; i++ {
		_ = db.Put(fmt.Sprintf("old-%d", i), "x")
	}

	fresh := map[string]string{
		"snap-a": "1",
		"snap-b": "2",
		"snap-c": "3",
	}
	if err := db.Restore(fresh); err != nil {
		t.Fatalf("restore: %v", err)
	}

	for k, wantV := range fresh {
		if v, ok := db.Get(k); !ok || v != wantV {
			t.Fatalf("after restore %s = (%q, %v), want (%q, true)", k, v, ok, wantV)
		}
	}
	if _, ok := db.Get("old-0"); ok {
		t.Fatal("old keys should be gone after restore")
	}
}
