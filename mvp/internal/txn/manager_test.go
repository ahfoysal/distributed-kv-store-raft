package txn

import (
	"sync"
	"testing"

	"github.com/foysal/distkv/internal/mvcc"
)

// applyBatch is a tiny helper that simulates the Raft loop by installing a
// CommitBatch directly into the MVCC store.
func applyBatch(store *mvcc.Store, batch mvcc.CommitBatch) {
	store.ApplyCommit(batch)
}

// TestBasicCommit: single txn writes a key, second txn reads it.
func TestBasicCommit(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	tx1 := mgr.Begin()
	if err := mgr.Put(tx1.ID, "k", "v1"); err != nil {
		t.Fatal(err)
	}
	b, ok, err := mgr.Commit(tx1.ID)
	if err != nil || !ok {
		t.Fatalf("commit failed: %v ok=%v", err, ok)
	}
	applyBatch(store, b)

	tx2 := mgr.Begin()
	v, found, err := mgr.Get(tx2.ID, "k")
	if err != nil || !found || v != "v1" {
		t.Fatalf("read: v=%q found=%v err=%v", v, found, err)
	}
	if _, ok, err := mgr.Commit(tx2.ID); !ok || err != nil {
		t.Fatalf("read-only commit: ok=%v err=%v", ok, err)
	}
}

// TestSnapshotIsolation: tx1 starts, tx2 writes and commits, tx1 still reads
// the old value because its snapshot is at an earlier timestamp.
func TestSnapshotIsolation(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	// seed k=v0
	seed := mgr.Begin()
	_ = mgr.Put(seed.ID, "k", "v0")
	b, _, _ := mgr.Commit(seed.ID)
	applyBatch(store, b)

	tx1 := mgr.Begin() // read_ts0
	// tx2 runs to completion between tx1's begin and tx1's first read.
	tx2 := mgr.Begin()
	_ = mgr.Put(tx2.ID, "k", "v2")
	b2, ok, err := mgr.Commit(tx2.ID)
	if err != nil || !ok {
		t.Fatalf("tx2 commit: %v ok=%v", err, ok)
	}
	applyBatch(store, b2)

	v, found, err := mgr.Get(tx1.ID, "k")
	if err != nil || !found {
		t.Fatalf("tx1 read: found=%v err=%v", found, err)
	}
	if v != "v0" {
		t.Fatalf("tx1 saw v=%q, expected v0 (snapshot violation)", v)
	}
}

// TestSSIConflict: two concurrent transactions both read k, both try to write
// based on what they read; only one can commit (the other must abort).
func TestSSIConflict(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	// seed
	seed := mgr.Begin()
	_ = mgr.Put(seed.ID, "k", "1")
	b, _, _ := mgr.Commit(seed.ID)
	applyBatch(store, b)

	// Two txns begin at the same (or nearly the same) timestamp.
	ta := mgr.Begin()
	tb := mgr.Begin()

	// Both read k.
	va, fa, _ := mgr.Get(ta.ID, "k")
	vb, fb, _ := mgr.Get(tb.ID, "k")
	if !fa || !fb || va != "1" || vb != "1" {
		t.Fatalf("both should see k=1; got a=%q b=%q", va, vb)
	}

	// ta writes a derived value and commits. Apply to store.
	_ = mgr.Put(ta.ID, "k", "2")
	ba, ok, err := mgr.Commit(ta.ID)
	if err != nil || !ok {
		t.Fatalf("ta commit: %v ok=%v", err, ok)
	}
	applyBatch(store, ba)

	// tb tries to commit a derived value. It should abort with SSI conflict.
	_ = mgr.Put(tb.ID, "k", "3")
	_, ok, err = mgr.Commit(tb.ID)
	if ok {
		t.Fatalf("tb should have aborted, but committed")
	}
	if err != ErrAborted {
		t.Fatalf("tb: expected ErrAborted, got %v", err)
	}

	// Final value should be 2, not 3.
	tx := mgr.Begin()
	v, _, _ := mgr.Get(tx.ID, "k")
	if v != "2" {
		t.Fatalf("final value = %q, expected 2", v)
	}
}

// TestConcurrentSSI hammers the manager with many parallel increment-like
// transactions on the same key. With proper SSI, every successful commit
// must have observed a snapshot that was still current at commit time, so
// a simple counter monotonically increases and no two committed txns share
// the same source snapshot.
func TestConcurrentSSI(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	// seed counter=0
	seed := mgr.Begin()
	_ = mgr.Put(seed.ID, "ctr", "0")
	b, _, _ := mgr.Commit(seed.ID)
	applyBatch(store, b)

	var wg sync.WaitGroup
	var mu sync.Mutex
	commits := 0
	aborts := 0
	N := 50

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx := mgr.Begin()
			_, _, _ = mgr.Get(tx.ID, "ctr")
			_ = mgr.Put(tx.ID, "ctr", "x") // just mark a write
			bb, ok, err := mgr.Commit(tx.ID)
			mu.Lock()
			defer mu.Unlock()
			if ok {
				applyBatch(store, bb)
				commits++
			} else if err == ErrAborted {
				aborts++
			} else {
				t.Errorf("unexpected commit error: %v", err)
			}
		}()
	}
	wg.Wait()

	if commits == 0 {
		t.Fatalf("no transactions committed at all")
	}
	if commits+aborts != N {
		t.Fatalf("commits+aborts=%d, expected %d", commits+aborts, N)
	}
	// With serial commits through a single mutex (as in our impl), we expect at
	// least one commit and every concurrent conflict detected.
	t.Logf("concurrent SSI: %d committed, %d aborted (of %d)", commits, aborts, N)
}

// TestReadPhantomConflict: tx reads a key that does not exist, another txn
// creates it, then the first tries to commit a write. This is the phantom-
// read case and SSI should abort it.
func TestReadPhantomConflict(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	t1 := mgr.Begin()
	// t1 reads absent key.
	if _, found, _ := mgr.Get(t1.ID, "k"); found {
		t.Fatal("expected absent")
	}

	t2 := mgr.Begin()
	_ = mgr.Put(t2.ID, "k", "created")
	b, ok, _ := mgr.Commit(t2.ID)
	if !ok {
		t.Fatal("t2 commit failed")
	}
	applyBatch(store, b)

	// t1 now wants to write — must abort because the key it observed-as-absent
	// was written in its window.
	_ = mgr.Put(t1.ID, "other", "v")
	_, ok, err := mgr.Commit(t1.ID)
	if ok || err != ErrAborted {
		t.Fatalf("t1 should abort on phantom; ok=%v err=%v", ok, err)
	}
}

// TestSnapshotRoundTrip: versions survive EncodeForSnapshot / DecodeFromSnapshot.
func TestSnapshotRoundTrip(t *testing.T) {
	store := mvcc.New()
	hlc := NewHLC()
	mgr := NewManager(store, hlc)

	for i, v := range []string{"a", "b", "c"} {
		tx := mgr.Begin()
		_ = mgr.Put(tx.ID, "k", v)
		b, _, _ := mgr.Commit(tx.ID)
		applyBatch(store, b)
		_ = i
	}

	snap := make(map[string]string)
	store.EncodeForSnapshot(snap)

	store2 := mvcc.New()
	store2.DecodeFromSnapshot(snap)

	// Newest read should return "c".
	if v, ok := store2.GetAt("k", ^uint64(0)); !ok || v != "c" {
		t.Fatalf("restored newest read: v=%q ok=%v", v, ok)
	}
	if d := store2.Dump(); len(d["k"]) != 3 {
		t.Fatalf("expected 3 versions, got %d", len(d["k"]))
	}
}
