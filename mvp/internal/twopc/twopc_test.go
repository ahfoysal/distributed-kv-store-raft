package twopc

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/foysal/distkv/internal/shard"
)

// helper: build a router with two shards s0/[-inf,m) and s1/[m,+inf).
func twoShardRouter(t *testing.T, dataRoot string) *shard.Router {
	t.Helper()
	manifest := filepath.Join(dataRoot, "manifest.json")
	r, err := shard.NewRouter(shard.Config{
		DataRoot:     dataRoot,
		ManifestPath: manifest,
		// SplitThreshold=0 disables the rebalancer — we set it up manually.
		RebalanceInterval: 0,
	})
	if err != nil {
		t.Fatalf("open router: %v", err)
	}
	if _, _, err := r.Split("s0", "m"); err != nil {
		t.Fatalf("split: %v", err)
	}
	return r
}

func TestTwoPCHappyPath(t *testing.T) {
	dir := t.TempDir()
	r := twoShardRouter(t, dir)
	defer r.Close()

	// seed both shards with starting balances.
	if err := r.Put("alice", "100"); err != nil { // shard s0
		t.Fatal(err)
	}
	if err := r.Put("zoe", "0"); err != nil { // shard s1 (z >= "m")
		t.Fatal(err)
	}
	// sanity: keys really did land on different shards.
	if r.Manifest().Find("alice").ID == r.Manifest().Find("zoe").ID {
		t.Fatalf("alice and zoe should be on different shards: %v / %v",
			r.Manifest().Find("alice").ID, r.Manifest().Find("zoe").ID)
	}

	c, err := NewCoordinator(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	ops := []Op{{Kind: "put", Key: "alice", Value: "40"}, {Kind: "put", Key: "zoe", Value: "60"}}
	_, out, err := c.Commit(ops)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if out != "commit" {
		t.Fatalf("outcome = %q, want commit", out)
	}
	if v, _ := r.Get("alice"); v != "40" {
		t.Fatalf("alice after commit = %q, want 40", v)
	}
	if v, _ := r.Get("zoe"); v != "60" {
		t.Fatalf("zoe after commit = %q, want 60", v)
	}
}

func TestTwoPCRecoveryAppliesAfterCrash(t *testing.T) {
	dir := t.TempDir()

	openRouter := func() *shard.Router {
		r, err := shard.NewRouter(shard.Config{
			DataRoot:     dir,
			ManifestPath: filepath.Join(dir, "manifest.json"),
		})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	// first open + split.
	r := openRouter()
	if _, _, err := r.Split("s0", "m"); err != nil {
		t.Fatalf("split: %v", err)
	}
	if err := r.Put("alice", "100"); err != nil {
		t.Fatal(err)
	}
	if err := r.Put("zoe", "0"); err != nil {
		t.Fatal(err)
	}

	c, err := NewCoordinator(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	txn := c.NewTxnID()
	groups, err := c.groupByShard([]Op{
		{Kind: "put", Key: "alice", Value: "40"},
		{Kind: "put", Key: "zoe", Value: "60"},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, g := range groups {
		pl, err := c.participantLog(g.ShardID)
		if err != nil {
			t.Fatal(err)
		}
		if err := pl.Prepare(txn, uint64(time.Now().UnixNano()), g.Ops); err != nil {
			t.Fatal(err)
		}
	}
	shardIDs := []string{}
	for _, g := range groups {
		shardIDs = append(shardIDs, g.ShardID)
	}
	if err := c.writeDecision(decisionRecord{
		TxnID: txn, Outcome: "commit", Shards: shardIDs,
	}); err != nil {
		t.Fatal(err)
	}
	// CRASH simulation: close router without applying.
	_ = r.Close()

	// cold start: recovery must apply the committed txn.
	r2 := openRouter()
	defer r2.Close()
	c2, err := NewCoordinator(dir, r2)
	if err != nil {
		t.Fatal(err)
	}
	committed, aborted, err := c2.Recover()
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if committed != 2 || aborted != 0 {
		t.Fatalf("recover: committed=%d aborted=%d, want 2/0", committed, aborted)
	}
	if v, _ := r2.Get("alice"); v != "40" {
		t.Fatalf("alice after recovery = %q, want 40", v)
	}
	if v, _ := r2.Get("zoe"); v != "60" {
		t.Fatalf("zoe after recovery = %q, want 60", v)
	}
}

func TestTwoPCRecoveryAbortsWithoutDecision(t *testing.T) {
	dir := t.TempDir()
	openRouter := func() *shard.Router {
		r, err := shard.NewRouter(shard.Config{
			DataRoot:     dir,
			ManifestPath: filepath.Join(dir, "manifest.json"),
		})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := openRouter()
	if _, _, err := r.Split("s0", "m"); err != nil {
		t.Fatal(err)
	}
	if err := r.Put("alice", "100"); err != nil {
		t.Fatal(err)
	}
	if err := r.Put("zoe", "0"); err != nil {
		t.Fatal(err)
	}

	c, err := NewCoordinator(dir, r)
	if err != nil {
		t.Fatal(err)
	}
	// Prepare only — NO decision written.
	txn := c.NewTxnID()
	groups, _ := c.groupByShard([]Op{
		{Kind: "put", Key: "alice", Value: "99"},
		{Kind: "put", Key: "zoe", Value: "99"},
	})
	for _, g := range groups {
		pl, _ := c.participantLog(g.ShardID)
		if err := pl.Prepare(txn, uint64(time.Now().UnixNano()), g.Ops); err != nil {
			t.Fatal(err)
		}
	}
	_ = r.Close()

	// Recovery must abort (no decision => abort).
	r2 := openRouter()
	defer r2.Close()
	c2, _ := NewCoordinator(dir, r2)
	committed, aborted, err := c2.Recover()
	if err != nil {
		t.Fatal(err)
	}
	if committed != 0 || aborted == 0 {
		t.Fatalf("recover: committed=%d aborted=%d, want 0/>=1", committed, aborted)
	}
	if v, _ := r2.Get("alice"); v != "100" {
		t.Fatalf("alice = %q, want untouched 100", v)
	}
	if v, _ := r2.Get("zoe"); v != "0" {
		t.Fatalf("zoe = %q, want untouched 0", v)
	}
}
