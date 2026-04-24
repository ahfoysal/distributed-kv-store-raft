package shard

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestManifestFind(t *testing.T) {
	dir := t.TempDir()
	m, err := LoadManifest(filepath.Join(dir, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Find("anything"); got.ID != "s0" {
		t.Fatalf("expected s0, got %s", got.ID)
	}
	if _, _, err := m.ReplaceWithSplit("s0", "s1", "m"); err != nil {
		t.Fatal(err)
	}
	if got := m.Find("a"); got.ID != "s0" {
		t.Fatalf("a should stay on s0, got %s", got.ID)
	}
	if got := m.Find("z"); got.ID != "s1" {
		t.Fatalf("z should move to s1, got %s", got.ID)
	}
	if got := m.Find("m"); got.ID != "s1" {
		t.Fatalf("m (boundary) should be on s1, got %s", got.ID)
	}
}

func TestManifestPersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	m, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ReplaceWithSplit("s0", "s1", "m"); err != nil {
		t.Fatal(err)
	}
	m2, err := LoadManifest(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m2.Shards) != 2 {
		t.Fatalf("expected 2 shards after reload, got %d", len(m2.Shards))
	}
}

func TestRouterPutGetSplit(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRouter(Config{
		DataRoot:       dir,
		ManifestPath:   filepath.Join(dir, "manifest.json"),
		SplitThreshold: 0, // no auto-split in the test
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		if err := r.Put(k, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if r.Shards()[0].KeyCount != 100 {
		t.Fatalf("expected 100 keys, got %d", r.Shards()[0].KeyCount)
	}
	if _, _, err := r.Split("s0", "key-050"); err != nil {
		t.Fatal(err)
	}
	shards := r.Shards()
	if len(shards) != 2 {
		t.Fatalf("expected 2 shards, got %d", len(shards))
	}
	if shards[0].KeyCount != 50 {
		t.Fatalf("left shard should have 50 keys, got %d", shards[0].KeyCount)
	}
	if shards[1].KeyCount != 50 {
		t.Fatalf("right shard should have 50 keys, got %d", shards[1].KeyCount)
	}
	// Values still readable through the router.
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%03d", i)
		v, ok := r.Get(k)
		if !ok || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("missing or wrong value for %s: %q/%v", k, v, ok)
		}
	}
}

func TestRebalancerAutoSplit(t *testing.T) {
	dir := t.TempDir()
	r, err := NewRouter(Config{
		DataRoot:       dir,
		ManifestPath:   filepath.Join(dir, "manifest.json"),
		SplitThreshold: 50,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for i := 0; i < 200; i++ {
		if err := r.Put(fmt.Sprintf("k-%04d", i), "x"); err != nil {
			t.Fatal(err)
		}
	}
	// Run rebalance passes until no shard is over threshold.
	for pass := 0; pass < 10; pass++ {
		ev := r.Rebalance()
		if len(ev) == 0 {
			break
		}
	}
	for _, s := range r.Shards() {
		if s.KeyCount > 50 {
			t.Fatalf("shard %s still has %d keys, expected <= 50", s.ID, s.KeyCount)
		}
	}
	if len(r.Shards()) < 4 {
		t.Fatalf("expected at least 4 shards after rebalancing, got %d", len(r.Shards()))
	}
}
