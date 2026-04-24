package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestBackupRestoreRoundTrip(t *testing.T) {
	src := t.TempDir()
	// Seed the source tree with a couple of files in nested dirs.
	files := map[string]string{
		"manifest.json":     `{"shards":[{"id":"s0","range":{"start":"","end":""}}]}`,
		"s0/prepare.log":    "{}\n",
		"s0/lsm/L0-1.sst":   "binary-blob-1",
		"s1/lsm/L0-2.sst":   "binary-blob-2",
		"catalog.json":      `{"tables":{}}`,
		"decision.log":      "",
		"ignored.tmp":       "should-be-filtered", // Create skips .tmp
	}
	for rel, body := range files {
		full := filepath.Join(src, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// Create backup.
	tarball := filepath.Join(t.TempDir(), "snap.tar.gz")
	if err := Create(src, tarball); err != nil {
		t.Fatalf("Create: %v", err)
	}
	info, err := os.Stat(tarball)
	if err != nil || info.Size() == 0 {
		t.Fatalf("backup empty or missing: %v", err)
	}

	// Restore into a fresh dir.
	dst := filepath.Join(t.TempDir(), "restored")
	if err := Restore(tarball, dst); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	// Every non-tmp file should exist with identical content.
	for rel, body := range files {
		if rel == "ignored.tmp" {
			if _, err := os.Stat(filepath.Join(dst, rel)); !os.IsNotExist(err) {
				t.Fatalf(".tmp file should not have been backed up: %v", err)
			}
			continue
		}
		got, err := os.ReadFile(filepath.Join(dst, rel))
		if err != nil {
			t.Fatalf("missing after restore: %s: %v", rel, err)
		}
		if string(got) != body {
			t.Fatalf("content mismatch for %s: got %q want %q", rel, got, body)
		}
	}
}

func TestRestoreRefusesExistingDir(t *testing.T) {
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	tarball := filepath.Join(t.TempDir(), "snap.tar.gz")
	if err := Create(src, tarball); err != nil {
		t.Fatal(err)
	}
	existing := t.TempDir() // TempDir already exists.
	if err := Restore(tarball, existing); err == nil {
		t.Fatalf("Restore must reject existing directory")
	}
}
