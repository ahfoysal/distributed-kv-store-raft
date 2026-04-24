// Package backup provides a simple snapshot-to-tarball and
// tarball-to-fresh-cluster restore. A backup captures every file under the
// sqlnode's data root (per-shard LSM files, the shard manifest, every
// participant's prepare log, the coordinator's decision log, and the
// schema catalog) — in short, everything needed to stand up a bit-for-bit
// identical cluster.
//
// Backups are NOT meant to be consistent with in-flight transactions; the
// caller is expected to flush first (sqlnode /backup endpoint does this
// for every shard before archiving).
package backup

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Create walks dataRoot and writes a gzip-compressed tarball to outPath.
// Paths inside the archive are stored relative to dataRoot so that Restore
// can recreate them under any target directory.
func Create(dataRoot, outPath string) error {
	absRoot, err := filepath.Abs(dataRoot)
	if err != nil {
		return err
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz := gzip.NewWriter(f)
	defer gz.Close()
	tw := tar.NewWriter(gz)
	defer tw.Close()

	return filepath.Walk(absRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(absRoot, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		// Skip any tmp artifacts — they're about to be renamed or
		// discarded, and the backup snapshot should not depend on them.
		if strings.HasSuffix(rel, ".tmp") {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = rel
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			in, err := os.Open(path)
			if err != nil {
				return err
			}
			if _, err := io.Copy(tw, in); err != nil {
				in.Close()
				return err
			}
			in.Close()
		}
		return nil
	})
}

// Restore extracts the tarball at tarPath into dataRoot. If dataRoot
// already exists, Restore aborts — the caller must pass a fresh directory
// so we never clobber live data.
func Restore(tarPath, dataRoot string) error {
	if _, err := os.Stat(dataRoot); err == nil {
		return fmt.Errorf("restore target %q already exists; refusing to overwrite", dataRoot)
	}
	if err := os.MkdirAll(dataRoot, 0o755); err != nil {
		return err
	}
	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		// Guard against archive paths trying to escape dataRoot.
		rel := filepath.Clean(hdr.Name)
		if strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
			return fmt.Errorf("archive contains unsafe path: %q", hdr.Name)
		}
		dst := filepath.Join(dataRoot, rel)
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(dst, os.FileMode(hdr.Mode)); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
				return err
			}
			out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.FileMode(hdr.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(out, tr); err != nil {
				out.Close()
				return err
			}
			out.Close()
		default:
			// Skip symlinks, fifos, etc — we don't create them.
		}
	}
	return nil
}
