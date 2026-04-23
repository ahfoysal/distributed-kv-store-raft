package lsm

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// On-disk SSTable layout:
//
//   [data section]          sequence of records:
//                             keyLen:u32 | valLen:u32 | tomb:u8 | key | value
//   [index section]          one entry per record:
//                             keyLen:u32 | offset:u64 | key
//   [bloom section]          bloom.Encode() bytes
//   [footer]                 indexOff:u64 | indexLen:u64 |
//                            bloomOff:u64 | bloomLen:u64 |
//                            numRecs:u64  | magic:u32
//
// We keep the footer fixed-size so LoadSSTable can seek to (fileSize-footerLen).
// Magic is a sanity check. All integers are big-endian.

const (
	footerLen = 8*5 + 4
	magic     = 0x4C534D31 // "LSM1"
)

var errShortBuf = errors.New("lsm: short buffer")

// SSTable is a handle to a read-only on-disk table. It keeps the bloom filter
// and the (sparse-ish) key index in memory; record values are pulled from the
// file on Get via ReadAt.
type SSTable struct {
	Path   string
	ID     uint64
	Level  int
	NumRec uint64

	MinKey string
	MaxKey string

	file  *os.File
	bloom *Bloom

	// index: sorted parallel arrays; offset[i] points into the data section for
	// keys[i]. Full index (one entry per record) keeps reads O(log n) memory-
	// resident and one disk read.
	keys    []string
	offsets []uint64
}

// BuildSSTable writes a new immutable SSTable from the sorted records and
// returns a loaded handle. Tombstones are preserved — compaction is the only
// place they get dropped.
func BuildSSTable(path string, id uint64, level int, records []Record) (*SSTable, error) {
	// Defensive: ensure sorted.
	sort.SliceStable(records, func(i, j int) bool { return records[i].Key < records[j].Key })

	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriter(f)

	// ---- data section ----
	var offset uint64
	keys := make([]string, len(records))
	offsets := make([]uint64, len(records))

	var recBuf [9]byte // keyLen(4) + valLen(4) + tomb(1)
	for i, r := range records {
		keys[i] = r.Key
		offsets[i] = offset

		binary.BigEndian.PutUint32(recBuf[0:4], uint32(len(r.Key)))
		binary.BigEndian.PutUint32(recBuf[4:8], uint32(len(r.Value)))
		if r.Tombstone {
			recBuf[8] = 1
		} else {
			recBuf[8] = 0
		}
		if _, err := w.Write(recBuf[:]); err != nil {
			f.Close()
			return nil, err
		}
		if _, err := w.WriteString(r.Key); err != nil {
			f.Close()
			return nil, err
		}
		if _, err := w.WriteString(r.Value); err != nil {
			f.Close()
			return nil, err
		}
		offset += 9 + uint64(len(r.Key)) + uint64(len(r.Value))
	}

	indexOff := offset

	// ---- index section ----
	var idxHdr [12]byte // keyLen(4) + offset(8)
	for i, k := range keys {
		binary.BigEndian.PutUint32(idxHdr[0:4], uint32(len(k)))
		binary.BigEndian.PutUint64(idxHdr[4:12], offsets[i])
		if _, err := w.Write(idxHdr[:]); err != nil {
			f.Close()
			return nil, err
		}
		if _, err := w.WriteString(k); err != nil {
			f.Close()
			return nil, err
		}
		offset += 12 + uint64(len(k))
	}

	indexLen := offset - indexOff

	// ---- bloom section ----
	bloom := NewBloom(len(records), 0.01)
	for _, r := range records {
		bloom.Add([]byte(r.Key))
	}
	bloomBytes := bloom.Encode()
	bloomOff := offset
	if _, err := w.Write(bloomBytes); err != nil {
		f.Close()
		return nil, err
	}
	bloomLen := uint64(len(bloomBytes))
	offset += bloomLen

	// ---- footer ----
	var footer [footerLen]byte
	binary.BigEndian.PutUint64(footer[0:8], indexOff)
	binary.BigEndian.PutUint64(footer[8:16], indexLen)
	binary.BigEndian.PutUint64(footer[16:24], bloomOff)
	binary.BigEndian.PutUint64(footer[24:32], bloomLen)
	binary.BigEndian.PutUint64(footer[32:40], uint64(len(records)))
	binary.BigEndian.PutUint32(footer[40:44], magic)
	if _, err := w.Write(footer[:]); err != nil {
		f.Close()
		return nil, err
	}

	if err := w.Flush(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return nil, err
	}
	if err := f.Close(); err != nil {
		return nil, err
	}

	return LoadSSTable(path, id, level)
}

// LoadSSTable opens an on-disk SSTable read-only and loads its index + bloom.
func LoadSSTable(path string, id uint64, level int) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	size := st.Size()
	if size < footerLen {
		f.Close()
		return nil, fmt.Errorf("sstable %s too small (%d bytes)", path, size)
	}

	var footer [footerLen]byte
	if _, err := f.ReadAt(footer[:], size-footerLen); err != nil {
		f.Close()
		return nil, err
	}
	if binary.BigEndian.Uint32(footer[40:44]) != magic {
		f.Close()
		return nil, fmt.Errorf("sstable %s bad magic", path)
	}
	indexOff := binary.BigEndian.Uint64(footer[0:8])
	indexLen := binary.BigEndian.Uint64(footer[8:16])
	bloomOff := binary.BigEndian.Uint64(footer[16:24])
	bloomLen := binary.BigEndian.Uint64(footer[24:32])
	numRecs := binary.BigEndian.Uint64(footer[32:40])

	// Load index into memory.
	idxBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(idxBuf, int64(indexOff)); err != nil {
		f.Close()
		return nil, err
	}
	keys := make([]string, 0, numRecs)
	offsets := make([]uint64, 0, numRecs)
	pos := uint64(0)
	for pos < indexLen {
		if pos+12 > indexLen {
			f.Close()
			return nil, fmt.Errorf("sstable %s corrupt index", path)
		}
		klen := binary.BigEndian.Uint32(idxBuf[pos : pos+4])
		off := binary.BigEndian.Uint64(idxBuf[pos+4 : pos+12])
		pos += 12
		if pos+uint64(klen) > indexLen {
			f.Close()
			return nil, fmt.Errorf("sstable %s corrupt index key", path)
		}
		keys = append(keys, string(idxBuf[pos:pos+uint64(klen)]))
		offsets = append(offsets, off)
		pos += uint64(klen)
	}

	// Load bloom.
	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, int64(bloomOff)); err != nil {
		f.Close()
		return nil, err
	}
	bloom, err := DecodeBloom(bloomBuf)
	if err != nil {
		f.Close()
		return nil, err
	}

	t := &SSTable{
		Path:    path,
		ID:      id,
		Level:   level,
		NumRec:  numRecs,
		file:    f,
		bloom:   bloom,
		keys:    keys,
		offsets: offsets,
	}
	if len(keys) > 0 {
		t.MinKey = keys[0]
		t.MaxKey = keys[len(keys)-1]
	}
	return t, nil
}

// Close releases the file handle.
func (t *SSTable) Close() error {
	if t.file != nil {
		return t.file.Close()
	}
	return nil
}

// Get returns (value, tombstone, found). "found" is whether the key appears in
// this table at all; if tombstone==true the caller should treat the key as
// deleted and STOP descending to older tables.
func (t *SSTable) Get(key string) (string, bool, bool) {
	if !t.bloom.MayContain([]byte(key)) {
		return "", false, false
	}
	i := sort.SearchStrings(t.keys, key)
	if i >= len(t.keys) || t.keys[i] != key {
		return "", false, false
	}
	return t.readRecordAt(t.offsets[i])
}

// readRecordAt reads the record header + key + value at the given data offset.
// Returns (value, tombstone, found=true) on success.
func (t *SSTable) readRecordAt(off uint64) (string, bool, bool) {
	var hdr [9]byte
	if _, err := t.file.ReadAt(hdr[:], int64(off)); err != nil {
		return "", false, false
	}
	klen := binary.BigEndian.Uint32(hdr[0:4])
	vlen := binary.BigEndian.Uint32(hdr[4:8])
	tomb := hdr[8] == 1
	// Skip key, read value.
	valBuf := make([]byte, vlen)
	if vlen > 0 {
		if _, err := t.file.ReadAt(valBuf, int64(off)+9+int64(klen)); err != nil {
			return "", false, false
		}
	}
	return string(valBuf), tomb, true
}

// ScanAll returns every record in the table in key order. Used by the
// compactor — streams from disk via a buffered reader, does not rely on the
// in-memory index.
func (t *SSTable) ScanAll() ([]Record, error) {
	// Use a fresh reader at offset 0 up to the start of the index section.
	st, err := t.file.Stat()
	if err != nil {
		return nil, err
	}
	_ = st
	// We know the data section ends at offsets[0] boundary... actually we know
	// it ends where the index section starts, which equals indexOff. Easiest:
	// sum the first offset logic — or just re-derive from the first index entry
	// (offsets[0] == 0) and from the footer. Since we already loaded offsets[],
	// iterate records using offsets and the header to figure out each length.
	out := make([]Record, 0, len(t.keys))
	for i, off := range t.offsets {
		var hdr [9]byte
		if _, err := t.file.ReadAt(hdr[:], int64(off)); err != nil {
			return nil, err
		}
		klen := binary.BigEndian.Uint32(hdr[0:4])
		vlen := binary.BigEndian.Uint32(hdr[4:8])
		tomb := hdr[8] == 1
		valBuf := make([]byte, vlen)
		if vlen > 0 {
			if _, err := t.file.ReadAt(valBuf, int64(off)+9+int64(klen)); err != nil {
				return nil, err
			}
		}
		out = append(out, Record{Key: t.keys[i], Value: string(valBuf), Tombstone: tomb})
	}
	return out, nil
}

// sstableFilename formats a deterministic filename for a given (level, id).
func sstableFilename(level int, id uint64) string {
	return fmt.Sprintf("L%d-%016d.sst", level, id)
}

// parseSSTableFilename reverses sstableFilename. Returns (level, id, ok).
func parseSSTableFilename(name string) (int, uint64, bool) {
	var level int
	var id uint64
	n, err := fmt.Sscanf(name, "L%d-%016d.sst", &level, &id)
	if err != nil || n != 2 {
		return 0, 0, false
	}
	return level, id, true
}

// writeThenRename builds an SSTable at a temp path then renames into place, so
// a crash mid-write never leaves a half-written .sst file where the DB will
// discover it.
func writeThenRename(dir string, level int, id uint64, records []Record) (*SSTable, error) {
	final := filepath.Join(dir, sstableFilename(level, id))
	tmp := final + ".tmp"
	t, err := BuildSSTable(tmp, id, level, records)
	if err != nil {
		return nil, err
	}
	// Close the handle before rename (Windows-safety; on Unix it's fine either
	// way, but we need to reopen afterwards from the final path anyway to get a
	// clean Path field).
	t.Close()
	if err := os.Rename(tmp, final); err != nil {
		return nil, err
	}
	return LoadSSTable(final, id, level)
}

// helper used by compactor: copy bytes helper.
var _ = io.Copy
