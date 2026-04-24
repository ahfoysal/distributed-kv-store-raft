// Package mvcc is a small multi-version concurrency control store used by the
// M4 transaction layer. Each key maps to a version list ordered newest-first:
//
//	user_key -> [(ts_commit, value, tombstone), ...]
//
// Reads at a snapshot timestamp `ts` return the newest version whose commit
// timestamp is <= ts. Writes append a new version at the transaction's commit
// timestamp.
//
// Durability: the canonical record of every committed version is the sequence
// of mvcc_commit entries in the Raft log (replicated to followers and
// re-applied on restart from WAL + snapshot). On top of that, the in-memory
// map is what every live Get consults.
//
// Encoding for on-disk LSM persistence (used when the Raft snapshot is taken)
// follows the spec in the M4 plan: encoded key = user_key | 0x00 | reverseTS,
// where reverseTS = ^uint64(ts) so that a forward byte-order scan yields
// newest-first versions. We serialize the full MVCC state into the Raft
// snapshot by piggy-backing on the existing map[string]string surface, with
// each version stored under a synthetic key. See EncodeForSnapshot /
// DecodeFromSnapshot below.
package mvcc

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// Version is a single (commit_ts, value|tombstone) record.
type Version struct {
	CommitTS  uint64 `json:"ts"`
	Value     string `json:"v,omitempty"`
	Tombstone bool   `json:"d,omitempty"`
}

// Store is the MVCC key -> version-list map.
type Store struct {
	mu       sync.RWMutex
	versions map[string][]Version // newest first (descending commit_ts)

	// writeTimes records, for every key, the list of commit timestamps at which
	// the key was written. Used by the SSI validator to check whether a
	// would-be-committing transaction observed a stale snapshot.
	writeTimes map[string][]uint64
}

func New() *Store {
	return &Store{
		versions:   make(map[string][]Version),
		writeTimes: make(map[string][]uint64),
	}
}

// GetAt returns the value of key visible at snapshot timestamp ts. The second
// return value is false if the key does not exist at that snapshot (either
// never written, or the newest version <= ts is a tombstone).
func (s *Store) GetAt(key string, ts uint64) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs := s.versions[key]
	for _, v := range vs { // newest first
		if v.CommitTS <= ts {
			if v.Tombstone {
				return "", false
			}
			return v.Value, true
		}
	}
	return "", false
}

// LastCommitTS returns the most recent commit timestamp for key, or 0 if
// none. Used by SSI validation.
func (s *Store) LastCommitTS(key string) uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vs := s.versions[key]
	if len(vs) == 0 {
		return 0
	}
	return vs[0].CommitTS
}

// HasWriteInRange reports whether any committed write to `key` has a commit
// timestamp strictly greater than `afterTS` and strictly less-or-equal to
// `uptoTS`. This is the read-write conflict check for SSI validation.
func (s *Store) HasWriteInRange(key string, afterTS, uptoTS uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, ts := range s.writeTimes[key] {
		if ts > afterTS && ts <= uptoTS {
			return true
		}
	}
	return false
}

// CommitBatch represents one transaction's write set being applied atomically
// at commit timestamp `ts`. A Tombstone write deletes the key.
type WriteOp struct {
	Key       string `json:"k"`
	Value     string `json:"v,omitempty"`
	Tombstone bool   `json:"d,omitempty"`
}

type CommitBatch struct {
	CommitTS uint64    `json:"ts"`
	Writes   []WriteOp `json:"w"`
}

// ApplyCommit installs every write in b at commit timestamp b.CommitTS. It is
// idempotent: replaying the same batch twice is a no-op on the second apply
// (we check for an existing version with the same ts).
func (s *Store) ApplyCommit(b CommitBatch) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range b.Writes {
		// Idempotency check.
		vs := s.versions[w.Key]
		dup := false
		for _, v := range vs {
			if v.CommitTS == b.CommitTS {
				dup = true
				break
			}
		}
		if dup {
			continue
		}
		nv := Version{CommitTS: b.CommitTS, Value: w.Value, Tombstone: w.Tombstone}
		// Insert so that versions stay sorted newest-first.
		inserted := false
		for i := range vs {
			if nv.CommitTS > vs[i].CommitTS {
				vs = append(vs[:i], append([]Version{nv}, vs[i:]...)...)
				inserted = true
				break
			}
		}
		if !inserted {
			vs = append(vs, nv)
		}
		s.versions[w.Key] = vs

		// Record commit time for SSI validation.
		s.writeTimes[w.Key] = append(s.writeTimes[w.Key], b.CommitTS)
	}
}

// ----------------------------------------------------------------------------
// Snapshot encoding: flatten the version map into flat key/value pairs for
// inclusion in the Raft snapshot (which uses map[string]string as its wire
// format). We use a reserved prefix to avoid colliding with regular KV keys.

const (
	// snapshotPrefix is prepended to every key written into the Raft snapshot
	// map. The encoded key is: snapshotPrefix | user_key | 0x00 | reverseTS.
	snapshotPrefix = "\x01mvcc\x00"
)

// EncodeForSnapshot appends every (key, version) pair into `out` using the
// reverse-timestamp key encoding. Intended to be called while the caller
// already holds any needed locks.
func (s *Store) EncodeForSnapshot(out map[string]string) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for k, vs := range s.versions {
		for _, v := range vs {
			enc := encodeVersionKey(k, v.CommitTS)
			payload, _ := json.Marshal(v)
			out[enc] = string(payload)
		}
	}
}

// DecodeFromSnapshot is the inverse: called when a Raft snapshot is being
// restored, it pulls every mvcc-prefixed entry out of `data` (and removes it
// from the map) and rebuilds the in-memory version lists.
func (s *Store) DecodeFromSnapshot(data map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.versions = make(map[string][]Version)
	s.writeTimes = make(map[string][]uint64)

	for k, raw := range data {
		if !strings.HasPrefix(k, snapshotPrefix) {
			continue
		}
		uk, _, ok := decodeVersionKey(k)
		if !ok {
			continue
		}
		var v Version
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			continue
		}
		s.versions[uk] = append(s.versions[uk], v)
		s.writeTimes[uk] = append(s.writeTimes[uk], v.CommitTS)
		delete(data, k)
	}
	// Ensure newest-first order.
	for k, vs := range s.versions {
		// simple insertion sort; lists are tiny in this project.
		for i := 1; i < len(vs); i++ {
			for j := i; j > 0 && vs[j].CommitTS > vs[j-1].CommitTS; j-- {
				vs[j], vs[j-1] = vs[j-1], vs[j]
			}
		}
		s.versions[k] = vs
	}
}

// IsSnapshotKey reports whether key was produced by EncodeForSnapshot.
// Exposed so the caller can route snapshot data to the right store.
func IsSnapshotKey(key string) bool {
	return strings.HasPrefix(key, snapshotPrefix)
}

func encodeVersionKey(userKey string, ts uint64) string {
	// reverseTS so a forward byte-order scan yields newest-first.
	rev := ^ts
	buf := make([]byte, 0, len(snapshotPrefix)+len(userKey)+1+8)
	buf = append(buf, snapshotPrefix...)
	buf = append(buf, userKey...)
	buf = append(buf, 0x00)
	var tsBytes [8]byte
	binary.BigEndian.PutUint64(tsBytes[:], rev)
	buf = append(buf, tsBytes[:]...)
	return string(buf)
}

func decodeVersionKey(enc string) (userKey string, ts uint64, ok bool) {
	if !strings.HasPrefix(enc, snapshotPrefix) {
		return "", 0, false
	}
	rest := enc[len(snapshotPrefix):]
	// Find the 0x00 separator followed by exactly 8 bytes.
	if len(rest) < 9 {
		return "", 0, false
	}
	sep := strings.LastIndexByte(rest[:len(rest)-8], 0x00)
	if sep < 0 {
		return "", 0, false
	}
	userKey = rest[:sep]
	tsBytes := rest[sep+1:]
	if len(tsBytes) != 8 {
		return "", 0, false
	}
	rev := binary.BigEndian.Uint64([]byte(tsBytes))
	ts = ^rev
	return userKey, ts, true
}

// Dump returns a debug-friendly copy of the version map. Used in tests.
func (s *Store) Dump() map[string][]Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string][]Version, len(s.versions))
	for k, vs := range s.versions {
		cp := make([]Version, len(vs))
		copy(cp, vs)
		out[k] = cp
	}
	return out
}

// Keys returns the sorted list of user keys with at least one version.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.versions))
	for k := range s.versions {
		out = append(out, k)
	}
	return out
}

// String is a debug helper.
func (s *Store) String() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var b strings.Builder
	for k, vs := range s.versions {
		fmt.Fprintf(&b, "%s: ", k)
		for _, v := range vs {
			if v.Tombstone {
				fmt.Fprintf(&b, "(ts=%d DEL) ", v.CommitTS)
			} else {
				fmt.Fprintf(&b, "(ts=%d %q) ", v.CommitTS, v.Value)
			}
		}
		b.WriteByte('\n')
	}
	return b.String()
}
