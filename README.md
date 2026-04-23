# Distributed KV Database

**Stack:** Go 1.22 · gRPC · Protobuf · BadgerDB (LSM) · etcd/raft library (study) then self-written · Prometheus · Docker Compose for cluster

## Full Vision
Multi-region Raft cluster, MVCC, SSI transactions, LSM storage, range sharding, rebalancing, SQL layer, online schema changes, backup/restore, Jepsen-tested.

## MVP (1 night)
3-node in-memory Raft, leader election, log replication, `get/set` over gRPC. Kill-a-node demo.

## Milestones
- **M1 (Week 1):** Raft election + replication + kill-node demo
- **M2 (Week 2):** Persistent WAL + snapshots + crash recovery
- **M3 (Week 4):** LSM storage engine (memtable, SSTables, compaction)
- **M4 (Week 8):** MVCC + SSI transactions
- **M5 (Week 12):** Range sharding + rebalancing + mini SQL parser

## Key References
- Raft paper (Ongaro & Ousterhout, 2014)
- CockroachDB design docs
- TiKV architecture

## M2 Status: Persistent WAL + Snapshots + Crash Recovery (shipped)

Per-node on-disk layout under `data/<node-id>/`:

| File            | Contents                                              | fsync trigger                     |
|-----------------|-------------------------------------------------------|-----------------------------------|
| `state.json`    | `currentTerm`, `votedFor`, `commitIndex`              | any change (vote, term, commit)   |
| `wal.log`       | JSON log entries, one per line                        | every `Propose` / `RewriteWAL`    |
| `snapshot.json` | `{last_included_index, last_included_term, data}`     | every N applied entries (`--snapshot-every`, default 100) |

### Design notes
- **WAL** is append-only JSON-per-line. On truncation (follower sees a conflicting suffix) the WAL is atomically rewritten via `tmp + rename`.
- **State** (`term`, `votedFor`, `commitIndex`) is written atomically (`tmp + rename`) on every change. Persisting `commitIndex` is beyond the Raft paper minimum but lets a node recover its applied KV state without waiting for a fresh majority to re-commit log-tail entries.
- **Snapshot** captures the full KV map plus the log index/term it covers, then truncates the WAL. On startup the snapshot is restored into the KV store first, the WAL is replayed on top, and `commitIndex` is rehydrated so `applyLoop` re-applies committed entries.
- **Synchronous applier.** The raft node invokes the KV applier inline (rather than via an async channel) so a subsequent snapshot is guaranteed to observe fully-applied state.
- **Known limitation:** no `InstallSnapshot` RPC yet; a follower that falls many snapshots behind cannot catch up until it receives a post-snapshot entry. Fine for the M2 test topology.

### Kill-and-restart test

`mvp/scripts/test_m2.sh` starts 3 nodes, writes 10 keys through the leader, `kill -9`s every process, restarts the cluster, and reads every key back:

```
=== phase 2: set 10 keys ===
=== phase 3: kill all nodes ===
=== phase 4: restart cluster ===
leader after restart on port 9003
=== phase 5: verify all 10 keys recovered ===
  OK  key1 -> {"found":true,"value":"value1"}
  OK  key2 -> {"found":true,"value":"value2"}
  ...
  OK  key10 -> {"found":true,"value":"value10"}

M2 CRASH-RECOVERY TEST: PASSED (10/10 keys recovered)
```

Snapshot path also verified by hand: with `--snapshot-every 50`, writing 60 keys produces `snapshot.json` at index 50 on all three nodes, a WAL truncated to entries 51–60, and a full kill+restart that correctly re-materializes every key (k1, k25, k50 from snapshot; k51–k60 from replayed WAL).

### Running it
```
cd mvp
bash scripts/test_m2.sh
# or manually:
go run ./cmd/node --id n1 --addr 127.0.0.1:9001 --peers n2@127.0.0.1:9002,n3@127.0.0.1:9003 --data data
```

## M3 Status: LSM storage engine (shipped)

Replaced the in-memory `map[string]string` KV with a small log-structured
merge-tree in `mvp/internal/lsm/`. The Raft state machine in
`mvp/internal/kv/store.go` now delegates every `Apply` / `Get` to the LSM; the
public `Snapshot` / `Restore` surface used by Raft is unchanged.

### Package layout (`mvp/internal/lsm/`)
| File            | Role                                                                  |
|-----------------|-----------------------------------------------------------------------|
| `memtable.go`   | In-memory `map[string]entry` with tombstones; sorts on flush          |
| `sstable.go`    | Immutable on-disk table: `data | index | bloom | footer`              |
| `bloom.go`      | Classic bit-array bloom filter, size computed from (n, fp=0.01)       |
| `compactor.go`  | Two-level (L0/L1) leveled compaction with k-way merge + tombstone GC  |
| `db.go`         | `Open` / `Put` / `Delete` / `Get` / `Flush` / `SnapshotAll` / `Restore` |

### SSTable on-disk layout
```
[ data   ]  keyLen:u32 | valLen:u32 | tomb:u8 | key | value   (repeated)
[ index  ]  keyLen:u32 | offset:u64 | key                     (one per record)
[ bloom  ]  k:u32 | m:u32 | bits...
[ footer ]  indexOff:u64 | indexLen:u64 |
            bloomOff:u64 | bloomLen:u64 |
            numRecs:u64  | magic:u32    (fixed 44 bytes)
```
Big-endian throughout. The footer is fixed-size so `LoadSSTable` can seek
`fileSize - 44` and recover the index/bloom offsets without scanning.

### Read path
`memtable → L0 (newest→oldest) → L1 (non-overlapping, binary-search by range)`.
A tombstone at any layer short-circuits and returns "not found" — older
tables are NOT consulted. Each SSTable `Get` consults the bloom filter first
(skips the binary-search + disk read when the bloom rejects).

### Write / flush / compaction
- `Put` / `Delete` append to the memtable. When it crosses
  `FlushThresholdBytes` (default 1 MiB) it is sorted and written as a new L0
  SSTable via temp-file + rename.
- When L0 has ≥ `L0Trigger` tables (default 4), the compactor takes every L0
  table plus any overlapping L1 tables, k-way-merges them (sources ordered
  newest→oldest so first occurrence wins), drops tombstones (safe because L1
  is the bottom level here), and writes one new L1 table.
- Old inputs are closed and `os.Remove`d after the new output is durable.

### Raft integration
- Each node's LSM lives at `data/<node-id>/lsm/`. `snapshot.json` and `wal.log`
  still sit next to it at `data/<node-id>/`.
- `kv.Store.Apply` now calls `db.Put` / `db.Delete`.
- Before Raft takes a snapshot, we call `store.Flush()` to push the memtable
  into a durable L0 SSTable, so a post-snapshot crash cannot lose writes that
  were committed before the snapshot was taken.
- `Snapshot` / `Restore` still ship a full `map[string]string` — that's
  deliberate, it keeps the wire format from M2 intact. A future milestone can
  switch to shipping SSTables directly.

### Tests
Unit tests in `mvp/internal/lsm/db_test.go`:
```
$ go test ./internal/lsm/... -v
=== RUN   TestBasicPutGetDelete           PASS
=== RUN   TestFlushAndRead                PASS
=== RUN   TestTombstoneAcrossLevels       PASS
=== RUN   TestCompaction                  PASS
=== RUN   TestOverwriteSemantics          PASS
=== RUN   Test10kKeysPersist              PASS  (persisted to 3 SSTable file(s))
=== RUN   TestRestore                     PASS
ok  github.com/foysal/distkv/internal/lsm  0.72s
```
`Test10kKeysPersist` is the headline case: insert 10,000 keys → verify reads →
`Close` → reopen → reads every key again → asserts `.sst` files exist on disk.

Integration test (3-node Raft cluster, `mvp/scripts/test_m3.sh`) with a 4 KiB
flush threshold to force multiple L0 flushes:
```
=== phase 2: write 1000 keys (will flush LSM memtable multiple times) ===
=== phase 5: inspect on-disk LSMs ===
  n1: 2 SSTable file(s)  (L0-...sst 15798 bytes, L0-...sst 15304 bytes)
  n2: 2 SSTable file(s)  (...)
  n3: 2 SSTable file(s)  (...)
=== phase 6: restart cluster ===
=== phase 7: verify all 1000 keys ===
M3 LSM TEST: PASSED (1000/1000 keys recovered, 6 SSTable(s) on disk)
```

The existing M2 crash-recovery test (`scripts/test_m2.sh`) continues to pass
unchanged, confirming the LSM swap did not regress WAL / snapshot recovery.

### Known limitations (intentional, for later milestones)
- No block-level index or compression — the index has one entry per record.
- L1 is a single run; there is no L2+.
- `SnapshotAll` scans every SSTable to build the full map — fine for
  hundreds of thousands of keys, not for a serious workload.
- No WAL inside the LSM itself; durability of un-flushed memtable writes
  comes from Raft's WAL above us.
