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

## M4 Status: MVCC + SSI transactions (shipped)

Adds multi-version concurrency control and optimistic serializable snapshot
isolation on top of the Raft-replicated state machine. Clients now have a
first-class transaction API (`/txn/begin`, `/txn/get`, `/txn/put`,
`/txn/delete`, `/txn/commit`, `/txn/abort`) with strong isolation guarantees.

### Package layout
| Path                    | Role                                                               |
|-------------------------|--------------------------------------------------------------------|
| `internal/mvcc/mvcc.go` | Key → `[(commit_ts, value|tombstone), ...]` store; `GetAt(key, ts)`; SSI conflict helpers |
| `internal/txn/hlc.go`   | Hybrid logical clock: 48 bits physical ms, 16 bits counter         |
| `internal/txn/manager.go` | Txn lifecycle: `Begin`/`Get`/`Put`/`Delete`/`Commit`/`Abort` with OCC validation |
| `cmd/node/main.go`      | Wires MVCC+txn into Raft; adds the `/txn/*` HTTP endpoints         |
| `internal/kv/store.go`  | New `Op{Kind:"txn_commit"}` routes replicated batches to MVCC      |

### Protocol
- **BEGIN**: allocate `read_ts = HLC.Now()`, return txn id.
- **GET**: record key in the txn's read set, return
  `store.GetAt(key, read_ts)` — the newest version whose commit timestamp
  is ≤ `read_ts`. Reads honor read-your-own-writes through the buffered
  write set.
- **PUT / DELETE**: buffered in the write set; invisible to other txns.
- **COMMIT**: allocate `commit_ts = HLC.Now()`. SSI validation: for every key
  the txn read (and every key it writes), require that no committed version
  exists with commit timestamp in `(read_ts, commit_ts]`. If validation
  passes, propose a `txn_commit` entry through Raft; every replica applies
  it via `store.TxnApplier`, which calls `mvcc.Store.ApplyCommit`.
  Otherwise return `{"committed":false}`.
- **ABORT**: drop buffered state.

### Durability & replication
Commits replicate through the existing Raft log — the new kv `Op` kind
`txn_commit` carries the MVCC `CommitBatch` as its payload. Followers apply
it exactly the way they apply `set`/`delete`. Snapshots are extended: the
MVCC store serializes every `(user_key, commit_ts) → version` pair into the
Raft snapshot map under a reserved `\x01mvcc\x00` prefix, and the restore
path reassembles version lists before handing the remaining plain pairs to
the LSM.

### Tests

Unit tests in `internal/txn/manager_test.go`:
```
$ go test ./internal/txn/... -v
=== RUN   TestBasicCommit            PASS
=== RUN   TestSnapshotIsolation      PASS
=== RUN   TestSSIConflict            PASS
=== RUN   TestConcurrentSSI          PASS  (24 committed, 26 aborted of 50)
=== RUN   TestReadPhantomConflict    PASS
=== RUN   TestSnapshotRoundTrip      PASS
ok  github.com/foysal/distkv/internal/txn  0.47s
```

Integration test (`scripts/test_m4.sh`) — the concurrent-txn scenario from
the M4 spec, verified end-to-end over HTTP against a 3-node cluster:
```
=== phase 3: begin two concurrent txns A and B ===
=== phase 4: both read k ===
  A read -> {"found":true,"value":"1"}
  B read -> {"found":true,"value":"1"}
=== phase 5: A writes k=2 and commits ===
  A commit -> {"commit_ts":116454935796711424,"committed":true,"index":2,"term":1,"writes":1}
=== phase 6: B writes k=3 and tries to commit (expect ABORT) ===
  B commit -> {"committed":false,"reason":"txn aborted: serialization conflict (SSI)"}
=== phase 7: verify final value is 2 ===
  fresh read -> {"found":true,"value":"2"}
M4 MVCC+SSI TEST: PASSED (A committed k=2, B aborted on read-write conflict)
```

### Known limitations (intentional, for later milestones)
- The transaction manager and HLC live on the leader only. A leader change
  with in-flight transactions drops them; we return 421 for every `/txn/*`
  request that does not arrive at the leader. A future milestone can push
  the txn coordinator onto a quorum-committed state.
- MVCC state lives in an in-memory map; on-disk representation only exists
  via the Raft snapshot encoding. A future milestone can persist MVCC
  versions directly into the LSM using the `user_key | reverseTS` scheme
  (the encoder/decoder in `mvcc.go` already implements it).
- No GC of old versions — every committed version lives until the process
  is restarted and the Raft snapshot truncates the log behind it.
