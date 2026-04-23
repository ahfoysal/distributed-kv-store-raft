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
