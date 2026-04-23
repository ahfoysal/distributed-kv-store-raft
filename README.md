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
