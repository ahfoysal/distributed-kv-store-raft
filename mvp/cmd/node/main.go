package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/foysal/distkv/internal/kv"
	"github.com/foysal/distkv/internal/lsm"
	"github.com/foysal/distkv/internal/mvcc"
	"github.com/foysal/distkv/internal/raft"
	"github.com/foysal/distkv/internal/transport"
	"github.com/foysal/distkv/internal/txn"
)

func main() {
	id := flag.String("id", "", "node id (e.g. n1)")
	addr := flag.String("addr", "", "listen addr (e.g. 127.0.0.1:9001)")
	peerList := flag.String("peers", "", "comma-separated id@addr list (excluding self)")
	dataDir := flag.String("data", "data", "data directory root (per-node subdir is created)")
	snapshotEvery := flag.Int("snapshot-every", 100, "take a snapshot every N applied entries (0 disables)")
	lsmFlushBytes := flag.Int("lsm-flush-bytes", 0, "LSM memtable flush threshold in bytes (0 = default 1MiB)")
	lsmL0Trigger := flag.Int("lsm-l0-trigger", 0, "compact when L0 has this many tables (0 = default 4)")
	flag.Parse()

	if *id == "" || *addr == "" {
		log.Fatal("--id and --addr required")
	}

	var peers []raft.Peer
	if *peerList != "" {
		for _, p := range strings.Split(*peerList, ",") {
			parts := strings.SplitN(p, "@", 2)
			if len(parts) != 2 {
				log.Fatalf("bad peer: %s", p)
			}
			peers = append(peers, raft.Peer{ID: parts[0], Addr: parts[1]})
		}
	}

	applyCh := make(chan raft.ApplyMsg, 256)
	rpc := transport.NewHTTP()
	node := raft.NewNode(*id, peers, rpc, applyCh)

	// Persistence (M2 + M3): one directory per node, containing wal.log,
	// state.json, snapshot.json, and an lsm/ subdir with SSTables (M3).
	nodeDir := filepath.Join(*dataDir, *id)
	store, err := kv.NewWithOptions(filepath.Join(nodeDir, "lsm"), lsm.Options{
		FlushThresholdBytes: *lsmFlushBytes,
		L0Trigger:           *lsmL0Trigger,
	})
	if err != nil {
		log.Fatalf("open kv store: %v", err)
	}
	persister, err := raft.NewPersister(nodeDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}

	// M4: MVCC store + transaction manager. The MVCC store lives alongside the
	// regular KV store. Committed MVCC batches are routed through Raft as
	// Op{Kind:"txn_commit"} entries so every replica replays them in order.
	mvccStore := mvcc.New()
	hlc := txn.NewHLC()
	txnMgr := txn.NewManager(mvccStore, hlc)
	store.TxnApplier = func(payload []byte) {
		var batch mvcc.CommitBatch
		if err := json.Unmarshal(payload, &batch); err != nil {
			log.Printf("mvcc apply: bad payload: %v", err)
			return
		}
		mvccStore.ApplyCommit(batch)
		hlc.Update(batch.CommitTS)
	}

	// Synchronous applier so snapshots always see fully-applied KV state.
	node.SetApplier(func(cmd string) { store.Apply(cmd) })
	// Wrap Snapshot to flush the LSM memtable first, so a post-snapshot crash
	// can't leave committed KV state behind a WAL-truncated Raft log.
	snapshotFn := func() map[string]string {
		if err := store.Flush(); err != nil {
			log.Printf("kv flush before snapshot: %v", err)
		}
		data := store.Snapshot()
		// M4: piggy-back MVCC state on the same snapshot map under a reserved
		// prefix. Restore() uses the inverse routing.
		mvccStore.EncodeForSnapshot(data)
		return data
	}
	restoreFn := func(data map[string]string) {
		// Pull MVCC-prefixed entries out first (DecodeFromSnapshot deletes them
		// from the map) so store.Restore only sees plain KV pairs.
		mvccStore.DecodeFromSnapshot(data)
		store.Restore(data)
	}
	if err := node.AttachPersistence(persister, *snapshotEvery, snapshotFn, restoreFn); err != nil {
		log.Fatalf("attach persistence: %v", err)
	}

	// applyCh is unused when an applier is set, but drain it defensively.
	go func() {
		for msg := range applyCh {
			_ = msg
		}
	}()

	mux := http.NewServeMux()

	mux.HandleFunc("/raft/vote", func(w http.ResponseWriter, r *http.Request) {
		var args raft.RequestVoteArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		reply := node.HandleRequestVote(args)
		_ = json.NewEncoder(w).Encode(reply)
	})

	mux.HandleFunc("/raft/append", func(w http.ResponseWriter, r *http.Request) {
		var args raft.AppendEntriesArgs
		if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		reply := node.HandleAppendEntries(args)
		_ = json.NewEncoder(w).Encode(reply)
	})

	// Client API
	mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, ok := store.Get(key)
		_ = json.NewEncoder(w).Encode(map[string]any{"value": v, "found": ok})
	})

	mux.HandleFunc("/kv/set", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		idx, term, isLeader := node.Propose(kv.EncodeSet(body.Key, body.Value))
		if !isLeader {
			_, _, leader := node.State()
			http.Error(w, "not leader, try "+leader, 421)
			return
		}
		// Wait briefly for commit (poll)
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if v, ok := store.Get(body.Key); ok && v == body.Value {
				_ = json.NewEncoder(w).Encode(map[string]any{"index": idx, "term": term, "committed": true})
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		http.Error(w, "commit timeout", 504)
	})

	// --- M4: transaction endpoints -----------------------------------------
	// These are leader-only: the transaction manager/HLC live on the leader,
	// commits are replicated as Op{Kind:"txn_commit"} entries, and replicas
	// apply them via store.TxnApplier.

	leaderOnly := func(w http.ResponseWriter) bool {
		state, _, leader := node.State()
		if state != raft.Leader {
			http.Error(w, "not leader, try "+leader, 421)
			return false
		}
		return true
	}

	mux.HandleFunc("/txn/begin", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		t := txnMgr.Begin()
		_ = json.NewEncoder(w).Encode(map[string]any{"txn_id": t.ID, "read_ts": t.ReadTS})
	})

	mux.HandleFunc("/txn/get", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		var body struct{ TxnID, Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		v, found, err := txnMgr.Get(body.TxnID, body.Key)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"value": v, "found": found})
	})

	mux.HandleFunc("/txn/put", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		var body struct{ TxnID, Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := txnMgr.Put(body.TxnID, body.Key, body.Value); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	mux.HandleFunc("/txn/delete", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		var body struct{ TxnID, Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := txnMgr.Delete(body.TxnID, body.Key); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	mux.HandleFunc("/txn/abort", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		var body struct{ TxnID string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := txnMgr.Abort(body.TxnID); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"aborted": true})
	})

	mux.HandleFunc("/txn/commit", func(w http.ResponseWriter, r *http.Request) {
		if !leaderOnly(w) {
			return
		}
		var body struct{ TxnID string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		batch, committed, err := txnMgr.Commit(body.TxnID)
		if err != nil {
			// Aborted-due-to-conflict is a 409. Other errors (unknown txn) are 400.
			if err == txn.ErrAborted {
				_ = json.NewEncoder(w).Encode(map[string]any{"committed": false, "reason": err.Error()})
				return
			}
			http.Error(w, err.Error(), 400)
			return
		}

		// Read-only transactions still need a response but bypass Raft.
		if !committed || len(batch.Writes) == 0 {
			_ = json.NewEncoder(w).Encode(map[string]any{"committed": true, "commit_ts": batch.CommitTS, "writes": 0})
			return
		}

		payload, _ := json.Marshal(batch)
		cmd := kv.EncodeTxnCommit(payload)
		idx, term, isLeader := node.Propose(cmd)
		if !isLeader {
			_, _, leader := node.State()
			http.Error(w, "not leader, try "+leader, 421)
			return
		}
		// Wait briefly for commit to be applied (we detect via a new version
		// appearing at commit_ts on any one of the written keys).
		probeKey := batch.Writes[0].Key
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if mvccStore.LastCommitTS(probeKey) >= batch.CommitTS {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"committed": true,
					"commit_ts": batch.CommitTS,
					"writes":    len(batch.Writes),
					"index":     idx,
					"term":      term,
				})
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		http.Error(w, "commit timeout", 504)
	})

	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		state, term, leader := node.State()
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":     *id,
			"state":  state.String(),
			"term":   term,
			"leader": leader,
		})
	})

	node.Start()
	fmt.Printf("[%s] listening on %s\n", *id, *addr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
