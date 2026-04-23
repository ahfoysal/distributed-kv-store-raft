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
	"github.com/foysal/distkv/internal/raft"
	"github.com/foysal/distkv/internal/transport"
)

func main() {
	id := flag.String("id", "", "node id (e.g. n1)")
	addr := flag.String("addr", "", "listen addr (e.g. 127.0.0.1:9001)")
	peerList := flag.String("peers", "", "comma-separated id@addr list (excluding self)")
	dataDir := flag.String("data", "data", "data directory root (per-node subdir is created)")
	snapshotEvery := flag.Int("snapshot-every", 100, "take a snapshot every N applied entries (0 disables)")
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

	store := kv.New()
	applyCh := make(chan raft.ApplyMsg, 256)
	rpc := transport.NewHTTP()
	node := raft.NewNode(*id, peers, rpc, applyCh)

	// Persistence (M2): one directory per node, containing wal.log, state.json,
	// and snapshot.json. Load existing state before starting raft loops.
	nodeDir := filepath.Join(*dataDir, *id)
	persister, err := raft.NewPersister(nodeDir)
	if err != nil {
		log.Fatalf("persister: %v", err)
	}
	// Synchronous applier so snapshots always see fully-applied KV state.
	node.SetApplier(func(cmd string) { store.Apply(cmd) })
	if err := node.AttachPersistence(persister, *snapshotEvery, store.Snapshot, store.Restore); err != nil {
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
