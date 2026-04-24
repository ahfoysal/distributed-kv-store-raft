// Command sqlnode is the M5 demo binary: a single-process range-sharded
// KV store with a mini SQL frontend. It is intentionally independent of
// the Raft cmd/node binary — the M5-lite scope is about sharding and SQL,
// not about wiring N Raft groups, so this binary runs every shard as a
// local LSM-backed store (see internal/shard). A future milestone can
// replace each shard's storage with a Raft group and reuse the rest of
// the layer unchanged.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/foysal/distkv/internal/shard"
	"github.com/foysal/distkv/internal/sql"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9301", "listen address")
	dataDir := flag.String("data", "data-m5", "data directory root")
	splitThresh := flag.Int("split-threshold", 5000, "rebalancer auto-splits shards with more than this many keys")
	rebalance := flag.Duration("rebalance-interval", 2*time.Second, "how often the rebalancer runs (0 disables)")
	flag.Parse()

	manifestPath := filepath.Join(*dataDir, "manifest.json")
	router, err := shard.NewRouter(shard.Config{
		DataRoot:          *dataDir,
		ManifestPath:      manifestPath,
		SplitThreshold:    *splitThresh,
		RebalanceInterval: *rebalance,
	})
	if err != nil {
		log.Fatalf("open router: %v", err)
	}
	engine := sql.NewEngine(router)

	mux := http.NewServeMux()

	// Raw KV API.
	mux.HandleFunc("/kv/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		v, ok := router.Get(key)
		_ = json.NewEncoder(w).Encode(map[string]any{"value": v, "found": ok})
	})
	mux.HandleFunc("/kv/put", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key, Value string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := router.Put(body.Key, body.Value); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	mux.HandleFunc("/kv/delete", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Key string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := router.Delete(body.Key); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})

	// Shard admin API.
	mux.HandleFunc("/shards", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"shards": router.Shards()})
	})
	mux.HandleFunc("/shards/split", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ ID, SplitKey string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		left, right, err := router.Split(body.ID, body.SplitKey)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"left": left, "right": right})
	})
	mux.HandleFunc("/shards/rebalance", func(w http.ResponseWriter, r *http.Request) {
		ev := router.Rebalance()
		_ = json.NewEncoder(w).Encode(map[string]any{"splits": ev})
	})

	// SQL API.
	mux.HandleFunc("/sql", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Query string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		res, err := engine.Exec(body.Query)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		_ = json.NewEncoder(w).Encode(res)
	})

	fmt.Printf("sqlnode listening on %s (data=%s, split-threshold=%d)\n", *addr, *dataDir, *splitThresh)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
