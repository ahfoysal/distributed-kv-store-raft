// Command sqlnode is the M5/M6 demo binary: a single-process range-sharded
// KV store with a mini SQL frontend, a cross-shard 2PC coordinator, an
// online schema-change catalog, and backup/restore endpoints.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"path/filepath"
	"time"

	"github.com/foysal/distkv/internal/backup"
	"github.com/foysal/distkv/internal/schema"
	"github.com/foysal/distkv/internal/shard"
	"github.com/foysal/distkv/internal/sql"
	"github.com/foysal/distkv/internal/twopc"
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

	// M6: schema catalog + 2PC coordinator + recovery pass on boot.
	cat, err := schema.Load(filepath.Join(*dataDir, "catalog.json"))
	if err != nil {
		log.Fatalf("open catalog: %v", err)
	}
	coord, err := twopc.NewCoordinator(*dataDir, router)
	if err != nil {
		log.Fatalf("open 2pc coordinator: %v", err)
	}
	if committed, aborted, err := coord.Recover(); err != nil {
		log.Fatalf("2pc recover: %v", err)
	} else if committed+aborted > 0 {
		log.Printf("2pc recovery: %d committed, %d aborted", committed, aborted)
	}

	engine := sql.NewEngine(router)
	engine.AttachCatalog(cat)

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

	// M6: cross-shard 2PC. Body shape:
	//   {"ops":[{"kind":"put","key":"alice","value":"40"},
	//           {"kind":"put","key":"zoe","value":"60"}]}
	// The coordinator decides commit/abort and returns the outcome + txn id.
	mux.HandleFunc("/txn2pc/commit", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ops []twopc.Op `json:"ops"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		txn, outcome, err := coord.Commit(body.Ops)
		if err != nil {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"txn": txn, "outcome": outcome, "error": err.Error(),
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"txn": txn, "outcome": outcome,
		})
	})

	// M6: CRASH-SIMULATION knob — perform PREPARE on both participants
	// and write the commit decision but NOT the apply step. A subsequent
	// process restart will have to recover the txn. The endpoint exists
	// only for the integration test; production code would never expose
	// this.
	mux.HandleFunc("/txn2pc/crash_after_decision", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Ops []twopc.Op `json:"ops"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		txn, err := coord.PrepareAndDecideCommitNoApply(body.Ops)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"txn": txn, "state": "committed-but-not-applied",
		})
	})

	// M6: schema introspection + online ALTER (handled via /sql for
	// statement shape, but /catalog exposes the version chain directly).
	mux.HandleFunc("/catalog", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"tables": cat.Snapshot()})
	})

	// M6: backup. Writes a tarball to the requested path.
	mux.HandleFunc("/backup", func(w http.ResponseWriter, r *http.Request) {
		var body struct{ Out string }
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		// Flush every shard before archiving so the tarball captures
		// a complete on-disk state.
		for _, info := range router.Shards() {
			if sh := router.Shard(info.ID); sh != nil {
				sh.Flush()
			}
		}
		if err := backup.Create(*dataDir, body.Out); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "out": body.Out})
	})

	fmt.Printf("sqlnode listening on %s (data=%s, split-threshold=%d)\n", *addr, *dataDir, *splitThresh)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
