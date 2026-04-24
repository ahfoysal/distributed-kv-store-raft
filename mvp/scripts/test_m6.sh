#!/usr/bin/env bash
# M6 integration test: cross-shard 2PC + online schema change + backup/restore.
#
# 1. Start sqlnode. Manually split s0 at "m" so alice/zoe land on different
#    shards (alice < "m" on s0, zoe >= "m" on new shard).
# 2. Seed alice=100, zoe=0. Run a cross-shard 2PC transfer debiting alice
#    by 60 and crediting zoe by 60. Verify balances atomically updated.
# 3. Crash-mid-commit test: PREPARE + write commit decision but NOT apply.
#    kill -9 sqlnode. Restart. Verify recovery applied the transfer on
#    both shards.
# 4. Online schema change: CREATE TABLE, INSERT a row, ALTER TABLE ADD
#    COLUMN with default, verify new rows + old rows are both visible.
# 5. Backup: snapshot data dir to a tarball. Restore into a FRESH data
#    directory, start a second sqlnode pointing at it, verify the
#    transferred balances + schema version survive.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_DIR="$ROOT/data-m6"
LOG_DIR="$ROOT/tmp-m6"
RESTORE_DIR="$ROOT/data-m6-restore"
BACKUP_TAR="$LOG_DIR/snapshot.tar.gz"
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR" "$RESTORE_DIR" "$BACKUP_TAR"
mkdir -p "$DATA_DIR"

ADDR="127.0.0.1:9601"
ADDR2="127.0.0.1:9602"

go build -o "$LOG_DIR/sqlnode" ./cmd/sqlnode

start_node() {
    local data="$1" addr="$2" tag="$3"
    "$LOG_DIR/sqlnode" --addr "$addr" --data "$data" \
        --split-threshold 0 --rebalance-interval 0 \
        >"$LOG_DIR/$tag.log" 2>&1 &
    echo $! > "$LOG_DIR/$tag.pid"
    for _ in $(seq 1 40); do
        if curl -s "http://$addr/shards" >/dev/null 2>&1; then
            return
        fi
        sleep 0.1
    done
    echo "FAIL: $tag did not come up"
    cat "$LOG_DIR/$tag.log"
    exit 1
}

stop_node() {
    local tag="$1"
    if [ -f "$LOG_DIR/$tag.pid" ]; then
        kill -9 "$(cat "$LOG_DIR/$tag.pid")" 2>/dev/null || true
        rm -f "$LOG_DIR/$tag.pid"
    fi
    sleep 0.2
}

cleanup() {
    stop_node node
    stop_node node2
    lsof -ti :9601 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    lsof -ti :9602 2>/dev/null | xargs -r kill -9 2>/dev/null || true
}
trap cleanup EXIT

curl_json() { curl -s -H 'Content-Type: application/json' "$@"; }
sql() { curl_json -X POST "http://$ADDR/sql" -d "{\"Query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}"; }

echo "=== phase 1: start sqlnode ==="
start_node "$DATA_DIR" "$ADDR" node

echo "=== phase 2: split s0 at \"m\" so alice(<m) and zoe(>=m) live on different shards ==="
curl_json -X POST "http://$ADDR/shards/split" -d '{"ID":"s0","SplitKey":"m"}' | python3 -m json.tool
nshards=$(curl -s "http://$ADDR/shards" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["shards"]))')
echo "  shards: $nshards"
[ "$nshards" = "2" ] || { echo "FAIL: expected 2 shards"; exit 1; }

echo "=== phase 3: seed balances alice=100, zoe=0 ==="
curl_json -X POST "http://$ADDR/kv/put" -d '{"Key":"alice","Value":"100"}' >/dev/null
curl_json -X POST "http://$ADDR/kv/put" -d '{"Key":"zoe","Value":"0"}' >/dev/null

echo "=== phase 4: cross-shard 2PC transfer: alice -= 60, zoe += 60 ==="
out=$(curl_json -X POST "http://$ADDR/txn2pc/commit" -d '{"ops":[{"kind":"put","key":"alice","value":"40"},{"kind":"put","key":"zoe","value":"60"}]}')
echo "  response: $out"
echo "$out" | grep -q '"outcome":"commit"' || { echo "FAIL: 2PC outcome not commit"; exit 1; }
a=$(curl -s "http://$ADDR/kv/get?key=alice" | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
z=$(curl -s "http://$ADDR/kv/get?key=zoe"   | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
echo "  post-commit: alice=$a zoe=$z"
[ "$a" = "40" ] && [ "$z" = "60" ] || { echo "FAIL: balances wrong after 2PC commit"; exit 1; }

echo "=== phase 5: CRASH-MID-COMMIT: alice -> 10, zoe -> 90 (prepare+decide, skip apply, then kill -9) ==="
out=$(curl_json -X POST "http://$ADDR/txn2pc/crash_after_decision" -d '{"ops":[{"kind":"put","key":"alice","value":"10"},{"kind":"put","key":"zoe","value":"90"}]}')
echo "  response: $out"
# Balances must NOT have been updated yet (apply skipped).
a=$(curl -s "http://$ADDR/kv/get?key=alice" | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
z=$(curl -s "http://$ADDR/kv/get?key=zoe"   | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
echo "  pre-kill (not yet applied): alice=$a zoe=$z"
[ "$a" = "40" ] && [ "$z" = "60" ] || { echo "FAIL: apply should not have happened yet"; exit 1; }

echo "=== phase 6: kill -9 sqlnode and restart ==="
stop_node node
start_node "$DATA_DIR" "$ADDR" node
echo "  waiting for recovery to replay the committed-but-not-applied txn..."
sleep 0.3

echo "=== phase 7: verify recovery applied the crashed txn on both shards ==="
a=$(curl -s "http://$ADDR/kv/get?key=alice" | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
z=$(curl -s "http://$ADDR/kv/get?key=zoe"   | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
echo "  post-recovery: alice=$a zoe=$z"
[ "$a" = "10" ] && [ "$z" = "90" ] || { echo "FAIL: recovery did not atomically apply 2PC txn"; exit 1; }

echo "=== phase 8: online schema change (ADD COLUMN, DROP COLUMN) ==="
sql "CREATE TABLE users (id STRING, name STRING, PRIMARY KEY (id))" >/dev/null
sql "INSERT INTO users (id, name) VALUES ('u1', 'alice-user')" >/dev/null
sql "ALTER TABLE users ADD COLUMN email STRING DEFAULT ''" >/dev/null
sql "INSERT INTO users (id, name, email) VALUES ('u2', 'bob-user', 'bob@example.com')" >/dev/null
out=$(sql "SELECT id, name, email FROM users WHERE id = 'u1'")
echo "  u1 -> $out"
echo "$out" | grep -q '"rows"' || { echo "FAIL: u1 select empty"; exit 1; }
out=$(sql "SELECT id, name, email FROM users WHERE id = 'u2'")
echo "  u2 -> $out"
echo "$out" | grep -q 'bob@example.com' || { echo "FAIL: u2 missing email"; exit 1; }
sql "ALTER TABLE users DROP COLUMN name" >/dev/null
# After DROP, SELECT of remaining columns works.
out=$(sql "SELECT id, email FROM users WHERE id = 'u2'")
echo "  u2 after DROP name -> $out"
echo "$out" | grep -q 'bob@example.com' || { echo "FAIL: SELECT after DROP COLUMN"; exit 1; }
# Catalog shows 3 versions (create + add + drop).
nversions=$(curl -s "http://$ADDR/catalog" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["tables"][0]["versions"]))')
echo "  schema versions on users: $nversions"
[ "$nversions" = "3" ] || { echo "FAIL: expected 3 schema versions, got $nversions"; exit 1; }

echo "=== phase 9: backup cluster to tarball ==="
curl_json -X POST "http://$ADDR/backup" -d "{\"Out\":\"$BACKUP_TAR\"}" | python3 -m json.tool
ls -lh "$BACKUP_TAR"

echo "=== phase 10: restore into a fresh cluster, start it, verify state ==="
stop_node node
mkdir -p "$LOG_DIR/restore-tool"
cat > "$LOG_DIR/restore-tool/main.go" <<'EOF'
package main
import (
    "log"
    "os"
    "github.com/foysal/distkv/internal/backup"
)
func main() {
    if err := backup.Restore(os.Args[1], os.Args[2]); err != nil {
        log.Fatalf("restore: %v", err)
    }
}
EOF
go build -o "$LOG_DIR/restore-tool/restore" "$LOG_DIR/restore-tool"
"$LOG_DIR/restore-tool/restore" "$BACKUP_TAR" "$RESTORE_DIR"
start_node "$RESTORE_DIR" "$ADDR2" node2
a=$(curl -s "http://$ADDR2/kv/get?key=alice" | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
z=$(curl -s "http://$ADDR2/kv/get?key=zoe"   | python3 -c 'import json,sys;print(json.load(sys.stdin)["value"])')
echo "  restored cluster: alice=$a zoe=$z"
[ "$a" = "10" ] && [ "$z" = "90" ] || { echo "FAIL: restored balances wrong"; exit 1; }
nversions=$(curl -s "http://$ADDR2/catalog" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["tables"][0]["versions"]))')
echo "  restored schema versions on users: $nversions"
[ "$nversions" = "3" ] || { echo "FAIL: restored catalog lost versions"; exit 1; }

echo
echo "M6 2PC + SCHEMA CHANGE + BACKUP/RESTORE TEST: PASSED"
