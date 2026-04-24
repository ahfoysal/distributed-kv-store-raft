#!/usr/bin/env bash
# M5 range sharding + rebalancer + mini SQL integration test:
#   1. Start sqlnode with split-threshold=3000.
#   2. CREATE TABLE users; insert 10,000 rows through the /sql endpoint.
#   3. Watch the rebalancer range-split the single shard as it grows.
#   4. Verify SELECTs cross-shard (point lookup + BETWEEN) still return
#      the right answers.
#   5. Verify UPDATE and DELETE via SQL.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_DIR="$ROOT/data-m5"
LOG_DIR="$ROOT/tmp-m5"
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

ADDR="127.0.0.1:9301"
SPLIT=3000

go build -o "$LOG_DIR/sqlnode" ./cmd/sqlnode

start_node() {
    "$LOG_DIR/sqlnode" --addr "$ADDR" --data "$DATA_DIR" \
        --split-threshold "$SPLIT" --rebalance-interval 500ms \
        >"$LOG_DIR/sqlnode.log" 2>&1 &
    echo $! > "$LOG_DIR/sqlnode.pid"
}

stop_node() {
    if [ -f "$LOG_DIR/sqlnode.pid" ]; then
        kill -9 "$(cat "$LOG_DIR/sqlnode.pid")" 2>/dev/null || true
        rm -f "$LOG_DIR/sqlnode.pid"
    fi
    lsof -ti :9301 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    sleep 0.2
}

trap stop_node EXIT

curl_json() { curl -s -H 'Content-Type: application/json' "$@"; }
sql() { curl_json -X POST "http://$ADDR/sql" -d "{\"Query\":$(python3 -c 'import json,sys;print(json.dumps(sys.argv[1]))' "$1")}"; }

echo "=== phase 1: start sqlnode (split-threshold=$SPLIT) ==="
stop_node
start_node
for _ in $(seq 1 40); do
    if curl -s "http://$ADDR/shards" >/dev/null 2>&1; then
        break
    fi
    sleep 0.1
done

echo "=== phase 2: create table ==="
sql "CREATE TABLE users (id STRING, name STRING, age INT, PRIMARY KEY (id))" | tee /dev/null
echo

echo "=== phase 3: insert 10,000 rows ==="
for i in $(seq 0 9999); do
    id=$(printf 'u%06d' "$i")
    sql "INSERT INTO users (id, name, age) VALUES ('$id', 'person-$i', $((i % 100)))" >/dev/null
    if [ $((i % 2000)) -eq 0 ] && [ $i -gt 0 ]; then
        n=$(curl -s "http://$ADDR/shards" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["shards"]))')
        echo "  after $i inserts: $n shard(s)"
    fi
done
sleep 1.0  # give the background rebalancer one more tick

echo "=== phase 4: dump shard manifest ==="
curl -s "http://$ADDR/shards" | python3 -m json.tool

nshards=$(curl -s "http://$ADDR/shards" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["shards"]))')
if [ "$nshards" -lt 3 ]; then
    echo "FAIL: expected at least 3 shards after inserting 10k rows with split-threshold=$SPLIT, got $nshards"
    exit 1
fi

echo "=== phase 5: SELECT by primary key (crosses shards) ==="
for probe in u000001 u002500 u005000 u007500 u009999; do
    out=$(sql "SELECT name FROM users WHERE id = '$probe'")
    echo "  $probe -> $out"
    echo "$out" | grep -q "person-" || { echo "FAIL: $probe lookup"; exit 1; }
done

echo "=== phase 6: SELECT ... BETWEEN (range scan across shards) ==="
out=$(sql "SELECT id FROM users WHERE id BETWEEN 'u000010' AND 'u000020'")
nrows=$(echo "$out" | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["rows"]))')
echo "  rows returned: $nrows (expected 11)"
[ "$nrows" = "11" ] || { echo "FAIL: BETWEEN rowcount"; exit 1; }

echo "=== phase 7: UPDATE + SELECT ==="
sql "UPDATE users SET age = 999 WHERE id = 'u000123'" >/dev/null
out=$(sql "SELECT age FROM users WHERE id = 'u000123'")
echo "  updated age -> $out"
echo "$out" | grep -q 999 || { echo "FAIL: UPDATE"; exit 1; }

echo "=== phase 8: DELETE + SELECT ==="
sql "DELETE FROM users WHERE id = 'u000123'" >/dev/null
out=$(sql "SELECT name FROM users WHERE id = 'u000123'")
echo "  deleted row -> $out"
# Missing/empty/null "rows" all mean "no rows" — the SELECT shape is
#   {"columns":[...]}                 (key absent)
#   {"columns":[...],"rows":null}     (explicit null)
#   {"columns":[...],"rows":[]}       (explicit empty)
nrows=$(echo "$out" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d.get("rows") or []))')
[ "$nrows" = "0" ] || { echo "FAIL: DELETE (still $nrows rows)"; exit 1; }

echo
echo "M5 SHARDING + SQL TEST: PASSED ($nshards shards, 10k rows, cross-shard SQL works)"
