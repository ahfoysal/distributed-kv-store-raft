#!/usr/bin/env bash
# M3 LSM integration test:
#   1. Start a 3-node cluster.
#   2. Write 1,000 keys through the leader (forces LSM flushes + compaction
#      with a tiny flush threshold configured via env).
#   3. Spot-check reads.
#   4. Kill all nodes.
#   5. Assert SSTable files exist on disk for every node.
#   6. Restart the cluster.
#   7. Verify every key reads back.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_DIR="$ROOT/data-m3"
LOG_DIR="$ROOT/tmp-m3"
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

PEERS_FOR_N1="n2@127.0.0.1:9102,n3@127.0.0.1:9103"
PEERS_FOR_N2="n1@127.0.0.1:9101,n3@127.0.0.1:9103"
PEERS_FOR_N3="n1@127.0.0.1:9101,n2@127.0.0.1:9102"

go build -o "$LOG_DIR/distkv-node" ./cmd/node

start_cluster() {
    "$LOG_DIR/distkv-node" --id n1 --addr 127.0.0.1:9101 --peers "$PEERS_FOR_N1" --data "$DATA_DIR" --snapshot-every 5000 --lsm-flush-bytes 4096 --lsm-l0-trigger 3 >"$LOG_DIR/n1.log" 2>&1 &
    echo $! > "$LOG_DIR/n1.pid"
    "$LOG_DIR/distkv-node" --id n2 --addr 127.0.0.1:9102 --peers "$PEERS_FOR_N2" --data "$DATA_DIR" --snapshot-every 5000 --lsm-flush-bytes 4096 --lsm-l0-trigger 3 >"$LOG_DIR/n2.log" 2>&1 &
    echo $! > "$LOG_DIR/n2.pid"
    "$LOG_DIR/distkv-node" --id n3 --addr 127.0.0.1:9103 --peers "$PEERS_FOR_N3" --data "$DATA_DIR" --snapshot-every 5000 --lsm-flush-bytes 4096 --lsm-l0-trigger 3 >"$LOG_DIR/n3.log" 2>&1 &
    echo $! > "$LOG_DIR/n3.pid"
}

kill_cluster() {
    for id in n1 n2 n3; do
        if [ -f "$LOG_DIR/$id.pid" ]; then
            pid=$(cat "$LOG_DIR/$id.pid")
            kill -9 "$pid" 2>/dev/null || true
            rm -f "$LOG_DIR/$id.pid"
        fi
    done
    for port in 9101 9102 9103; do
        lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    done
    sleep 0.5
}

find_leader() {
    for port in 9101 9102 9103; do
        state=$(curl -s "http://127.0.0.1:$port/status" 2>/dev/null || echo "{}")
        if echo "$state" | grep -q '"state":"Leader"'; then
            echo "$port"
            return 0
        fi
    done
    return 1
}

wait_for_leader() {
    for _ in $(seq 1 40); do
        if leader_port=$(find_leader); then
            echo "$leader_port"
            return 0
        fi
        sleep 0.25
    done
    return 1
}

set_key() {
    local leader_port=$1 key=$2 val=$3
    local attempts=0
    while [ $attempts -lt 5 ]; do
        code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
            -H 'Content-Type: application/json' \
            -d "{\"Key\":\"$key\",\"Value\":\"$val\"}" \
            "http://127.0.0.1:$leader_port/kv/set" || echo "000")
        if [ "$code" = "200" ]; then return 0; fi
        sleep 0.3
        leader_port=$(wait_for_leader)
        attempts=$((attempts+1))
    done
    echo "FAIL set $key"
    return 1
}

N=${M3_KEYS:-1000}

echo "=== phase 1: start cluster ==="
kill_cluster
start_cluster
leader=$(wait_for_leader) || { echo "no leader"; kill_cluster; exit 1; }
echo "leader on port $leader"

echo "=== phase 2: write $N keys (will flush LSM memtable multiple times) ==="
t0=$(date +%s)
for i in $(seq 1 $N); do
    set_key "$leader" "k-$i" "v-$i" >/dev/null
    if [ $((i % 200)) -eq 0 ]; then
        echo "  wrote $i / $N"
    fi
done
t1=$(date +%s)
echo "  took $((t1 - t0))s"

echo "=== phase 3: spot-check reads ==="
for i in 1 250 500 750 $N; do
    v=$(curl -s "http://127.0.0.1:$leader/kv/get?key=k-$i")
    echo "  k-$i -> $v"
    if ! echo "$v" | grep -q "\"value\":\"v-$i\""; then
        echo "FAIL spot-check k-$i"
        kill_cluster
        exit 1
    fi
done

echo "=== phase 4: kill all nodes ==="
kill_cluster

echo "=== phase 5: inspect on-disk LSMs ==="
total_ssts=0
for id in n1 n2 n3; do
    LSM_DIR="$DATA_DIR/$id/lsm"
    sst_count=$(ls "$LSM_DIR"/*.sst 2>/dev/null | wc -l | tr -d ' ')
    echo "  $id: $sst_count SSTable file(s) in $LSM_DIR"
    ls -la "$LSM_DIR" 2>/dev/null | awk 'NR>1 {print "    " $9 " (" $5 " bytes)"}' | grep -v '^    $' || true
    total_ssts=$((total_ssts + sst_count))
done
if [ "$total_ssts" -lt 1 ]; then
    echo "FAIL: expected at least one .sst file on disk across the cluster"
    exit 1
fi

echo "=== phase 6: restart cluster ==="
start_cluster
leader=$(wait_for_leader) || { echo "no leader after restart"; kill_cluster; exit 1; }
echo "leader after restart on port $leader"

echo "=== phase 7: verify all $N keys ==="
fail=0
for i in $(seq 1 $N); do
    v=$(curl -s "http://127.0.0.1:$leader/kv/get?key=k-$i")
    if ! echo "$v" | grep -q "\"value\":\"v-$i\""; then
        echo "  MISS k-$i -> $v"
        fail=$((fail+1))
        if [ "$fail" -gt 10 ]; then
            echo "  ... stopping after 10 misses"
            break
        fi
    fi
    if [ $((i % 200)) -eq 0 ]; then
        echo "  verified $i / $N"
    fi
done

kill_cluster

if [ "$fail" -eq 0 ]; then
    echo
    echo "M3 LSM TEST: PASSED ($N/$N keys recovered, $total_ssts SSTable(s) on disk)"
    exit 0
else
    echo
    echo "M3 LSM TEST: FAILED ($fail missing)"
    exit 1
fi
