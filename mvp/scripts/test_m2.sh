#!/usr/bin/env bash
# M2 crash-recovery test:
#   1. Start 3 nodes.
#   2. Set 10 keys via the leader.
#   3. Kill all 3.
#   4. Restart all 3.
#   5. Verify every key is readable.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_DIR="$ROOT/data"
LOG_DIR="$ROOT/tmp"
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

PEERS_FOR_N1="n2@127.0.0.1:9002,n3@127.0.0.1:9003"
PEERS_FOR_N2="n1@127.0.0.1:9001,n3@127.0.0.1:9003"
PEERS_FOR_N3="n1@127.0.0.1:9001,n2@127.0.0.1:9002"

go build -o "$LOG_DIR/distkv-node" ./cmd/node

start_cluster() {
    "$LOG_DIR/distkv-node" --id n1 --addr 127.0.0.1:9001 --peers "$PEERS_FOR_N1" --data "$DATA_DIR" >"$LOG_DIR/n1.log" 2>&1 &
    echo $! > "$LOG_DIR/n1.pid"
    "$LOG_DIR/distkv-node" --id n2 --addr 127.0.0.1:9002 --peers "$PEERS_FOR_N2" --data "$DATA_DIR" >"$LOG_DIR/n2.log" 2>&1 &
    echo $! > "$LOG_DIR/n2.pid"
    "$LOG_DIR/distkv-node" --id n3 --addr 127.0.0.1:9003 --peers "$PEERS_FOR_N3" --data "$DATA_DIR" >"$LOG_DIR/n3.log" 2>&1 &
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
    # Paranoia: kill anything still listening on those ports.
    for port in 9001 9002 9003; do
        lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    done
    sleep 0.5
}

find_leader() {
    for port in 9001 9002 9003; do
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
        resp=$(curl -s -o /tmp/distkv-set-body -w '%{http_code}' -X POST \
            -H 'Content-Type: application/json' \
            -d "{\"Key\":\"$key\",\"Value\":\"$val\"}" \
            "http://127.0.0.1:$leader_port/kv/set" || echo "000")
        if [ "$resp" = "200" ]; then return 0; fi
        sleep 0.3
        leader_port=$(wait_for_leader)
        attempts=$((attempts+1))
    done
    echo "FAIL set $key"
    return 1
}

echo "=== phase 1: start cluster ==="
kill_cluster
start_cluster
leader=$(wait_for_leader) || { echo "no leader elected"; kill_cluster; exit 1; }
echo "leader on port $leader"

echo "=== phase 2: set 10 keys ==="
for i in $(seq 1 10); do
    set_key "$leader" "key$i" "value$i"
done

echo "=== verify before kill ==="
for i in $(seq 1 10); do
    v=$(curl -s "http://127.0.0.1:$leader/kv/get?key=key$i")
    echo "  key$i -> $v"
done

echo "=== phase 3: kill all nodes ==="
kill_cluster

echo "=== on-disk artifacts ==="
ls -la "$DATA_DIR"/n1/ || true
for id in n1 n2 n3; do
    wal="$DATA_DIR/$id/wal.log"
    state="$DATA_DIR/$id/state.json"
    echo "-- $id --"
    echo "  state.json: $(cat "$state" 2>/dev/null || echo missing)"
    echo "  wal.log lines: $(wc -l < "$wal" 2>/dev/null || echo 0)"
done

echo "=== phase 4: restart cluster ==="
start_cluster
leader=$(wait_for_leader) || { echo "no leader after restart"; kill_cluster; exit 1; }
echo "leader after restart on port $leader"

echo "=== phase 5: verify all 10 keys recovered ==="
fail=0
for i in $(seq 1 10); do
    resp=$(curl -s "http://127.0.0.1:$leader/kv/get?key=key$i")
    expected="\"value\":\"value$i\""
    if echo "$resp" | grep -q "$expected"; then
        echo "  OK  key$i -> $resp"
    else
        echo "  MISS key$i -> $resp"
        fail=$((fail+1))
    fi
done

kill_cluster

if [ $fail -eq 0 ]; then
    echo
    echo "M2 CRASH-RECOVERY TEST: PASSED (10/10 keys recovered)"
    exit 0
else
    echo
    echo "M2 CRASH-RECOVERY TEST: FAILED ($fail missing)"
    exit 1
fi
