#!/usr/bin/env bash
# M4 MVCC + SSI transactions integration test:
#   1. Start a 3-node cluster.
#   2. Seed key k=1 through a single txn.
#   3. Start two concurrent txns A and B. Both read k=1.
#   4. A writes k=2 and commits. B then writes k=3 and tries to commit.
#      SSI validation must abort B.
#   5. Verify the committed value is 2 via a fresh snapshot read.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

DATA_DIR="$ROOT/data-m4"
LOG_DIR="$ROOT/tmp-m4"
mkdir -p "$LOG_DIR"
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"

PEERS_FOR_N1="n2@127.0.0.1:9202,n3@127.0.0.1:9203"
PEERS_FOR_N2="n1@127.0.0.1:9201,n3@127.0.0.1:9203"
PEERS_FOR_N3="n1@127.0.0.1:9201,n2@127.0.0.1:9202"

go build -o "$LOG_DIR/distkv-node" ./cmd/node

start_cluster() {
    "$LOG_DIR/distkv-node" --id n1 --addr 127.0.0.1:9201 --peers "$PEERS_FOR_N1" --data "$DATA_DIR" >"$LOG_DIR/n1.log" 2>&1 &
    echo $! > "$LOG_DIR/n1.pid"
    "$LOG_DIR/distkv-node" --id n2 --addr 127.0.0.1:9202 --peers "$PEERS_FOR_N2" --data "$DATA_DIR" >"$LOG_DIR/n2.log" 2>&1 &
    echo $! > "$LOG_DIR/n2.pid"
    "$LOG_DIR/distkv-node" --id n3 --addr 127.0.0.1:9203 --peers "$PEERS_FOR_N3" --data "$DATA_DIR" >"$LOG_DIR/n3.log" 2>&1 &
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
    for port in 9201 9202 9203; do
        lsof -ti :$port 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    done
    sleep 0.5
}

find_leader() {
    for port in 9201 9202 9203; do
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

trap kill_cluster EXIT

echo "=== phase 1: start cluster ==="
kill_cluster
start_cluster
leader=$(wait_for_leader) || { echo "no leader"; exit 1; }
echo "leader on port $leader"
L="http://127.0.0.1:$leader"

curl_json() {
    curl -s -H 'Content-Type: application/json' "$@"
}

echo "=== phase 2: seed k=1 via a single txn ==="
begin=$(curl_json -X POST "$L/txn/begin" -d '{}')
tid=$(echo "$begin" | sed -E 's/.*"txn_id":"([^"]+)".*/\1/')
echo "  seed txn_id=$tid"
curl_json -X POST "$L/txn/put" -d "{\"TxnID\":\"$tid\",\"Key\":\"k\",\"Value\":\"1\"}" >/dev/null
commit=$(curl_json -X POST "$L/txn/commit" -d "{\"TxnID\":\"$tid\"}")
echo "  seed commit -> $commit"

echo "=== phase 3: begin two concurrent txns A and B ==="
begA=$(curl_json -X POST "$L/txn/begin" -d '{}')
begB=$(curl_json -X POST "$L/txn/begin" -d '{}')
A=$(echo "$begA" | sed -E 's/.*"txn_id":"([^"]+)".*/\1/')
B=$(echo "$begB" | sed -E 's/.*"txn_id":"([^"]+)".*/\1/')
echo "  A=$A"
echo "  B=$B"

echo "=== phase 4: both read k ==="
readA=$(curl_json -X POST "$L/txn/get" -d "{\"TxnID\":\"$A\",\"Key\":\"k\"}")
readB=$(curl_json -X POST "$L/txn/get" -d "{\"TxnID\":\"$B\",\"Key\":\"k\"}")
echo "  A read -> $readA"
echo "  B read -> $readB"
echo "$readA" | grep -q '"value":"1"' || { echo "FAIL: A did not read 1"; exit 1; }
echo "$readB" | grep -q '"value":"1"' || { echo "FAIL: B did not read 1"; exit 1; }

echo "=== phase 5: A writes k=2 and commits ==="
curl_json -X POST "$L/txn/put" -d "{\"TxnID\":\"$A\",\"Key\":\"k\",\"Value\":\"2\"}" >/dev/null
commitA=$(curl_json -X POST "$L/txn/commit" -d "{\"TxnID\":\"$A\"}")
echo "  A commit -> $commitA"
echo "$commitA" | grep -q '"committed":true' || { echo "FAIL: A should have committed"; exit 1; }

echo "=== phase 6: B writes k=3 and tries to commit (expect ABORT) ==="
curl_json -X POST "$L/txn/put" -d "{\"TxnID\":\"$B\",\"Key\":\"k\",\"Value\":\"3\"}" >/dev/null
commitB=$(curl_json -X POST "$L/txn/commit" -d "{\"TxnID\":\"$B\"}")
echo "  B commit -> $commitB"
echo "$commitB" | grep -q '"committed":false' || { echo "FAIL: B should have been aborted by SSI"; exit 1; }

echo "=== phase 7: verify final value is 2 ==="
begC=$(curl_json -X POST "$L/txn/begin" -d '{}')
C=$(echo "$begC" | sed -E 's/.*"txn_id":"([^"]+)".*/\1/')
readC=$(curl_json -X POST "$L/txn/get" -d "{\"TxnID\":\"$C\",\"Key\":\"k\"}")
echo "  fresh read -> $readC"
echo "$readC" | grep -q '"value":"2"' || { echo "FAIL: expected final value 2"; exit 1; }
curl_json -X POST "$L/txn/abort" -d "{\"TxnID\":\"$C\"}" >/dev/null

echo
echo "M4 MVCC+SSI TEST: PASSED (A committed k=2, B aborted on read-write conflict)"
