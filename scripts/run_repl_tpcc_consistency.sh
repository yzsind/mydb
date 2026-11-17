#!/usr/bin/env bash
# TPC-C master→replica physical-replication consistency check.
#
# Starts a fresh mydb master + mydb replica pair (native physical
# replication over MYDB_REPL_PORT), runs BenchmarkSQL TPC-C against the
# master, waits for the replica to catch up, then compares per-table
# row counts and column checksums between the two instances.
#
# Usage:
#   BSQL_RUN=/path/to/benchmarksql5.1/run scripts/run_repl_tpcc_consistency.sh
set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
: "${BSQL_RUN:?Set BSQL_RUN to the BenchmarkSQL run directory}"
source "$ROOT_DIR/scripts/_docker_test.sh"

HOST="127.0.0.1"
MASTER_PORT="${MASTER_PORT:-3406}"
REPLICA_PORT="${REPLICA_PORT:-3407}"
REPL_PORT="${REPL_PORT:-3416}"
MASTER_DIR="${MASTER_DIR:-$(mktemp -d /tmp/mydb/repl-tpcc-master.XXXXXX)}"
REPLICA_DIR="${REPLICA_DIR:-$(mktemp -d /tmp/mydb/repl-tpcc-replica.XXXXXX)}"
WAREHOUSES="${WAREHOUSES:-2}"
LOAD_WORKERS="${LOAD_WORKERS:-4}"
TERMINALS="${TERMINALS:-10}"
RUN_MINS="${RUN_MINS:-1}"
PASSWORD="${MYDB_PASSWORD:-root}"
BUILD_PROFILE="${MYDB_BUILD_PROFILE:-release}"
export MYSQL_PWD="$PASSWORD"
export MYDB_REPL_AUTH_TOKEN="${MYDB_REPL_AUTH_TOKEN:-$(python3 -c 'import secrets; print(secrets.token_hex(24))')}"
BIN="${MYDB_BINARY:-$ROOT_DIR/target/${BUILD_PROFILE}/mydb}"

mydb_assert_ports_free "$MASTER_PORT" "$REPLICA_PORT" "$REPL_PORT"
mkdir -p "$MASTER_DIR" "$REPLICA_DIR"
if [[ -n "$(find "$MASTER_DIR" -mindepth 1 -maxdepth 1 -print -quit)" || \
      -n "$(find "$REPLICA_DIR" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
  echo "MASTER_DIR and REPLICA_DIR must be empty" >&2
  exit 2
fi

cleanup() {
  for pid in "${REPLICA_PID:-}" "${MASTER_PID:-}"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done
  wait 2>/dev/null || true
}
trap cleanup EXIT

wait_for_port() {
  local port="$1" pid="$2" log="$3"
  for _ in {1..200}; do
    if mysql -h"$HOST" -P"$port" -uroot -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "server on port $port exited early; log tail:" >&2
      tail -20 "$log" >&2
      exit 1
    fi
    sleep 0.2
  done
  echo "server on port $port did not accept connections; log tail:" >&2
  tail -20 "$log" >&2
  exit 1
}

echo "=== starting master (port $MASTER_PORT, repl port $REPL_PORT) ==="
MYDB_DURABILITY=strict MYDB_DATA_DIR="$MASTER_DIR" MYDB_ROOT_PASSWORD="$PASSWORD" \
  MYDB_ROLE=master MYDB_REPL_PORT="$REPL_PORT" MYDB_REPL_BIND_HOST="$HOST" \
  MYDB_SLOW_SQL_MS=5000 \
  "$BIN" --host "$HOST" --port "$MASTER_PORT" >"$MASTER_DIR/server.log" 2>&1 &
MASTER_PID=$!
wait_for_port "$MASTER_PORT" "$MASTER_PID" "$MASTER_DIR/server.log"

echo "=== starting replica (port $REPLICA_PORT) ==="
MYDB_DURABILITY=strict MYDB_DATA_DIR="$REPLICA_DIR" MYDB_ROOT_PASSWORD="$PASSWORD" \
  MYDB_ROLE=replica MYDB_REPL_MASTER_HOST="$HOST" MYDB_REPL_MASTER_PORT="$REPL_PORT" \
  MYDB_SLOW_SQL_MS=5000 \
  "$BIN" --host "$HOST" --port "$REPLICA_PORT" >"$REPLICA_DIR/server.log" 2>&1 &
REPLICA_PID=$!
wait_for_port "$REPLICA_PORT" "$REPLICA_PID" "$REPLICA_DIR/server.log"

echo "=== loading TPC-C (warehouses=$WAREHOUSES) and running ${RUN_MINS}min on the master ==="
mysql -h"$HOST" -P"$MASTER_PORT" -uroot -e "CREATE DATABASE IF NOT EXISTS tpcc"
PROPS="$(mktemp /tmp/mydb/repl-tpcc-props.XXXXXX)"
cat >"$PROPS" <<EOF
db=mysql
driver=com.mysql.cj.jdbc.Driver
conn=jdbc:mysql://${HOST}:${MASTER_PORT}/tpcc?serverTimezone=UTC&cachePrepStmts=true&useServerPrepStmts=true&prepStmtCacheSize=256&prepStmtCacheSqlLimit=2048&maintainTimeStats=false&rewriteBatchedStatements=true
user=root
password=${PASSWORD}
warehouses=${WAREHOUSES}
loadWorkers=${LOAD_WORKERS}
fastBatchInsert=true
nURandCLast=123
terminals=${TERMINALS}
runTxnsPerTerminal=0
runMins=${RUN_MINS}
limitTxnsPerMin=100000000
terminalWarehouseFixed=false
newOrderWeight=45
paymentWeight=43
orderStatusWeight=4
deliveryWeight=4
stockLevelWeight=4
resultDirectory=/tmp/mydb/repl-tpcc-result_%tY%tm%td_%tH%tM%tS
EOF
cd "$BSQL_RUN"
./runSQL.sh "$PROPS" tableDrops >/dev/null 2>&1 || true
./runSQL.sh "$PROPS" tableCreates
./runLoader.sh "$PROPS"
./runSQL.sh "$PROPS" indexCreates
./runSQL.sh "$PROPS" foreignKeys || true
./runSQL.sh "$PROPS" buildFinish || true
./runBenchmark.sh "$PROPS"

echo "=== verifying durable applied progress and complete typed rows ==="
python3 "$ROOT_DIR/scripts/replication_check.py" --physical \
  --source-port "$MASTER_PORT" --replica-port "$REPLICA_PORT" \
  --database tpcc --timeout "${CATCHUP_TIMEOUT_S:-300}"
