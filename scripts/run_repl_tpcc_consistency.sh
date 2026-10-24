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
BIN="$ROOT_DIR/target/${BUILD_PROFILE}/mydb"

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
if [[ "${KEEP_ON_FAIL:-0}" != "1" ]]; then trap cleanup EXIT; fi

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
conn=jdbc:mysql://${HOST}:${MASTER_PORT}/tpcc?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&cachePrepStmts=true&useServerPrepStmts=true&prepStmtCacheSize=256&prepStmtCacheSqlLimit=2048&maintainTimeStats=false&rewriteBatchedStatements=true
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

echo "=== waiting for the replica to catch up ==="
master_lsn() {
  mysql -h"$HOST" -P"$MASTER_PORT" -uroot -N -e "SHOW MYDB MASTER STATUS" 2>/dev/null | awk '{print $6}'
}
replica_lsn() {
  mysql -h"$HOST" -P"$REPLICA_PORT" -uroot -N -e "SHOW MYDB REPLICA STATUS" 2>/dev/null | awk '{print $10}'
}
deadline=$((SECONDS + ${CATCHUP_TIMEOUT_S:-300}))
while true; do
  m="$(master_lsn)"; sleep 1; m2="$(master_lsn)"; r="$(replica_lsn)"
  echo "master_lsn=$m2 replica_applied=$r"
  if [[ -n "$m" && "$m" == "$m2" && -n "$r" && "$r" -ge "$m2" ]]; then
    echo "replica caught up at LSN $r"
    break
  fi
  if (( SECONDS > deadline )); then
    echo "FAIL: replica did not catch up within ${CATCHUP_TIMEOUT_S:-300}s" >&2
    exit 1
  fi
  sleep 1
done

echo "=== comparing per-table checksums ==="
declare -a CHECKS=(
  "warehouse|SELECT COUNT(*), SUM(w_ytd) FROM bmsql_warehouse"
  "district|SELECT COUNT(*), SUM(d_ytd), SUM(d_next_o_id) FROM bmsql_district"
  "customer|SELECT COUNT(*), SUM(c_balance), SUM(c_ytd_payment), SUM(c_payment_cnt), SUM(c_delivery_cnt) FROM bmsql_customer"
  "history|SELECT COUNT(*), SUM(h_amount) FROM bmsql_history"
  "oorder|SELECT COUNT(*), SUM(o_id), SUM(o_carrier_id), SUM(o_ol_cnt) FROM bmsql_oorder"
  "new_order|SELECT COUNT(*), SUM(no_o_id) FROM bmsql_new_order"
  "order_line|SELECT COUNT(*), SUM(ol_amount), SUM(ol_quantity), SUM(ol_delivery_d IS NOT NULL) FROM bmsql_order_line"
  "stock|SELECT COUNT(*), SUM(s_quantity), SUM(s_ytd), SUM(s_order_cnt), SUM(s_remote_cnt) FROM bmsql_stock"
  "item|SELECT COUNT(*), SUM(i_price) FROM bmsql_item"
)
fail=0
for entry in "${CHECKS[@]}"; do
  table="${entry%%|*}"
  query="${entry#*|}"
  mrow="$(mysql -h"$HOST" -P"$MASTER_PORT" -uroot -N tpcc -e "$query")"
  rrow="$(mysql -h"$HOST" -P"$REPLICA_PORT" -uroot -N tpcc -e "$query")"
  if [[ "$mrow" == "$rrow" ]]; then
    echo "OK   $table: $mrow"
  else
    echo "FAIL $table:"
    echo "  master : $mrow"
    echo "  replica: $rrow"
    fail=1
  fi
done

if [[ "$fail" == "0" ]]; then
  echo "=== RESULT: master and replica are CONSISTENT across all TPC-C tables ==="
else
  echo "=== RESULT: INCONSISTENCY DETECTED ===" >&2
  exit 1
fi
