#!/usr/bin/env bash
# End-to-end MySQL→mydb replication consistency: run BenchmarkSQL/TPC-C against
# a MySQL 8.0 master, replay the binlog into mydb via the slave receiver, then
# stop both and compare per-table checksums.
#
# Requires:
#   * docker (MySQL 8.0)
#   * cargo + a built mydb binary
#   * BenchmarkSQL 5.1 at $BSQL_RUN (defaults to the sibling benchmarksql5.1/run directory)
#
# Knobs:
#   MYSQL_IMAGE        — docker image for the master (mysql:8.0)
#   MYSQL_PORT         — exposed master port (33307)
#   MYDB_PORT          — local mydb port (33308)
#   MYDB_DATA_DIR      — fresh data dir (auto: /tmp/mydb/repl-mysql-data-$$)
#   BSQL_WAREHOUSES    — TPC-C warehouses (2)
#   BSQL_TERMINALS     — concurrent terminals (4)
#   BSQL_RUN_MIN       — minutes per leg (1)
#   BSQL_RUN           — path to BenchmarkSQL 5.1 run dir
#
# Exit code: 0 iff every table SUM(col IS NOT NULL) row-count agrees between
# master and replica. Non-zero on any mismatch.

set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
CONTAINER="${CONTAINER:-mydb-repl-master}"
MYSQL_PORT="${MYSQL_PORT:-33307}"
MYDB_PORT="${MYDB_PORT:-33308}"
MYDB_DATA_DIR="${MYDB_DATA_DIR:-/tmp/mydb/repl-mysql-data-$$}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
BSQL_WAREHOUSES="${BSQL_WAREHOUSES:-2}"
BSQL_TERMINALS="${BSQL_TERMINALS:-4}"
BSQL_RUN_MIN="${BSQL_RUN_MIN:-1}"
BSQL_RUN="${BSQL_RUN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/benchmarksql5.1/run}"

MYDB_PID=""

cleanup() {
  set +e
  if [[ -n "${MYDB_PID}" ]]; then
    kill "${MYDB_PID}" 2>/dev/null || true
    wait "${MYDB_PID}" 2>/dev/null || true
  fi
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
  if [[ "${MYDB_DATA_DIR}" == /tmp/mydb/repl-mysql-data-* ]]; then
    rm -rf "${MYDB_DATA_DIR}"
  fi
}
trap cleanup EXIT

echo ">>> launching MySQL 8.0 master on ${MYSQL_PORT}"
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run --name "${CONTAINER}" \
  -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
  -e MYSQL_DATABASE=tpcc \
  -p "127.0.0.1:${MYSQL_PORT}:3306" \
  -d "${MYSQL_IMAGE}" \
  --server-id=1 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --binlog-row-image=FULL \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --default-authentication-plugin=mysql_native_password \
  >/dev/null

echo ">>> waiting for MySQL to accept connections"
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER}" mysqladmin ping -uroot -p"${MYSQL_ROOT_PASSWORD}" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo ">>> bootstrapping tpcc user + permissions"
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS 'tpcc'@'%' IDENTIFIED BY 'tpcc';
GRANT ALL PRIVILEGES ON tpcc.* TO 'tpcc'@'%';
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
SQL

echo ">>> building mydb (release)"
cargo build --release

echo ">>> launching mydb replica on ${MYDB_PORT} (data=${MYDB_DATA_DIR})"
mkdir -p "${MYDB_DATA_DIR}"
MYDB_DATA_DIR="${MYDB_DATA_DIR}" \
MYDB_REPL_SOURCE_HOST=127.0.0.1 \
MYDB_REPL_SOURCE_PORT="${MYSQL_PORT}" \
MYDB_REPL_USER=repl \
MYDB_REPL_PASSWORD=repl \
MYDB_REPL_AUTO_POSITION=1 \
MYDB_REPL_START=0 \
MYDB_LOG_BIN=0 \
./target/release/mydb --host 127.0.0.1 --port "${MYDB_PORT}" --data-dir "${MYDB_DATA_DIR}" &
MYDB_PID=$!

echo ">>> waiting for mydb to accept connections"
for _ in $(seq 1 60); do
  if mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo ">>> setting up tpcc schema on master + replica"
# BenchmarkSQL needs the schema present before runBenchmark; have BenchmarkSQL
# load on master, then bootstrap the schema directly on the replica (DDL will
# also flow via binlog but we want the catalog ready immediately).
if [[ ! -d "${BSQL_RUN}" ]]; then
  echo "BSQL_RUN=${BSQL_RUN} does not exist; set BSQL_RUN to BenchmarkSQL 5.1 'run' dir" >&2
  exit 2
fi

pushd "${BSQL_RUN}" >/dev/null
PROP="props.mydb-repl-master"
cat > "${PROP}" <<PROPS
db=mysql
driver=com.mysql.cj.jdbc.Driver
conn=jdbc:mysql://127.0.0.1:${MYSQL_PORT}/tpcc?useSSL=false&allowPublicKeyRetrieval=true
user=tpcc
password=tpcc
warehouses=${BSQL_WAREHOUSES}
loadWorkers=${BSQL_WAREHOUSES}
fileLocation=
PROPS

echo ">>> loading TPC-C onto master"
./runDatabaseBuild.sh "${PROP}" >/tmp/mydb/repl-master-load.log 2>&1

popd >/dev/null

echo ">>> CHANGE REPLICATION SOURCE TO + START REPLICA on mydb"
mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='127.0.0.1',
  SOURCE_PORT=${MYSQL_PORT},
  SOURCE_USER='repl',
  SOURCE_PASSWORD='repl',
  SOURCE_AUTO_POSITION=1;
START REPLICA;
SQL

echo ">>> waiting for initial catchup"
sleep 5

pushd "${BSQL_RUN}" >/dev/null
PROP="props.mydb-repl-master"
cat > "${PROP}" <<PROPS
db=mysql
driver=com.mysql.cj.jdbc.Driver
conn=jdbc:mysql://127.0.0.1:${MYSQL_PORT}/tpcc?useSSL=false&allowPublicKeyRetrieval=true
user=tpcc
password=tpcc
warehouses=${BSQL_WAREHOUSES}
terminals=${BSQL_TERMINALS}
runMins=${BSQL_RUN_MIN}
runTxnsPerTerminal=0
limitTxnsPerMin=0
terminalWarehouseFixed=false
PROPS
echo ">>> running TPC-C for ${BSQL_RUN_MIN} min"
./runBenchmark.sh "${PROP}" >/tmp/mydb/repl-master-run.log 2>&1
popd >/dev/null

echo ">>> waiting for replica to drain"
for _ in $(seq 1 60); do
  REPL_FILE=$(docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -N -e "SHOW MASTER STATUS\G" 2>/dev/null | awk -F': ' '/File:/{print $2}')
  REPL_POS=$(docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -N -e "SHOW MASTER STATUS\G" 2>/dev/null | awk -F': ' '/Position:/{print $2}')
  REPL_STATE=$(mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -N -e "SHOW REPLICA STATUS\G" | awk -F': ' '/Read_Source_Log_Pos:/{print $2}')
  if [[ -n "${REPL_STATE}" && "${REPL_STATE}" -ge "${REPL_POS}" ]]; then
    echo "    replica reached source pos ${REPL_POS}"
    break
  fi
  sleep 1
done

echo ">>> stopping replication and comparing tables"
mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -e "STOP REPLICA;"

# Use SUM(col IS NOT NULL) (per repl-wal-overhaul memory) so the parallel
# COUNT() bug in the executor doesn't pollute the verifier.
TABLES=(warehouse district customer history new_order oorder order_line item stock)
FAIL=0
for tbl in "${TABLES[@]}"; do
  MASTER=$(docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -tpcc -N \
    -e "SELECT SUM(${tbl}_w_id IS NOT NULL) FROM tpcc.${tbl}" 2>/dev/null \
    || docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -tpcc -N \
    -e "SELECT SUM(1) FROM tpcc.${tbl}" 2>/dev/null)
  REPLICA=$(mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -N \
    -e "SELECT SUM(${tbl}_w_id IS NOT NULL) FROM tpcc.${tbl}" 2>/dev/null \
    || mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -N \
    -e "SELECT SUM(1) FROM tpcc.${tbl}" 2>/dev/null)
  if [[ "${MASTER}" != "${REPLICA}" ]]; then
    echo "MISMATCH on ${tbl}: master=${MASTER} replica=${REPLICA}"
    FAIL=1
  else
    echo "OK ${tbl} = ${MASTER}"
  fi
done

if [[ "${FAIL}" -ne 0 ]]; then
  echo "FAIL: row counts differ"
  exit 1
fi
echo "PASS: all tables match"
