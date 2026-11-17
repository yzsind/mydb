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
# Exit code: 0 only after applied progress and complete typed rows match.

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_docker_test.sh"
cd "$ROOT_DIR"

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.46}"
MYSQL_PORT="${MYSQL_PORT:-33307}"
MYDB_PORT="${MYDB_PORT:-33308}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
mydb_container_init "mydb-repl-master" "${CONTAINER:-}"
BSQL_WAREHOUSES="${BSQL_WAREHOUSES:-2}"
BSQL_TERMINALS="${BSQL_TERMINALS:-4}"
BSQL_RUN_MIN="${BSQL_RUN_MIN:-1}"
BSQL_RUN="${BSQL_RUN:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/benchmarksql5.1/run}"

MYDB_PID=""

cleanup() {
  set +e
  if [[ -n "${MYDB_PID}" ]]; then
    mydb_stop_native "$MYDB_PID"
  fi
  mydb_container_cleanup
  mydb_test_data_cleanup
}
trap cleanup EXIT
mydb_test_data_init
mydb_assert_ports_free "$MYDB_PORT" "$MYSQL_PORT"

echo ">>> launching MySQL 8.0 master on ${MYSQL_PORT}"
mydb_container_run \
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
mydb_container_wait "${MYSQL_READY_ATTEMPTS:-90}"

echo ">>> bootstrapping tpcc user + permissions"
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CREATE USER IF NOT EXISTS 'tpcc'@'%' IDENTIFIED BY 'tpcc';
GRANT ALL PRIVILEGES ON tpcc.* TO 'tpcc'@'%';
CREATE USER IF NOT EXISTS 'repl'@'%' IDENTIFIED BY 'repl';
GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%';
FLUSH PRIVILEGES;
SQL

MYDB_BINARY="${MYDB_BINARY:-$ROOT_DIR/target/release/mydb}"
[[ -x "$MYDB_BINARY" ]] || { echo "Build mydb or set MYDB_BINARY to a built executable" >&2; exit 2; }

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
MYDB_ROOT_PASSWORD="${MYDB_ROOT_PASSWORD:-root}" "$MYDB_BINARY" --host 127.0.0.1 --port "${MYDB_PORT}" --data-dir "${MYDB_DATA_DIR}" &
MYDB_PID=$!

echo ">>> waiting for mydb to accept connections"
mydb_wait_native "$MYDB_PORT" "$MYDB_PID"

echo ">>> setting up tpcc schema on master + replica"
# BenchmarkSQL needs the schema present before runBenchmark; have BenchmarkSQL
# load on master, then bootstrap the schema directly on the replica (DDL will
# also flow via binlog but we want the catalog ready immediately).
if [[ ! -d "${BSQL_RUN}" ]]; then
  echo "BSQL_RUN=${BSQL_RUN} does not exist; set BSQL_RUN to BenchmarkSQL 5.1 'run' dir" >&2
  exit 2
fi

pushd "${BSQL_RUN}" >/dev/null
PROP="${MYDB_DATA_DIR}/benchmarksql.properties"
cat > "${PROP}" <<PROPS
db=mysql
driver=com.mysql.cj.jdbc.Driver
conn=jdbc:mysql://127.0.0.1:${MYSQL_PORT}/tpcc
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
mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -p"${MYDB_ROOT_PASSWORD:-root}" <<SQL
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
PROP="${MYDB_DATA_DIR}/benchmarksql.properties"
cat > "${PROP}" <<PROPS
db=mysql
driver=com.mysql.cj.jdbc.Driver
conn=jdbc:mysql://127.0.0.1:${MYSQL_PORT}/tpcc
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

echo ">>> verifying applied progress and complete typed rows"
export MYSQL_ROOT_PASSWORD MYDB_ROOT_PASSWORD="${MYDB_ROOT_PASSWORD:-root}"
python3 "$ROOT_DIR/scripts/replication_check.py" \
  --source-port "${MYSQL_PORT}" --source-password-env MYSQL_ROOT_PASSWORD \
  --replica-port "${MYDB_PORT}" --replica-password-env MYDB_ROOT_PASSWORD \
  --database tpcc --timeout "${CATCHUP_TIMEOUT_S:-300}"
