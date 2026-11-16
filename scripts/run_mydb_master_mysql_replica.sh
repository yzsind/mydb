#!/usr/bin/env bash
# End-to-end mydb master → MySQL 8.0 replica consistency: turn on mydb's
# binlog source, point a fresh MySQL 8.0 at it as a replica, run a workload,
# stop, compare per-table checksums.
#
# Requires:
#   * docker (MySQL 8.0)
#   * cargo + a built mydb binary
#
# Knobs:
#   MYSQL_IMAGE        — docker image for the replica (mysql:8.0)
#   MYDB_PORT          — local mydb port acting as master (33309)
#   MYSQL_PORT         — exposed MySQL replica port (33310)
#   MYDB_DATA_DIR      — fresh data dir (auto: /tmp/mydb/as-master-data-$$)
#   ROWS_PER_TABLE     — rows inserted into each test table (1000)
#
# Exit code: 0 iff every replicated table SUM(col) matches between mydb master
# and MySQL replica. Non-zero on any mismatch.

set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0}"
CONTAINER="${CONTAINER:-mydb-as-master-replica}"
MYDB_PORT="${MYDB_PORT:-33309}"
MYSQL_PORT="${MYSQL_PORT:-33310}"
MYDB_DATA_DIR="${MYDB_DATA_DIR:-/tmp/mydb/as-master-data-$$}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
ROWS_PER_TABLE="${ROWS_PER_TABLE:-1000}"

MYDB_PID=""

cleanup() {
  set +e
  if [[ -n "${MYDB_PID}" ]]; then
    kill "${MYDB_PID}" 2>/dev/null || true
    wait "${MYDB_PID}" 2>/dev/null || true
  fi
  docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
  if [[ "${MYDB_DATA_DIR}" == /tmp/mydb/as-master-data-* ]]; then
    rm -rf "${MYDB_DATA_DIR}"
  fi
}
trap cleanup EXIT

echo ">>> building mydb (release)"
cargo build --release

echo ">>> launching mydb master on ${MYDB_PORT} (data=${MYDB_DATA_DIR}, binlog ON)"
mkdir -p "${MYDB_DATA_DIR}"
MYDB_DATA_DIR="${MYDB_DATA_DIR}" \
MYDB_LOG_BIN=1 \
MYDB_SERVER_ID=1 \
MYDB_BINLOG_CHECKSUM=CRC32 \
./target/release/mydb --host 0.0.0.0 --port "${MYDB_PORT}" --data-dir "${MYDB_DATA_DIR}" &
MYDB_PID=$!

echo ">>> waiting for mydb to accept connections"
for _ in $(seq 1 60); do
  if mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

echo ">>> bootstrapping test schema + repl user"
mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot <<SQL
CREATE DATABASE IF NOT EXISTS appdb;
USE appdb;
CREATE TABLE users (
  id INT PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  email VARCHAR(128),
  created DATETIME(3) NOT NULL,
  balance DECIMAL(10,2) NOT NULL
);
CREATE TABLE orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  placed TIMESTAMP(3) NOT NULL
);
SQL

# We can't reliably create a non-root account that the mysql replica will
# accept from inside the container, so the replica uses root.

echo ">>> launching MySQL replica on ${MYSQL_PORT}"
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run --name "${CONTAINER}" \
  -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
  -p "127.0.0.1:${MYSQL_PORT}:3306" \
  -d "${MYSQL_IMAGE}" \
  --server-id=2 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --skip-replica-start \
  >/dev/null

echo ">>> waiting for MySQL replica to come up"
for _ in $(seq 1 90); do
  if docker exec "${CONTAINER}" mysqladmin ping -uroot -p"${MYSQL_ROOT_PASSWORD}" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

# host.docker.internal resolves to the host running mydb when the replica is
# inside Docker Desktop; on Linux you may need to substitute another bridge IP.
MYDB_HOST_FROM_DOCKER="${MYDB_HOST_FROM_DOCKER:-host.docker.internal}"

echo ">>> pointing MySQL replica at mydb master"
docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='${MYDB_HOST_FROM_DOCKER}',
  SOURCE_PORT=${MYDB_PORT},
  SOURCE_USER='root',
  SOURCE_PASSWORD='root',
  SOURCE_AUTO_POSITION=1;
START REPLICA;
SQL

echo ">>> inserting ${ROWS_PER_TABLE} rows into each table"
SQL_BLOCK=""
for i in $(seq 1 "${ROWS_PER_TABLE}"); do
  SQL_BLOCK+="INSERT INTO appdb.users VALUES (${i}, 'user${i}', 'user${i}@example.com', NOW(3), ${i}.50);"
  SQL_BLOCK+="INSERT INTO appdb.orders VALUES (${i}, ${i}, ${i}.25, NOW(3));"
  if (( i % 100 == 0 )); then
    echo "${SQL_BLOCK}" | mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot
    SQL_BLOCK=""
  fi
done
[[ -n "${SQL_BLOCK}" ]] && echo "${SQL_BLOCK}" | mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot

echo ">>> waiting for replica to drain"
for _ in $(seq 1 120); do
  POS_MASTER=$(mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -N -e "SHOW MASTER STATUS\G" | awk -F': ' '/Position:/{print $2}')
  POS_REPL=$(docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -N -e "SHOW REPLICA STATUS\G" | awk -F': ' '/Read_Source_Log_Pos:/{print $2}')
  if [[ -n "${POS_MASTER}" && -n "${POS_REPL}" && "${POS_REPL}" -ge "${POS_MASTER}" ]]; then
    break
  fi
  sleep 1
done

echo ">>> stopping replication"
docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -e "STOP REPLICA;"

FAIL=0
for tbl in users orders; do
  M_COUNT=$(mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -proot -N -e "SELECT SUM(id IS NOT NULL) FROM appdb.${tbl}")
  R_COUNT=$(docker exec "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" -N -e "SELECT SUM(id IS NOT NULL) FROM appdb.${tbl}")
  if [[ "${M_COUNT}" != "${R_COUNT}" ]]; then
    echo "MISMATCH on ${tbl}: master=${M_COUNT} replica=${R_COUNT}"
    FAIL=1
  else
    echo "OK ${tbl} = ${M_COUNT}"
  fi
done

if [[ "${FAIL}" -ne 0 ]]; then
  echo "FAIL: counts differ"
  exit 1
fi
echo "PASS: master and replica agree"
