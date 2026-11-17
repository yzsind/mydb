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
# Exit code: 0 only after applied progress and complete typed rows match.

set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_docker_test.sh"
cd "$ROOT_DIR"

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

MYSQL_IMAGE="${MYSQL_IMAGE:-mysql:8.0.46}"
MYDB_PORT="${MYDB_PORT:-33309}"
MYSQL_PORT="${MYSQL_PORT:-33310}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
mydb_container_init "mydb-as-master-replica" "${CONTAINER:-}"
ROWS_PER_TABLE="${ROWS_PER_TABLE:-1000}"
mydb_source_network

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

MYDB_BINARY="${MYDB_BINARY:-$ROOT_DIR/target/release/mydb}"
[[ -x "$MYDB_BINARY" ]] || { echo "Build mydb or set MYDB_BINARY to a built executable" >&2; exit 2; }

echo ">>> launching mydb master on ${MYDB_PORT} (data=${MYDB_DATA_DIR}, binlog ON)"
mkdir -p "${MYDB_DATA_DIR}"
MYDB_DATA_DIR="${MYDB_DATA_DIR}" \
MYDB_LOG_BIN=1 \
MYDB_SERVER_ID=1 \
MYDB_BINLOG_CHECKSUM=CRC32 \
MYDB_ROOT_PASSWORD="${MYDB_ROOT_PASSWORD:-root}" "$MYDB_BINARY" --host "${MYDB_BIND_HOST:-127.0.0.1}" --port "${MYDB_PORT}" --data-dir "${MYDB_DATA_DIR}" &
MYDB_PID=$!

echo ">>> waiting for mydb to accept connections"
mydb_wait_native "$MYDB_PORT" "$MYDB_PID"

echo ">>> bootstrapping test schema + repl user"
mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -p"${MYDB_ROOT_PASSWORD:-root}" <<SQL
CREATE USER 'replication_check'@'%' IDENTIFIED WITH mysql_native_password BY 'local-replication-check';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'replication_check'@'%';
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

echo ">>> launching MySQL replica on ${MYSQL_PORT}"
mydb_container_run \
  -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
  "${MYSQL_NETWORK[@]}" \
  -d "${MYSQL_IMAGE}" \
  --server-id=2 \
  --port="$MYSQL_CONTAINER_PORT" \
  --bind-address="$MYSQL_CONTAINER_BIND" \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --gtid-mode=ON \
  --enforce-gtid-consistency=ON \
  --skip-replica-start \
  >/dev/null

echo ">>> waiting for MySQL replica to come up"
mydb_container_wait "${MYSQL_READY_ATTEMPTS:-90}" "$MYSQL_CONTAINER_PORT"

# Docker Desktop routes to host loopback; Linux uses isolated loopback host networking.

echo ">>> pointing MySQL replica at mydb master"
docker exec -i "${CONTAINER}" mysql -uroot -p"${MYSQL_ROOT_PASSWORD}" <<SQL
CHANGE REPLICATION SOURCE TO
  SOURCE_HOST='${MYDB_HOST_FROM_DOCKER}',
  SOURCE_PORT=${MYDB_PORT},
  SOURCE_USER='replication_check',
  SOURCE_PASSWORD='local-replication-check',
  SOURCE_AUTO_POSITION=1;
START REPLICA;
SQL

echo ">>> inserting ${ROWS_PER_TABLE} rows into each table"
SQL_BLOCK=""
for i in $(seq 1 "${ROWS_PER_TABLE}"); do
  SQL_BLOCK+="INSERT INTO appdb.users VALUES (${i}, 'user${i}', 'user${i}@example.com', NOW(3), ${i}.50);"
  SQL_BLOCK+="INSERT INTO appdb.orders VALUES (${i}, ${i}, ${i}.25, NOW(3));"
  if (( i % 100 == 0 )); then
    echo "${SQL_BLOCK}" | mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -p"${MYDB_ROOT_PASSWORD:-root}"
    SQL_BLOCK=""
  fi
done
[[ -n "${SQL_BLOCK}" ]] && echo "${SQL_BLOCK}" | mysql -h127.0.0.1 -P"${MYDB_PORT}" -uroot -p"${MYDB_ROOT_PASSWORD:-root}"

echo ">>> verifying applied progress and complete typed rows"
export MYSQL_ROOT_PASSWORD MYDB_ROOT_PASSWORD="${MYDB_ROOT_PASSWORD:-root}"
python3 "$ROOT_DIR/scripts/replication_check.py" \
  --source-port "${MYDB_PORT}" --source-password-env MYDB_ROOT_PASSWORD \
  --replica-port "${MYSQL_PORT}" --replica-password-env MYSQL_ROOT_PASSWORD \
  --database appdb --timeout "${CATCHUP_TIMEOUT_S:-300}"
