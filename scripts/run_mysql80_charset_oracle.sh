#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_docker_test.sh"
cd "$ROOT_DIR"


IMAGE="${MYSQL80_IMAGE:-mysql:8.0.46}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
mydb_container_init "mydb-charset" "${MYSQL80_CONTAINER:-}"

cleanup() {
  mydb_container_cleanup
}
trap cleanup EXIT

mydb_container_run \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
  -e MYSQL_DATABASE=test \
  -d "$IMAGE" >/dev/null

mydb_container_wait "${MYSQL_READY_ATTEMPTS:-90}"

mysql80() {
  docker exec "$CONTAINER" mysql --default-character-set=utf8mb4 -uroot -p"$MYSQL_ROOT_PASSWORD" \
    --batch --raw --skip-column-names test -e "$1"
}

echo "mysql80> SHOW CHARACTER SET"
mysql80 "SHOW CHARACTER SET"

echo "mysql80> SHOW COLLATION"
mysql80 "SHOW COLLATION"

echo "mysql80> representative comparisons"
mysql80 "SELECT \
  'a' COLLATE utf8mb4_0900_ai_ci = 'A', \
  'e' COLLATE utf8mb4_0900_ai_ci = 'é', \
  'a' COLLATE utf8mb4_0900_ai_ci = 'a ', \
  'a' COLLATE utf8mb4_0900_as_cs = 'A', \
  HEX(WEIGHT_STRING('AbCd' COLLATE utf8mb4_0900_ai_ci))"

echo "mysql80> representative charset DDL"
mysql80 "DROP TABLE IF EXISTS charset_oracle; \
  CREATE TABLE charset_oracle ( \
    id INT PRIMARY KEY, \
    c_gbk VARCHAR(20) CHARACTER SET gbk, \
    c_cp932 VARCHAR(20) CHARACTER SET cp932, \
    c_latin1 VARCHAR(20) CHARACTER SET latin1 \
  ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; \
  SHOW CREATE TABLE charset_oracle"
