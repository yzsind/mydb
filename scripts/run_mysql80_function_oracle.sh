#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_docker_test.sh"
cd "$ROOT_DIR"


IMAGE="${MYSQL80_IMAGE:-mysql:8.0.46}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
mydb_container_init "mydb-functions" "${MYSQL80_CONTAINER:-}"
PORT="${MYSQL80_PORT:-33808}"

cleanup() {
  mydb_container_cleanup
}
trap cleanup EXIT

mydb_container_run \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
  -e MYSQL_DATABASE=test \
  -p "127.0.0.1:${PORT}:3306" \
  -d "$IMAGE" >/dev/null

mydb_container_wait "${MYSQL_READY_ATTEMPTS:-90}"

queries=(
  "SELECT TO_BASE64('hello'), EXPORT_SET(5,'Y','N','/',4), BIT_COUNT(15), 7 DIV 3"
  "SELECT JSON_CONTAINS_PATH('{\"a\":[1],\"b\":2}','all','$.a','$.b'), JSON_DEPTH('{\"a\":[1,{\"b\":2}]}')"
  "SELECT BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db', 1), 1), INET_NTOA(INET_ATON('127.0.0.1'))"
  "SELECT UNCOMPRESSED_LENGTH(COMPRESS('hello')), AES_DECRYPT(AES_ENCRYPT('hello','k'),'k')"
)

for sql in "${queries[@]}"; do
  echo "mysql80> $sql"
  docker exec "$CONTAINER" mysql --default-character-set=utf8mb4 -uroot -p"$MYSQL_ROOT_PASSWORD" --batch --raw --skip-column-names test -e "$sql"
done
