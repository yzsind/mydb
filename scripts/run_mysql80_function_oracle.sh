#!/usr/bin/env bash
set -euo pipefail

IMAGE="${MYSQL80_IMAGE:-mysql:8.0}"
CONTAINER="${MYSQL80_CONTAINER:-mydb-mysql80-functions}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
PORT="${MYSQL80_PORT:-33808}"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
docker run --name "$CONTAINER" \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
  -e MYSQL_DATABASE=test \
  -p "127.0.0.1:${PORT}:3306" \
  -d "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" mysqladmin ping -uroot -p"$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

queries=(
  "SELECT TO_BASE64('hello'), EXPORT_SET(5,'Y','N','/',4), BIT_COUNT(15), 7 DIV 3"
  "SELECT JSON_CONTAINS_PATH('{\"a\":[1],\"b\":2}','all','$.a','$.b'), JSON_DEPTH('{\"a\":[1,{\"b\":2}]}')"
  "SELECT BIN_TO_UUID(UUID_TO_BIN('6ccd780c-baba-1026-9564-5b8c656024db', 1), 1), INET_NTOA(INET_ATON('127.0.0.1'))"
  "SELECT UNCOMPRESSED_LENGTH(COMPRESS('hello')), AES_DECRYPT(AES_ENCRYPT('hello','k'),'k')"
)

for sql in "${queries[@]}"; do
  echo "mysql80> $sql"
  docker exec "$CONTAINER" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" --batch --raw --skip-column-names test -e "$sql"
done
