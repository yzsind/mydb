#!/usr/bin/env bash
set -euo pipefail

IMAGE="${MYSQL80_IMAGE:-mysql:8.0}"
CONTAINER="${MYSQL80_CONTAINER:-mydb-mysql80-charset}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"

cleanup() {
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
docker run --name "$CONTAINER" \
  -e MYSQL_ROOT_PASSWORD="$MYSQL_ROOT_PASSWORD" \
  -e MYSQL_DATABASE=test \
  -d "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
  if docker exec "$CONTAINER" mysqladmin ping -uroot -p"$MYSQL_ROOT_PASSWORD" --silent >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

mysql80() {
  docker exec "$CONTAINER" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
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
