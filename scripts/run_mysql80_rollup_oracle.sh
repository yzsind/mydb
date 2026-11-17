#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT_DIR/scripts/_docker_test.sh"
cd "$ROOT_DIR"


IMAGE="${MYSQL80_IMAGE:-mysql:8.0.46}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
mydb_container_init "mydb-rollup" "${MYSQL80_CONTAINER:-}"
PORT="${MYSQL80_PORT:-33818}"

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

docker exec -i "$CONTAINER" mysql --default-character-set=utf8mb4 -uroot -p"$MYSQL_ROOT_PASSWORD" \
  --batch --raw --skip-column-names test <<'SQL'
CREATE TABLE sales(region varchar(20), product varchar(20), amount int);
INSERT INTO sales VALUES
  ('east','book',10),
  ('east','pen',5),
  ('west','book',7),
  ('west',NULL,3),
  (NULL,'book',4);

SELECT 'multi-column rollup';
SELECT region, product, COUNT(*), SUM(amount),
       GROUPING(region), GROUPING(product), GROUPING(region, product)
FROM sales
GROUP BY region, product WITH ROLLUP;

SELECT 'alias and ordinal';
# GROUPING arguments must name grouping expressions, not aliases/ordinals.
SELECT region AS r, product AS p, SUM(amount), GROUPING(region), GROUPING(product)
FROM sales
GROUP BY r, 2 WITH ROLLUP;

SELECT 'having order limit';
SELECT region, product, SUM(amount), GROUPING(product)
FROM sales
GROUP BY region, product WITH ROLLUP
HAVING GROUPING(product) = 1
ORDER BY GROUPING(region), region
LIMIT 3;

SELECT 'grouped empty input';
CREATE TABLE empty_sales(region int, amount int);
SELECT region, COUNT(*), SUM(amount), GROUPING(region)
FROM empty_sales
GROUP BY region WITH ROLLUP;
SQL
