#!/usr/bin/env bash
set -euo pipefail

IMAGE="${MYSQL80_IMAGE:-mysql:8.0}"
CONTAINER="${MYSQL80_CONTAINER:-mydb-mysql80-rollup}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-mydb}"
PORT="${MYSQL80_PORT:-33818}"

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

docker exec "$CONTAINER" mysql -uroot -p"$MYSQL_ROOT_PASSWORD" \
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
SELECT region AS r, product AS p, SUM(amount), GROUPING(r), GROUPING(2)
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
