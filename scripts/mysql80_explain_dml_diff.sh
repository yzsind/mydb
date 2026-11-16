#!/usr/bin/env bash
set -euo pipefail

# Static EXPLAIN compatibility oracle. The MySQL endpoint must report exactly
# 8.0.46; mydb defaults to the local development port from AGENTS.md.
MYSQL80_HOST="${MYSQL80_HOST:-127.0.0.1}"
MYSQL80_PORT="${MYSQL80_PORT:-3308}"
MYSQL80_USER="${MYSQL80_USER:-root}"
MYSQL80_PASSWORD="${MYSQL80_PASSWORD:-root}"
MYDB_HOST="${MYDB_HOST:-127.0.0.1}"
MYDB_PORT="${MYDB_PORT:-3307}"
MYDB_USER="${MYDB_USER:-root}"
MYDB_PASSWORD="${MYDB_PASSWORD:-root}"

mysql80=(mysql --protocol=TCP -h "$MYSQL80_HOST" -P "$MYSQL80_PORT" -u "$MYSQL80_USER" --ssl-mode=DISABLED)
mydb=(mysql --protocol=TCP -h "$MYDB_HOST" -P "$MYDB_PORT" -u "$MYDB_USER" --ssl-mode=DISABLED)

mysql80_query() {
    MYSQL_PWD="$MYSQL80_PASSWORD" "${mysql80[@]}" --batch --raw "$@"
}

mydb_query() {
    MYSQL_PWD="$MYDB_PASSWORD" "${mydb[@]}" --batch --raw "$@"
}

version="$(mysql80_query --skip-column-names -e 'SELECT VERSION()')"
if [[ "$version" != 8.0.46* ]]; then
    echo "expected MySQL 8.0.46 oracle, got: $version" >&2
    exit 1
fi

setup_sql="
DROP DATABASE IF EXISTS mydb_explain_diff;
CREATE DATABASE mydb_explain_diff;
USE mydb_explain_diff;
CREATE TABLE src (id INT PRIMARY KEY, v INT, KEY idx_v(v));
CREATE TABLE dst (id INT PRIMARY KEY, v INT)
  PARTITION BY HASH(id) PARTITIONS 4;
INSERT INTO src VALUES (1,10),(2,20),(3,30);
"
mysql80_query -e "$setup_sql"
mydb_query -e "$setup_sql"

queries=(
    "EXPLAIN INSERT INTO dst VALUES (4,40)"
    "EXPLAIN REPLACE INTO dst VALUES (4,40)"
    "EXPLAIN INSERT INTO dst SELECT id,v FROM src WHERE id=1"
    "EXPLAIN UPDATE src SET v=11 WHERE id=1"
    "EXPLAIN DELETE FROM src WHERE id=1"
)

expected_header=$'id\tselect_type\ttable\tpartitions\ttype\tpossible_keys\tkey\tkey_len\tref\trows\tfiltered\tExtra'
for sql in "${queries[@]}"; do
    mysql_out="$(mysql80_query mydb_explain_diff -e "$sql")"
    mydb_out="$(mydb_query mydb_explain_diff -e "$sql")"
    mysql_header="${mysql_out%%$'\n'*}"
    mydb_header="${mydb_out%%$'\n'*}"
    [[ "$mysql_header" == "$expected_header" ]] || {
        echo "unexpected MySQL metadata for: $sql" >&2
        exit 1
    }
    [[ "$mydb_header" == "$mysql_header" ]] || {
        echo "metadata mismatch for: $sql" >&2
        diff -u <(printf '%s\n' "$mysql_header") <(printf '%s\n' "$mydb_header") || true
        exit 1
    }
done

# Static EXPLAIN must leave target data unchanged on both servers.
mysql_count="$(mysql80_query --skip-column-names mydb_explain_diff -e 'SELECT COUNT(*) FROM dst')"
mydb_count="$(mydb_query --skip-column-names mydb_explain_diff -e 'SELECT COUNT(*) FROM dst')"
[[ "$mysql_count" == 0 && "$mydb_count" == 0 ]] || {
    echo "static EXPLAIN changed data (mysql=$mysql_count mydb=$mydb_count)" >&2
    exit 1
}

# JSON is compared structurally rather than by optimizer-specific cost/row
# estimates. jq exits non-zero for malformed output.
mydb_json="$(mydb_query --skip-column-names mydb_explain_diff \
    -e 'EXPLAIN FORMAT=JSON UPDATE src SET v=11 WHERE id=1')"
printf '%s\n' "$mydb_json" | jq -e '.mutation.operation == "update" and (.mutation.input | type == "object")' >/dev/null

echo "MySQL 8.0.46 DML EXPLAIN differential checks passed"
