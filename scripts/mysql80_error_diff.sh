#!/usr/bin/env bash
set -euo pipefail

# Differential oracle for deterministic client-visible conditions.
# Both endpoints must be disposable: the script recreates `error_diff`.

MYSQL_BIN="${MYSQL_BIN:-/opt/homebrew/opt/mysql@8.0/bin/mysql}"
MYSQL80_HOST="${MYSQL80_HOST:-127.0.0.1}"
MYSQL80_PORT="${MYSQL80_PORT:-3308}"
MYSQL80_USER="${MYSQL80_USER:-root}"
MYSQL80_PASSWORD="${MYSQL80_PASSWORD:-}"
MYDB_HOST="${MYDB_HOST:-127.0.0.1}"
MYDB_PORT="${MYDB_PORT:-3307}"
MYDB_USER="${MYDB_USER:-root}"
MYDB_PASSWORD="${MYDB_PASSWORD:-root}"

mysql_for() {
    local endpoint=$1
    shift
    if [[ "$endpoint" == mysql ]]; then
        MYSQL_PWD="$MYSQL80_PASSWORD" "$MYSQL_BIN" \
            --protocol=TCP -h "$MYSQL80_HOST" -P "$MYSQL80_PORT" \
            -u "$MYSQL80_USER" --ssl-mode=DISABLED \
            --batch --raw --skip-column-names "$@"
    else
        MYSQL_PWD="$MYDB_PASSWORD" "$MYSQL_BIN" \
            --protocol=TCP -h "$MYDB_HOST" -P "$MYDB_PORT" \
            -u "$MYDB_USER" --ssl-mode=DISABLED \
            --batch --raw --skip-column-names "$@"
    fi
}

version=$(mysql_for mysql -NBe "SELECT VERSION()")
if [[ "$version" != 8.0.46* ]]; then
    echo "expected MySQL 8.0.46 oracle, got: $version" >&2
    exit 1
fi

for endpoint in mysql mydb; do
    mysql_for "$endpoint" -e \
        "DROP DATABASE IF EXISTS error_diff; CREATE DATABASE error_diff"
done

capture_error() {
    local endpoint=$1
    local sql=$2
    local output
    output=$(mysql_for "$endpoint" --force error_diff -e "$sql" 2>&1 >/dev/null || true)
    printf '%s\n' "$output" \
        | sed -nE 's/ at line [0-9]+: /: /p' \
        | tail -n 1
}

cases=(
    "missing-table|ER_NO_SUCH_TABLE|1146|42S02|SELECT * FROM missing_table"
    "duplicate-table|ER_TABLE_EXISTS_ERROR|1050|42S01|CREATE TABLE dup_t(id INT); CREATE TABLE dup_t(id INT)"
    "duplicate-key|ER_DUP_ENTRY|1062|23000|CREATE TABLE dup_k(id INT PRIMARY KEY); INSERT INTO dup_k VALUES (1); INSERT INTO dup_k VALUES (1)"
    "unknown-column|ER_BAD_FIELD_ERROR|1054|42S22|SELECT missing_column FROM dup_k"
    "not-null|ER_BAD_NULL_ERROR|1048|23000|CREATE TABLE null_t(v INT NOT NULL); INSERT INTO null_t VALUES (NULL)"
    "all-invisible|ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN|4028|HY000|CREATE TABLE visible_t(a INT INVISIBLE)"
    "signal|dynamic-SIGNAL|50000|45A01|DROP PROCEDURE IF EXISTS p_signal; CREATE PROCEDURE p_signal() SIGNAL SQLSTATE '45A01' SET MYSQL_ERRNO=50000, MESSAGE_TEXT='raised by differential'; CALL p_signal()"
)

for test_case in "${cases[@]}"; do
    IFS='|' read -r name symbol errno state sql <<<"$test_case"
    mysql_error=$(capture_error mysql "$sql")
    mydb_error=$(capture_error mydb "$sql")
    if [[ "$mysql_error" != "$mydb_error" ]]; then
        printf '%s (%s) mismatch\nMySQL: %s\nmydb:  %s\n' \
            "$name" "$symbol" "$mysql_error" "$mydb_error" >&2
        exit 1
    fi
    metadata_prefix="ERROR $errno ($state):"
    if [[ "$mysql_error" != "$metadata_prefix"* ]]; then
        printf '%s (%s) returned unexpected metadata: %s\n' \
            "$name" "$symbol" "$mysql_error" >&2
        exit 1
    fi
done

warning_sql="DROP TABLE IF EXISTS warn_t; \
CREATE TABLE warn_t(v TINYINT NOT NULL); \
INSERT IGNORE INTO warn_t VALUES(999); \
SHOW WARNINGS; SELECT @@warning_count"
mysql_warning=$(mysql_for mysql error_diff -e "$warning_sql")
mydb_warning=$(mysql_for mydb error_diff -e "$warning_sql")
if [[ "$mysql_warning" != "$mydb_warning" ]]; then
    printf 'warning diagnostics mismatch\nMySQL:\n%s\nmydb:\n%s\n' \
        "$mysql_warning" "$mydb_warning" >&2
    exit 1
fi

echo "MySQL 8.0.46 error/diagnostic differential checks passed"
