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
            --no-defaults --protocol=TCP -h "$MYSQL80_HOST" -P "$MYSQL80_PORT" \
            -u "$MYSQL80_USER" --ssl-mode=DISABLED \
            --batch --raw --skip-column-names "$@"
    else
        MYSQL_PWD="$MYDB_PASSWORD" "$MYSQL_BIN" \
            --no-defaults --protocol=TCP -h "$MYDB_HOST" -P "$MYDB_PORT" \
            -u "$MYDB_USER" --ssl-mode=REQUIRED \
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
        | sed -nE '/^ERROR / {s/ at line [0-9]+: /: /; p;}' \
        | tail -n 1
}

cases=(
    "syntax|ER_PARSE_ERROR|1064|42000|SELECT 1 +"
    "missing-table|ER_NO_SUCH_TABLE|1146|42S02|SELECT * FROM missing_table"
    "duplicate-table|ER_TABLE_EXISTS_ERROR|1050|42S01|CREATE TABLE dup_t(id INT); CREATE TABLE dup_t(id INT)"
    "duplicate-key|ER_DUP_ENTRY|1062|23000|CREATE TABLE dup_k(id INT PRIMARY KEY); INSERT INTO dup_k VALUES (1); INSERT INTO dup_k VALUES (1)"
    "unknown-column|ER_BAD_FIELD_ERROR|1054|42S22|SELECT missing_column FROM dup_k"
    "unknown-predicate|ER_BAD_FIELD_ERROR|1054|42S22|SELECT id FROM dup_k WHERE missing_column=3"
    "not-null|ER_BAD_NULL_ERROR|1048|23000|CREATE TABLE null_t(v INT NOT NULL); INSERT INTO null_t VALUES (NULL)"
    "overflow|ER_WARN_DATA_OUT_OF_RANGE|1264|22003|CREATE TABLE range_t(v TINYINT); INSERT INTO range_t VALUES(1),(999)"
    "invalid-integer|ER_TRUNCATED_WRONG_VALUE_FOR_FIELD|1366|HY000|INSERT INTO range_t VALUES('invalid')"
    "value-count|ER_WRONG_VALUE_COUNT_ON_ROW|1136|21S01|INSERT INTO range_t VALUES(1),(2,3)"
    "check|ER_CHECK_CONSTRAINT_VIOLATED|3819|HY000|CREATE TABLE check_t(v INT CONSTRAINT positive CHECK (v > 0)); INSERT INTO check_t VALUES(1), (0)"
    "all-invisible|ER_TABLE_MUST_HAVE_A_VISIBLE_COLUMN|4028|HY000|CREATE TABLE visible_t(a INT INVISIBLE)"
    "signal|dynamic-SIGNAL|50000|45A01|DROP PROCEDURE IF EXISTS p_signal; CREATE PROCEDURE p_signal() SIGNAL SQLSTATE '45A01' SET MYSQL_ERRNO=50000, MESSAGE_TEXT='raised by differential'; CALL p_signal()"
)

for test_case in "${cases[@]}"; do
    IFS='|' read -r name symbol errno state sql <<<"$test_case"
    mysql_error=$(capture_error mysql "$sql")
    mydb_error=$(capture_error mydb "$sql")
    metadata_prefix="ERROR $errno ($state):"
    if [[ "$mysql_error" != "$metadata_prefix"* || "$mydb_error" != "$metadata_prefix"* ]]; then
        printf '%s (%s) metadata mismatch\nMySQL: %s\nmydb:  %s\n' \
            "$name" "$symbol" "$mysql_error" "$mydb_error" >&2
        exit 1
    fi
    if ! (
        case "$name" in
        syntax) [[ "$mydb_error" == *"line 1, column 11"* ]];;
        overflow) [[ "$mydb_error" == *"column 'v' at row 2"* && "$mydb_error" == *'value "999"'* ]];;
        invalid-integer) [[ "$mydb_error" == *"column 'v' at row 1"* ]];;
        value-count|check) [[ "$mydb_error" == *"at row 2"* ]];;
        missing-table|duplicate-table|duplicate-key|unknown-column|not-null)
            [[ "$mydb_error" == "$mysql_error"* ]];;
        *) [[ "$mydb_error" == "$mysql_error" ]];;
        esac
    ); then
        printf '%s (%s) client message mismatch\nMySQL: %s\nmydb:  %s\n' \
            "$name" "$symbol" "$mysql_error" "$mydb_error" >&2
        exit 1
    fi
done

warning_sql="CREATE TABLE warn_t(v TINYINT NOT NULL); \
INSERT IGNORE INTO warn_t VALUES(1), (999), (998); \
SHOW WARNINGS; GET DIAGNOSTICS CONDITION 1 @warn_state = RETURNED_SQLSTATE; \
SELECT @@warning_count, @warn_state"
mysql_warning=$(mysql_for mysql error_diff -e "$warning_sql")
mydb_warning=$(mysql_for mydb error_diff -e "$warning_sql")
warning_metadata() {
    awk 'BEGIN {FS=OFS="\t"} /^(Warning|Error|Note)\t/ {print $1, $2; next} {print}'
}
if [[ "$(warning_metadata <<<"$mysql_warning")" != "$(warning_metadata <<<"$mydb_warning")" ]]; then
    printf 'warning diagnostics mismatch\nMySQL:\n%s\nmydb:\n%s\n' "$mysql_warning" "$mydb_warning" >&2
    exit 1
fi
if [[ "$mydb_warning" != *"column 'v' at row 2"* || "$mydb_warning" != *"column 'v' at row 3"* ]]; then
    printf 'warning row numbers mismatch\n%s\n' "$mydb_warning" >&2
    exit 1
fi

echo "MySQL 8.0.46 error/diagnostic differential checks passed"
