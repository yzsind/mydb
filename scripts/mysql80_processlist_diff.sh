#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

# Compare observable SHOW PROCESSLIST states between a MySQL 8.0 reference
# and mydb. IDs, client ports and TIME are intentionally normalized.
# Both servers must already be running and accept the credentials below.

MYSQL_BIN="${MYSQL_BIN:-mysql}"
MYSQL80_HOST="${MYSQL80_HOST:-127.0.0.1}"
MYSQL80_PORT="${MYSQL80_PORT:-3308}"
MYSQL80_USER="${MYSQL80_USER:-root}"
MYSQL80_PASSWORD="${MYSQL80_PASSWORD:-root}"
MYDB_HOST="${MYDB_HOST:-127.0.0.1}"
MYDB_PORT="${MYDB_PORT:-3307}"
MYDB_USER="${MYDB_USER:-root}"
MYDB_PASSWORD="${MYDB_PASSWORD:-root}"

tmp_dir="$(mktemp -d "/tmp/mydb/mydb-processlist-diff.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

build_prepared_probe() {
    local mysql_config_bin="${MYSQL_CONFIG:-mysql_config}"
    command -v cc >/dev/null || {
        echo "cc is required for the protocol prepared-statement probe" >&2
        return 1
    }
    cat >"$tmp_dir/prepared_probe.c" <<'EOF'
#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(int argc, char **argv) {
    static const char sql[] = "SELECT SLEEP(4) /* processlist_diff_prepared */";
    if (argc != 5) return 2;
    MYSQL *mysql = mysql_init(NULL);
    if (mysql == NULL) return 3;
    if (mysql_real_connect(mysql, argv[1], argv[3], argv[4], "processlist_diff",
                           (unsigned int)strtoul(argv[2], NULL, 10), NULL, 0) == NULL) {
        fprintf(stderr, "%s\n", mysql_error(mysql));
        mysql_close(mysql);
        return 4;
    }
    MYSQL_STMT *stmt = mysql_stmt_init(mysql);
    if (stmt == NULL || mysql_stmt_prepare(stmt, sql, sizeof(sql) - 1) != 0 ||
        mysql_stmt_execute(stmt) != 0) {
        fprintf(stderr, "%s\n", stmt == NULL ? mysql_error(mysql) : mysql_stmt_error(stmt));
        if (stmt != NULL) mysql_stmt_close(stmt);
        mysql_close(mysql);
        return 5;
    }
    mysql_stmt_free_result(stmt);
    mysql_stmt_close(stmt);
    mysql_close(mysql);
    return 0;
}
EOF
    if command -v pkg-config >/dev/null && pkg-config --exists mysqlclient; then
        # pkg-config intentionally emits a shell-splittable flag list.
        # shellcheck disable=SC2046
        cc -O2 $(pkg-config --cflags mysqlclient) "$tmp_dir/prepared_probe.c" \
            $(pkg-config --libs mysqlclient) -o "$tmp_dir/prepared_probe"
    elif command -v "$mysql_config_bin" >/dev/null; then
        # mysql_config intentionally emits a shell-splittable flag list.
        # shellcheck disable=SC2046
        cc -O2 $("$mysql_config_bin" --cflags) "$tmp_dir/prepared_probe.c" \
            $("$mysql_config_bin" --libs) -o "$tmp_dir/prepared_probe"
    else
        echo "pkg-config mysqlclient or $mysql_config_bin is required for the protocol prepared-statement probe" >&2
        return 1
    fi
}

query() {
    local endpoint="$1"
    shift
    if [[ "$endpoint" == mysql80 ]]; then
        MYSQL_PWD="$MYSQL80_PASSWORD" "$MYSQL_BIN" --batch --raw --skip-column-names \
            --protocol=TCP -h "$MYSQL80_HOST" -P "$MYSQL80_PORT" -u "$MYSQL80_USER" \
            --ssl-mode=DISABLED "$@"
    else
        MYSQL_PWD="$MYDB_PASSWORD" "$MYSQL_BIN" --batch --raw --skip-column-names \
            --protocol=TCP -h "$MYDB_HOST" -P "$MYDB_PORT" -u "$MYDB_USER" \
            --ssl-mode=DISABLED "$@"
    fi
}

normalize() {
    sed -E \
        -e 's/localhost:[0-9]+/localhost:<port>/g' \
        -e 's/127\.0\.0\.1:[0-9]+/127.0.0.1:<port>/g' \
        -e 's/\[[^]]+\]:[0-9]+/[ipv6]:<port>/g' \
        -e 's/^[0-9]+\t/<id>\t/' \
        -e 's/\t[0-9]+\t/\t<time>\t/' \
        -e $'s/\tSleep\t<time>\t\t/\tSleep\t<time>\t<NULL>\t/' \
        | LC_ALL=C sort
}

snapshot() {
    local endpoint="$1"
    query "$endpoint" processlist_diff -e \
        "SELECT ID,USER,HOST,IFNULL(DB,'<NULL>'),COMMAND,TIME,IFNULL(STATE,'<NULL>'),IFNULL(INFO,'<NULL>') FROM information_schema.PROCESSLIST" \
        | normalize
}

show_snapshot() {
    local endpoint="$1"
    query "$endpoint" -e "SHOW FULL PROCESSLIST" | normalize
}

wait_for_state() {
    local endpoint="$1"
    local pattern="$2"
    local deadline=$((SECONDS + 10))
    while (( SECONDS < deadline )); do
        if snapshot "$endpoint" | grep -Fq "$pattern"; then
            return 0
        fi
        sleep 0.05
    done
    echo "timed out waiting for state '$pattern' on $endpoint" >&2
    snapshot "$endpoint" >&2
    return 1
}

setup_endpoint() {
    local endpoint="$1"
    if [[ "$endpoint" == mysql80 ]]; then
        # MySQL 8.0's legacy process-list backend reports `init` for the
        # current SHOW and uses a different event-daemon idle label. Exercise
        # the 8.0.26+ Performance Schema implementation used by this
        # compatibility baseline and ensure the daemon has an active queue.
        query "$endpoint" -e \
            "SET GLOBAL performance_schema_show_processlist=ON; SET GLOBAL event_scheduler=ON"
    fi
    query "$endpoint" -e \
        "CREATE DATABASE IF NOT EXISTS processlist_diff; CREATE TABLE IF NOT EXISTS processlist_diff.t(id INT PRIMARY KEY,v INT); INSERT INTO processlist_diff.t VALUES(1,0) ON DUPLICATE KEY UPDATE v=VALUES(v); CREATE EVENT IF NOT EXISTS processlist_diff.processlist_future_event ON SCHEDULE EVERY 1 HOUR DO SELECT 1"
}

capture_endpoint() {
    local endpoint="$1"
    local output="$tmp_dir/$endpoint.txt"
    setup_endpoint "$endpoint"

    local idle_fifo="$tmp_dir/$endpoint-idle.fifo"
    mkfifo "$idle_fifo"
    exec 9<>"$idle_fifo"
    if [[ "$endpoint" == mysql80 ]]; then
        MYSQL_PWD="$MYSQL80_PASSWORD" "$MYSQL_BIN" --batch --raw --protocol=TCP \
            -h "$MYSQL80_HOST" -P "$MYSQL80_PORT" -u "$MYSQL80_USER" \
            --ssl-mode=DISABLED <"$idle_fifo" >"$tmp_dir/$endpoint-idle.out" 2>&1 &
    else
        MYSQL_PWD="$MYDB_PASSWORD" "$MYSQL_BIN" --batch --raw --protocol=TCP \
            -h "$MYDB_HOST" -P "$MYDB_PORT" -u "$MYDB_USER" \
            --ssl-mode=DISABLED <"$idle_fifo" >"$tmp_dir/$endpoint-idle.out" 2>&1 &
    fi
    local idle_pid=$!
    wait_for_state "$endpoint" $'\tSleep\t'
    echo '[idle]' >>"$output"
    snapshot "$endpoint" | grep $'\tSleep\t' >>"$output"
    kill -TERM "$idle_pid"
    wait "$idle_pid" 2>/dev/null || true
    exec 9>&-

    echo '[current-query]' >>"$output"
    show_snapshot "$endpoint" | grep 'SHOW FULL PROCESSLIST' >>"$output"

    query "$endpoint" processlist_diff -e "SELECT SLEEP(4) /* processlist_diff_sleep */" \
        >"$tmp_dir/$endpoint-sleep.out" 2>&1 &
    local sleep_pid=$!
    wait_for_state "$endpoint" $'User sleep'
    echo '[sleep]' >>"$output"
    snapshot "$endpoint" | grep -E 'processlist_diff_sleep|User sleep' >>"$output"
    wait "$sleep_pid"

    query "$endpoint" processlist_diff -e \
        "START TRANSACTION; UPDATE t SET v=v+1 WHERE id=1; SELECT SLEEP(4); COMMIT" \
        >"$tmp_dir/$endpoint-holder.out" 2>&1 &
    local holder_pid=$!
    wait_for_state "$endpoint" 'User sleep'
    query "$endpoint" processlist_diff -e \
        "UPDATE t SET v=v+1 WHERE id=1 /* processlist_diff_row_lock */" \
        >"$tmp_dir/$endpoint-row-wait.out" 2>&1 &
    local row_pid=$!
    wait_for_state "$endpoint" $'updating'
    echo '[row-lock]' >>"$output"
    snapshot "$endpoint" | grep -E 'processlist_diff_row_lock|updating' >>"$output"
    wait "$holder_pid"
    wait "$row_pid"

    query "$endpoint" processlist_diff -e \
        "START TRANSACTION; SELECT * FROM t; SELECT SLEEP(4); COMMIT" \
        >"$tmp_dir/$endpoint-mdl-holder.out" 2>&1 &
    local mdl_holder_pid=$!
    wait_for_state "$endpoint" 'User sleep'
    query "$endpoint" processlist_diff -e \
        "ALTER TABLE t ADD COLUMN processlist_diff_col INT NULL" \
        >"$tmp_dir/$endpoint-mdl-wait.out" 2>&1 &
    local mdl_pid=$!
    wait_for_state "$endpoint" 'Waiting for table metadata lock'
    echo '[mdl]' >>"$output"
    snapshot "$endpoint" | grep -E 'ALTER TABLE|Waiting for table metadata lock' >>"$output"
    wait "$mdl_holder_pid"
    wait "$mdl_pid"
    query "$endpoint" processlist_diff -e \
        "ALTER TABLE t DROP COLUMN processlist_diff_col" >/dev/null

    if [[ "$endpoint" == mysql80 ]]; then
        "$tmp_dir/prepared_probe" "$MYSQL80_HOST" "$MYSQL80_PORT" \
            "$MYSQL80_USER" "$MYSQL80_PASSWORD" >"$tmp_dir/$endpoint-prepared.out" 2>&1 &
    else
        "$tmp_dir/prepared_probe" "$MYDB_HOST" "$MYDB_PORT" \
            "$MYDB_USER" "$MYDB_PASSWORD" >"$tmp_dir/$endpoint-prepared.out" 2>&1 &
    fi
    local prepared_pid=$!
    if ! wait_for_state "$endpoint" 'processlist_diff_prepared'; then
        wait "$prepared_pid" 2>/dev/null || true
        cat "$tmp_dir/$endpoint-prepared.out" >&2
        return 1
    fi
    echo '[prepared-statement]' >>"$output"
    snapshot "$endpoint" | grep 'processlist_diff_prepared' >>"$output"
    wait "$prepared_pid"

    echo '[event-scheduler]' >>"$output"
    snapshot "$endpoint" | grep -E 'event_scheduler|Waiting for next activation' >>"$output" || true

    echo '[replication]' >>"$output"
    snapshot "$endpoint" \
        | grep -Ei $'\tsystem user\t|\tBinlog Dump\t|source event|replica|relay log|binlog event' \
        >>"$output" || true
}

build_prepared_probe
capture_endpoint mysql80
capture_endpoint mydb

diff -u "$tmp_dir/mysql80.txt" "$tmp_dir/mydb.txt"
echo "MySQL 8.0 and mydb normalized PROCESSLIST captures match"
