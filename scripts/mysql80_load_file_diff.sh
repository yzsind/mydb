#!/usr/bin/env bash
set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

# Local black-box differential for LOAD_FILE() against the repository's fixed
# MySQL 8.0.46 oracle. No remote server is contacted.
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mysql_prefix="${MYSQL80_PREFIX:-/opt/homebrew/opt/mysql@8.0}"
mysql_bin="${MYSQL_BIN:-$mysql_prefix/bin/mysql}"
mysqld_bin="${MYSQLD_BIN:-$mysql_prefix/bin/mysqld}"
mydb_bin="${MYDB_BIN:-$repo_dir/target/debug/mydb}"
mysql_port="${MYSQL_PORT:-34180}"
mydb_port="${MYDB_PORT:-34179}"
work_dir="$(mktemp -d /tmp/mydb/load-file-diff.XXXXXX)"

mysql_pid=""
mydb_pid=""
cleanup() {
    if [[ -n "$mydb_pid" ]] && kill -0 "$mydb_pid" 2>/dev/null; then
        kill "$mydb_pid" 2>/dev/null || true
        wait "$mydb_pid" 2>/dev/null || true
    fi
    if [[ -n "$mysql_pid" ]] && kill -0 "$mysql_pid" 2>/dev/null; then
        kill "$mysql_pid" 2>/dev/null || true
        wait "$mysql_pid" 2>/dev/null || true
    fi
    if [[ "${KEEP_LOAD_FILE_DIFF_DATA:-0}" != 1 ]]; then
        rm -rf "$work_dir"
    else
        printf 'LOAD_FILE differential data retained at %s\n' "$work_dir"
    fi
}
trap cleanup EXIT INT TERM

for executable in "$mysql_bin" "$mysqld_bin" "$mydb_bin"; do
    if [[ ! -x "$executable" ]]; then
        printf 'missing executable: %s\n' "$executable" >&2
        exit 1
    fi
done

oracle_version="$("$mysqld_bin" --version)"
if [[ "$oracle_version" != *"Ver 8.0.46"* ]]; then
    printf 'LOAD_FILE oracle must be MySQL 8.0.46, got: %s\n' "$oracle_version" >&2
    exit 1
fi

mkdir -p "$work_dir/mysql-data" "$work_dir/mydb-data" \
    "$work_dir/files/allowed" "$work_dir/files/outside"
printf 'a\0b\377\n' >"$work_dir/files/allowed/binary.bin"
: >"$work_dir/files/allowed/empty.bin"
printf 'outside' >"$work_dir/files/outside/outside.bin"
dd if=/dev/zero of="$work_dir/files/allowed/exact.bin" bs=1024 count=1 status=none
dd if=/dev/zero of="$work_dir/files/allowed/over.bin" bs=1025 count=1 status=none
chmod -R a+rX "$work_dir/files"

"$mysqld_bin" --no-defaults --initialize-insecure \
    --datadir="$work_dir/mysql-data" \
    --log-error="$work_dir/mysql-init.log"

MYDB_DATA_DIR="$work_dir/mydb-data" "$mydb_bin" \
    --host 127.0.0.1 --port "$mydb_port" \
    >"$work_dir/mydb.log" 2>&1 &
mydb_pid=$!

mysql_args=(
    "$mysql_bin" --protocol=TCP -h127.0.0.1 -P"$mysql_port" -uroot
    --batch --raw --skip-column-names
)
mydb_args=(
    env MYSQL_PWD=root "$mysql_bin" --protocol=TCP -h127.0.0.1 -P"$mydb_port" -uroot
    --ssl-mode=DISABLED --batch --raw --skip-column-names
)

wait_for_server() {
    local label="$1"
    shift
    local attempt
    for attempt in $(seq 1 120); do
        if "$@" -e "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 0.25
    done
    printf '%s did not become ready\n' "$label" >&2
    return 1
}
wait_for_server mydb "${mydb_args[@]}"

start_mysql() {
    local mode="$1"
    local secure_arg
    case "$mode" in
        empty) secure_arg="--secure-file-priv=" ;;
        directory) secure_arg="--secure-file-priv=$work_dir/files/allowed" ;;
        null) secure_arg="--secure-file-priv=NULL" ;;
        *) printf 'unknown mode: %s\n' "$mode" >&2; return 1 ;;
    esac
    "$mysqld_bin" --no-defaults \
        --datadir="$work_dir/mysql-data" \
        --socket="$work_dir/mysql.sock" \
        --port="$mysql_port" \
        --bind-address=127.0.0.1 \
        "$secure_arg" \
        --skip-log-bin \
        --log-error="$work_dir/mysql-$mode.log" \
        --pid-file="$work_dir/mysql.pid" &
    mysql_pid=$!
    wait_for_server mysql "${mysql_args[@]}"
}

stop_mysql() {
    if [[ -n "$mysql_pid" ]] && kill -0 "$mysql_pid" 2>/dev/null; then
        kill "$mysql_pid"
        wait "$mysql_pid" || true
    fi
    mysql_pid=""
}

compare_sql() {
    local label="$1"
    local sql="$2"
    local mysql_out mydb_out
    mysql_out="$("${mysql_args[@]}" -e "$sql" 2>&1)" || {
        printf 'MySQL failed for %s:\n%s\n' "$label" "$mysql_out" >&2
        return 1
    }
    mydb_out="$("${mydb_args[@]}" -e "$sql" 2>&1)" || {
        printf 'mydb failed for %s:\n%s\n' "$label" "$mydb_out" >&2
        return 1
    }
    if [[ "$mysql_out" != "$mydb_out" ]]; then
        printf 'LOAD_FILE differential mismatch: %s\n' "$label" >&2
        diff -u <(printf '%s\n' "$mysql_out") <(printf '%s\n' "$mydb_out") >&2 || true
        return 1
    fi
}

compare_error() {
    local label="$1"
    local sql="$2"
    local mysql_out mydb_out mysql_status mydb_status
    set +e
    mysql_out="$("${mysql_args[@]}" -e "$sql" 2>&1)"
    mysql_status=$?
    mydb_out="$("${mydb_args[@]}" -e "$sql" 2>&1)"
    mydb_status=$?
    set -e
    if [[ "$mysql_status" == 0 || "$mydb_status" == 0 ]]; then
        printf 'expected both servers to fail for %s\n' "$label" >&2
        return 1
    fi
    # mysql CLI may append an input line number depending on build options.
    mysql_out="$(printf '%s' "$mysql_out" | sed -E 's/ at line [0-9]+//')"
    mydb_out="$(printf '%s' "$mydb_out" | sed -E 's/ at line [0-9]+//')"
    if [[ "$mysql_out" != "$mydb_out" ]]; then
        printf 'LOAD_FILE error differential mismatch: %s\n' "$label" >&2
        diff -u <(printf '%s\n' "$mysql_out") <(printf '%s\n' "$mydb_out") >&2 || true
        return 1
    fi
}

inside="$work_dir/files/allowed/binary.bin"
empty="$work_dir/files/allowed/empty.bin"
outside="$work_dir/files/outside/outside.bin"
exact="$work_dir/files/allowed/exact.bin"
over="$work_dir/files/allowed/over.bin"

for mode in empty directory null; do
    start_mysql "$mode"
    case "$mode" in
        empty) mydb_secure="" ;;
        directory) mydb_secure="$work_dir/files/allowed" ;;
        null) mydb_secure="NULL" ;;
    esac
    "${mydb_args[@]}" -e "SET GLOBAL secure_file_priv='$mydb_secure'"
    compare_sql "$mode path matrix" \
        "SELECT IFNULL(HEX(LOAD_FILE('$inside')),'NULL'),
                IFNULL(LENGTH(LOAD_FILE('$empty')),'NULL'),
                IFNULL(HEX(LOAD_FILE('$outside')),'NULL'),
                CHARSET(LOAD_FILE('$inside')),COLLATION(LOAD_FILE('$inside'));"
    stop_mysql
done

start_mysql empty
"${mydb_args[@]}" -e "SET GLOBAL secure_file_priv=''"
compare_sql "NULL and missing" \
    "SELECT LOAD_FILE(NULL) IS NULL,
            LOAD_FILE('$work_dir/files/missing') IS NULL;"
compare_error "arity 1582" "SELECT LOAD_FILE()"
compare_error "non-regular 1085" "SELECT LOAD_FILE('$work_dir/files/allowed')"

# Both servers apply max_allowed_packet to a newly established session.
"${mysql_args[@]}" -e "SET GLOBAL max_allowed_packet=1024"
"${mydb_args[@]}" -e "SET GLOBAL max_allowed_packet=1024"
compare_sql "packet exact and overflow warning" \
    "SELECT LENGTH(LOAD_FILE('$exact'));
     SELECT LOAD_FILE('$over') IS NULL;
     SHOW WARNINGS;"

compare_sql "prepared reload" \
    "SET @p='$inside';
     PREPARE lf FROM 'SELECT HEX(LOAD_FILE(?))';
     EXECUTE lf USING @p;
     EXECUTE lf USING @p;
     DEALLOCATE PREPARE lf;"

"${mysql_args[@]}" -e \
    "CREATE USER IF NOT EXISTS 'load_diff'@'%' IDENTIFIED BY 'pw';
     REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'load_diff'@'%';"
"${mydb_args[@]}" -e \
    "CREATE USER IF NOT EXISTS 'load_diff'@'%' IDENTIFIED BY 'pw';
     REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'load_diff'@'%';"
mysql_user_args=(
    env MYSQL_PWD=pw "$mysql_bin" --protocol=TCP -h127.0.0.1 -P"$mysql_port" -uload_diff
    --batch --raw --skip-column-names
)
mydb_user_args=(
    env MYSQL_PWD=pw "$mysql_bin" --protocol=TCP -h127.0.0.1 -P"$mydb_port" -uload_diff
    --ssl-mode=DISABLED --batch --raw --skip-column-names
)
mysql_no_file="$("${mysql_user_args[@]}" -e "SELECT LOAD_FILE('$inside') IS NULL")"
mydb_no_file="$("${mydb_user_args[@]}" -e "SELECT LOAD_FILE('$inside') IS NULL")"
[[ "$mysql_no_file" == "$mydb_no_file" && "$mysql_no_file" == "1" ]]
"${mysql_args[@]}" -e "GRANT FILE ON *.* TO 'load_diff'@'%'"
"${mydb_args[@]}" -e "GRANT FILE ON *.* TO 'load_diff'@'%'"
mysql_has_file="$("${mysql_user_args[@]}" -e "SELECT HEX(LOAD_FILE('$inside'))")"
mydb_has_file="$("${mydb_user_args[@]}" -e "SELECT HEX(LOAD_FILE('$inside'))")"
[[ "$mysql_has_file" == "$mydb_has_file" ]]

stop_mysql
printf 'LOAD_FILE differential passed against MySQL 8.0.46\n'
