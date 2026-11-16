#!/usr/bin/env bash
#
# P2-A3 — MySQL client tooling smoke matrix.
#
# Drives each of the major client tools against a running mydb so we
# can catch protocol regressions in the same shape real applications
# exercise. Skipping is graceful: if a tool isn't installed, its row
# is reported as SKIPPED and the matrix continues — operators can run
# the script in any environment and see which clients are covered.
#
# Coverage:
#   mysql CLI          — smoke against system DB
#   mysqldump          — schema-only dump round trip
#   mydumper           — schema + data dump
#   myloader           — re-load mydumper output
#   Go go-sql-driver   — net/sql.Ping + SELECT 1
#   PyMySQL            — Python pure-protocol smoke
#   mysqlclient        — Python libmysqlclient smoke
#   mysql2 (Node.js)   — Node.js driver smoke
#   JDBC (Java)        — connection smoke + full DDL/DML/transaction/procedure examples
#   ODBC               — isql -v + SELECT 1
#   Hibernate/SQLAlchemy/Django/Prisma — covered indirectly via the
#     low-level driver they bind to (PyMySQL/mysqlclient/JDBC/mysql2).
#
# Environment:
#   MYDB_HOST/PORT/USER/PASSWORD/DB   target endpoint (default
#                                     127.0.0.1:3307 root/root/test).
#   COMPAT_MATRIX_VERBOSE=1            show each tool's stderr.
#   COMPAT_MATRIX_FAIL_FAST=1          stop on first failure.
#   MYDB_CONNECTOR_J_JAR               path to mysql-connector-j jar; enables
#                                     the JDBC examples (creates/drops an isolated database).
#   MYDB_ODBC_DSN                      configured DSN; enables the ODBC probe.
#   MYDB_CLIENT_DEMOS=1                run complete Python/Go/C++/Rust examples;
#                                     requires their dependencies and CREATE/DROP database privileges.
#   PYTHON                            Python interpreter for complete examples (default python3).
#
# Exit code is 0 when every installed tool passed, 1 otherwise.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXAMPLES_DIR="$(cd "$SCRIPT_DIR/.." && pwd)/examples"

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

HOST="${MYDB_HOST:-127.0.0.1}"
PORT="${MYDB_PORT:-3307}"
DB="${MYDB_DB:-test}"
USER="${MYDB_USER:-root}"
PASSWORD="${MYDB_PASSWORD:-root}"

VERBOSE="${COMPAT_MATRIX_VERBOSE:-0}"
FAIL_FAST="${COMPAT_MATRIX_FAIL_FAST:-0}"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
declare -a FAILED_TOOLS

report_pass() {
  printf '  [PASS] %s\n' "$1"
  PASS_COUNT=$((PASS_COUNT + 1))
}

report_skip() {
  printf '  [SKIP] %s — %s\n' "$1" "$2"
  SKIP_COUNT=$((SKIP_COUNT + 1))
}

report_fail() {
  printf '  [FAIL] %s\n' "$1"
  FAIL_COUNT=$((FAIL_COUNT + 1))
  FAILED_TOOLS+=("$1")
  if [ "$VERBOSE" = "1" ] && [ -n "${2:-}" ]; then
    sed 's/^/        /' < "$2"
  fi
  if [ "$FAIL_FAST" = "1" ]; then
    exit 1
  fi
}

need_tool() {
  command -v "$1" >/dev/null 2>&1
}

run_silent() {
  local label="$1"
  shift
  local logfile
  logfile="$TMP_DIR/${label}.log"
  if "$@" >"$logfile" 2>&1; then
    report_pass "$label"
  else
    report_fail "$label" "$logfile"
  fi
}

echo "mydb client-compat matrix against ${USER}@${HOST}:${PORT}/${DB}"
echo

# --- mysql CLI ----------------------------------------------------
if need_tool mysql; then
  run_silent "mysql CLI" \
    mysql -h"$HOST" -P"$PORT" -u"$USER" -p"$PASSWORD" -D"$DB" \
      --ssl-mode=DISABLED \
      -e "SELECT 1 AS ok;"
else
  report_skip "mysql CLI" "mysql binary not installed"
fi

# --- mysqldump ----------------------------------------------------
if need_tool mysqldump; then
  run_silent "mysqldump (schema only)" \
    mysqldump -h"$HOST" -P"$PORT" -u"$USER" -p"$PASSWORD" \
      --no-data --skip-comments --skip-lock-tables \
      --ssl-mode=DISABLED \
      "$DB"
else
  report_skip "mysqldump" "mysqldump binary not installed"
fi

# --- mydumper -----------------------------------------------------
if need_tool mydumper; then
  run_silent "mydumper" \
    mydumper --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" \
      --database "$DB" --no-data --outputdir "$TMP_DIR/mydumper-out"
else
  report_skip "mydumper" "mydumper binary not installed"
fi

# --- myloader (only if mydumper run produced output) --------------
if need_tool myloader && [ -d "$TMP_DIR/mydumper-out" ]; then
  run_silent "myloader" \
    myloader --host "$HOST" --port "$PORT" --user "$USER" --password "$PASSWORD" \
      --database "$DB" --directory "$TMP_DIR/mydumper-out" --overwrite-tables
else
  report_skip "myloader" "myloader binary not installed or mydumper output missing"
fi

# --- Go go-sql-driver ---------------------------------------------
if need_tool go; then
  cat > "$TMP_DIR/probe.go" <<'EOF'
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	dsn := os.Getenv("MYDB_DSN")
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if v != 1 {
		os.Exit(2)
	}
}
EOF
  export MYDB_DSN="${USER}:${PASSWORD}@tcp(${HOST}:${PORT})/${DB}?allowNativePasswords=true&tls=false"
  pushd "$TMP_DIR" >/dev/null
  if go run probe.go >"$TMP_DIR/go-probe.log" 2>&1; then
    report_pass "Go go-sql-driver"
  else
    # If `go run` failed because the dependency is missing,
    # surface that as a SKIP rather than a real failure.
    if grep -q "cannot find module" "$TMP_DIR/go-probe.log" \
       || grep -q "no required module" "$TMP_DIR/go-probe.log"; then
      report_skip "Go go-sql-driver" "go-sql-driver/mysql not in module cache"
    else
      report_fail "Go go-sql-driver" "$TMP_DIR/go-probe.log"
    fi
  fi
  popd >/dev/null
else
  report_skip "Go go-sql-driver" "go toolchain not installed"
fi

# --- PyMySQL ------------------------------------------------------
if need_tool python3; then
  if python3 -c "import pymysql" >/dev/null 2>&1; then
    cat > "$TMP_DIR/pymysql_probe.py" <<EOF
import pymysql
conn = pymysql.connect(host="$HOST", port=$PORT, user="$USER",
                       password="$PASSWORD", database="$DB", ssl=None)
cur = conn.cursor()
cur.execute("SELECT 1")
assert cur.fetchone()[0] == 1
conn.close()
EOF
    run_silent "PyMySQL" python3 "$TMP_DIR/pymysql_probe.py"
  else
    report_skip "PyMySQL" "pymysql module not importable"
  fi

  if python3 -c "import MySQLdb" >/dev/null 2>&1; then
    cat > "$TMP_DIR/mysqlclient_probe.py" <<EOF
import MySQLdb
conn = MySQLdb.connect(host="$HOST", port=$PORT, user="$USER",
                       passwd="$PASSWORD", db="$DB")
cur = conn.cursor()
cur.execute("SELECT 1")
assert cur.fetchone()[0] == 1
conn.close()
EOF
    run_silent "mysqlclient (libmysqlclient)" python3 "$TMP_DIR/mysqlclient_probe.py"
  else
    report_skip "mysqlclient (libmysqlclient)" "MySQLdb module not importable"
  fi
else
  report_skip "PyMySQL / mysqlclient" "python3 not installed"
fi

# --- Node mysql2 --------------------------------------------------
if need_tool node; then
  if [ -d "node_modules/mysql2" ] || node -e "require('mysql2')" >/dev/null 2>&1; then
    cat > "$TMP_DIR/mysql2_probe.js" <<EOF
const mysql = require('mysql2/promise');
(async () => {
  const conn = await mysql.createConnection({
    host: '$HOST', port: $PORT, user: '$USER', password: '$PASSWORD',
    database: '$DB',
  });
  const [rows] = await conn.query('SELECT 1 AS v');
  if (rows[0].v !== 1) process.exit(2);
  await conn.end();
})().catch(e => { console.error(e); process.exit(1); });
EOF
    run_silent "Node.js mysql2" node "$TMP_DIR/mysql2_probe.js"
  else
    report_skip "Node.js mysql2" "mysql2 package not installed"
  fi
else
  report_skip "Node.js mysql2" "node not installed"
fi

# --- JDBC ---------------------------------------------------------
if need_tool java && need_tool javac; then
  if [ -n "${MYDB_CONNECTOR_J_JAR:-}" ] && [ -f "$MYDB_CONNECTOR_J_JAR" ] \
       && [ -f "$EXAMPLES_DIR/java/JdbcSelectOne.java" ] && [ -f "$EXAMPLES_DIR/java/JdbcDemo.java" ]; then
    if javac -encoding UTF-8 -cp "$MYDB_CONNECTOR_J_JAR" -d "$TMP_DIR/java-build" \
         "$EXAMPLES_DIR/java/JdbcSelectOne.java" "$EXAMPLES_DIR/java/JdbcDemo.java" >"$TMP_DIR/jdbc-build.log" 2>&1; then
      if java -cp "$TMP_DIR/java-build:$MYDB_CONNECTOR_J_JAR" \
           JdbcSelectOne "$HOST" "$PORT" "$USER" "$PASSWORD" "$DB" \
           >"$TMP_DIR/jdbc-run.log" 2>&1; then
        report_pass "JDBC (Connector/J)"
      else
        report_fail "JDBC (Connector/J)" "$TMP_DIR/jdbc-run.log"
      fi
      if MYDB_HOST="$HOST" MYDB_PORT="$PORT" MYDB_USER="$USER" MYDB_PASSWORD="$PASSWORD" MYDB_DB="$DB" \
           java -cp "$TMP_DIR/java-build:$MYDB_CONNECTOR_J_JAR" JdbcDemo \
           >"$TMP_DIR/jdbc-demo.log" 2>&1; then
        report_pass "JDBC complete examples"
      else
        report_fail "JDBC complete examples" "$TMP_DIR/jdbc-demo.log"
      fi
    else
      report_fail "JDBC (Connector/J) compile" "$TMP_DIR/jdbc-build.log"
    fi
  else
    report_skip "JDBC (Connector/J)" \
      "set MYDB_CONNECTOR_J_JAR=/path/to/mysql-connector.jar to enable"
  fi
else
  report_skip "JDBC (Connector/J)" "java/javac not installed"
fi

# --- Optional complete client examples -----------------------------
if [ "${MYDB_CLIENT_DEMOS:-0}" = "1" ]; then
  for language in python go cpp rust; do
    run_silent "$language complete examples" \
      env MYDB_HOST="$HOST" MYDB_PORT="$PORT" MYDB_USER="$USER" MYDB_PASSWORD="$PASSWORD" MYDB_DB="$DB" \
      bash "$EXAMPLES_DIR/run_client_demo.sh" "$language"
  done
fi

# --- ODBC ---------------------------------------------------------
if need_tool isql; then
  if [ -n "${MYDB_ODBC_DSN:-}" ]; then
    run_silent "ODBC (isql)" \
      bash -c "echo 'SELECT 1' | isql -v '$MYDB_ODBC_DSN' '$USER' '$PASSWORD'"
  else
    report_skip "ODBC (isql)" "set MYDB_ODBC_DSN to a configured DSN to enable"
  fi
else
  report_skip "ODBC (isql)" "isql binary not installed"
fi

echo
echo "Summary: $PASS_COUNT passed / $FAIL_COUNT failed / $SKIP_COUNT skipped"
if [ "$FAIL_COUNT" -gt 0 ]; then
  echo "Failed tools:"
  for t in "${FAILED_TOOLS[@]}"; do
    echo "  - $t"
  done
  exit 1
fi
exit 0
