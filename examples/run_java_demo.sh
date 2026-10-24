#!/usr/bin/env bash
# Compile outside the checkout and run the complete JDBC example against a running mydb.
set -euo pipefail

EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if ! command -v javac >/dev/null 2>&1 || ! command -v java >/dev/null 2>&1; then
  echo "Java JDK 8 or later (java and javac) is required." >&2
  exit 2
fi
if [[ ! -f "${MYDB_CONNECTOR_J_JAR:-}" ]]; then
  echo "Set MYDB_CONNECTOR_J_JAR to a MySQL Connector/J jar path." >&2
  exit 2
fi

mkdir -p /tmp/mydb
BUILD_DIR="$(mktemp -d /tmp/mydb/jdbc-demo.XXXXXX)"
trap 'rm -rf "$BUILD_DIR"' EXIT
javac -encoding UTF-8 -cp "$MYDB_CONNECTOR_J_JAR" -d "$BUILD_DIR" "$EXAMPLES_DIR/java/JdbcDemo.java"
java -cp "$BUILD_DIR:$MYDB_CONNECTOR_J_JAR" JdbcDemo "$@"
