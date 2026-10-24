#!/usr/bin/env bash
# Build artifacts and tool caches stay outside the checkout.
set -euo pipefail
if [[ $# -gt 1 ]]; then
  echo "usage: $0 [python|go|cpp|rust|all]" >&2
  exit 2
fi
EXAMPLES_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb
export PYTHONDONTWRITEBYTECODE=1
export GOCACHE="${GOCACHE:-/tmp/mydb/demo-go-cache}"
export GOMODCACHE="${GOMODCACHE:-/tmp/mydb/demo-go-mod}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/mydb/demo-rust-target}"

need() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Required tool not found: $1" >&2
    exit 2
  fi
}

case "${1:-all}" in
  python)
    PYTHON_BIN="${PYTHON:-python3}"
    need "$PYTHON_BIN"
    "$PYTHON_BIN" -c 'import pymysql' || {
      echo "Install examples/python/requirements.txt with the selected PYTHON interpreter." >&2
      exit 2
    }
    "$PYTHON_BIN" "$EXAMPLES_DIR/python/demo.py"
    ;;
  go)
    need go
    cd "$EXAMPLES_DIR/go"
    go run -mod=readonly .
    ;;
  cpp)
    CXX_BIN="${CXX:-c++}"
    need "$CXX_BIN"
    BUILD_DIR="$(mktemp -d /tmp/mydb/cpp-demo.XXXXXX)"
    trap 'rm -rf "$BUILD_DIR"' EXIT
    # Configuration tools produce whitespace-separated flags, not shell code.
    if [[ -z "${MYSQL_CONFIG:-}" ]] && command -v pkg-config >/dev/null 2>&1 && pkg-config --exists mysqlclient; then
      read -r -a MYSQL_CFLAGS <<< "$(pkg-config --cflags mysqlclient)"
      read -r -a MYSQL_LIBS <<< "$(pkg-config --libs mysqlclient)"
    else
      MYSQL_CONFIG_BIN="${MYSQL_CONFIG:-mysql_config}"
      need "$MYSQL_CONFIG_BIN"
      read -r -a MYSQL_CFLAGS <<< "$("$MYSQL_CONFIG_BIN" --cflags)"
      read -r -a MYSQL_LIBS <<< "$("$MYSQL_CONFIG_BIN" --libs)"
    fi
    "$CXX_BIN" -std=c++17 -Wall -Wextra -Werror "${MYSQL_CFLAGS[@]}" \
      "$EXAMPLES_DIR/cpp/demo.cpp" "${MYSQL_LIBS[@]}" -o "$BUILD_DIR/demo"
    "$BUILD_DIR/demo"
    ;;
  rust)
    need cargo
    cargo run --locked --manifest-path "$EXAMPLES_DIR/rust/Cargo.toml"
    ;;
  all)
    for language in python go cpp rust; do
      bash "$EXAMPLES_DIR/run_client_demo.sh" "$language"
    done
    ;;
  *)
    echo "usage: $0 [python|go|cpp|rust|all]" >&2
    exit 2
    ;;
esac
