#!/usr/bin/env bash
# Shared ownership and readiness checks for disposable local Docker instances.

mydb_container_init() {
  MYDB_CONTAINER_OWNER="$(python3 -c 'import uuid; print(uuid.uuid4().hex)')"
  CONTAINER="${2:-${1}-${MYDB_CONTAINER_OWNER}}"
  MYDB_CONTAINER_ID=""
}

mydb_container_run() {
  MYDB_CONTAINER_ID="$(docker run --name "$CONTAINER" \
    --label "org.mydb.test.owner=$MYDB_CONTAINER_OWNER" "$@")"
  [[ -n "$MYDB_CONTAINER_ID" ]] || { echo "docker run returned no container ID" >&2; return 1; }
}

mydb_container_cleanup() {
  if [[ -n "${MYDB_CONTAINER_ID:-}" ]]; then
    local owner
    owner="$(docker inspect --format '{{ index .Config.Labels "org.mydb.test.owner" }}' \
      "$MYDB_CONTAINER_ID" 2>/dev/null)" || return 0
    if [[ "$owner" == "$MYDB_CONTAINER_OWNER" ]]; then
      docker rm -fv "$MYDB_CONTAINER_ID" >/dev/null 2>&1 || true
    fi
  fi
}

mydb_container_wait() {
  local attempts="${1:-90}"
  for (( attempt=0; attempt<attempts; attempt++ )); do
    # The image bootstraps a temporary socket-only server before its final
    # instance. TCP readiness must not accept that temporary instance.
    if docker exec "$CONTAINER" mysql --protocol=TCP --host=127.0.0.1 --port=3306 -uroot -p"$MYSQL_ROOT_PASSWORD" \
      --batch --skip-column-names -e 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "MySQL did not become ready in $attempts attempts: $CONTAINER" >&2
  return 1
}

mydb_test_data_init() {
  MYDB_TEST_OWNS_DATA=0
  if [[ -z "${MYDB_DATA_DIR:-}" ]]; then
    MYDB_DATA_DIR="$(mktemp -d /tmp/mydb/replication-data.XXXXXX)"
    MYDB_TEST_OWNS_DATA=1
  else
    MYDB_DATA_DIR="$(python3 - "$MYDB_DATA_DIR" <<'PY'
from pathlib import Path
import sys
path = Path(sys.argv[1]).resolve()
if not path.is_relative_to(Path('/tmp/mydb').resolve()) or path == Path('/tmp/mydb').resolve():
    sys.exit('MYDB_DATA_DIR must be a dedicated empty directory below /tmp/mydb')
if path.exists() and any(path.iterdir()):
    sys.exit('MYDB_DATA_DIR must be empty')
path.mkdir(parents=True, exist_ok=True)
print(path)
PY
)" || return 1
  fi
}

mydb_test_data_cleanup() {
  if [[ "${MYDB_TEST_OWNS_DATA:-0}" == 1 ]]; then
    rm -rf -- "$MYDB_DATA_DIR"
  fi
}

mydb_assert_ports_free() {
  python3 - "$@" <<'PY'
import contextlib, socket, sys
ports = [int(value) for value in sys.argv[1:]]
if len(ports) != len(set(ports)):
    sys.exit('test ports must be distinct')
with contextlib.ExitStack() as stack:
    for port in ports:
        listener = stack.enter_context(socket.socket())
        listener.bind(('127.0.0.1', port))
PY
}

mydb_wait_native() {
  local port="$1" pid="$2"
  for (( attempt=0; attempt<120; attempt++ )); do
    kill -0 "$pid" 2>/dev/null || { echo "mydb exited during startup" >&2; return 1; }
    if MYSQL_PWD="${MYDB_ROOT_PASSWORD:-root}" mysql --protocol=TCP -h127.0.0.1 \
      -P"$port" -uroot -e 'SELECT 1' >/dev/null 2>&1; then
      return 0
    fi
    sleep .5
  done
  echo "mydb did not become ready on port $port" >&2
  return 1
}

mydb_stop_native() {
  local pid="$1"
  if kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    for (( attempt=0; attempt<60; attempt++ )); do
      kill -0 "$pid" 2>/dev/null || break
      sleep .5
    done
    kill -KILL "$pid" 2>/dev/null || true
  fi
  wait "$pid" 2>/dev/null || true
}
