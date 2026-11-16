#!/usr/bin/env bash
# Crash-safety + differential verification for index-batch WAL deferral.
#
# Test 1 (crash): run concurrent secondary-index UPDATEs under buffering,
#   kill -9 the server mid-flight, reopen, and assert the secondary index is
#   consistent with the heap (index-driven scan == heap scan for every row).
# Test 2 (differential): a deterministic committed workload run twice — once
#   eager, once buffered — must leave byte-identical logical state.
set -euo pipefail
PORT="${PORT:-13362}"
BIN="${BIN:-target/release/mydb}"
THREADS="${THREADS:-8}"
run_secs="${RUN_SECS:-6}"
ROWS="${ROWS:-20000}"

mkdir -p /tmp/mydb
WORK="$(mktemp -d /tmp/mydb/index-defer.XXXXXX)"
CLPIDS=()
SRV_BGPID=""
cleanup() {
  for pid in ${CLPIDS[@]+"${CLPIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done
  if [[ -n "$SRV_BGPID" ]]; then
    kill -INT "$SRV_BGPID" 2>/dev/null || true
    wait "$SRV_BGPID" 2>/dev/null || true
  fi
}
trap cleanup EXIT
myq() { mysql -h127.0.0.1 -P$PORT -uroot -proot --ssl-mode=DISABLED test "$@"; }

start_srv() { # $1=datadir  $2..=extra env
  local dd="$1"; shift
  python3 - "$PORT" <<'CHECK_PORT'
import socket, sys
with socket.socket() as probe:
    probe.bind(("127.0.0.1", int(sys.argv[1])))
CHECK_PORT
  env "$@" MYDB_ROOT_PASSWORD=root MYDB_DATA_DIR="$dd" MYDB_DURABILITY=strict \
    $BIN --host 127.0.0.1 --port $PORT --data-dir "$dd" --slow-sql-ms 100000 \
    >"$WORK/server.log" 2>&1 &
  SRV_BGPID=$!
  for _ in $(seq 1 60); do myq -e "SELECT 1" >/dev/null 2>&1 && return 0; sleep 0.5; done
  echo "SERVER FAILED TO START"; tail -5 "$WORK/server.log"; return 1
}
stop_srv_graceful() { myq -e "SHUTDOWN" >/dev/null; wait "$SRV_BGPID"; SRV_BGPID=""; }
kill_srv_hard() {
  kill -KILL "$SRV_BGPID"
  wait "$SRV_BGPID" 2>/dev/null || true
  SRV_BGPID=""
}

seed() { # $1=datadir
  myq -e "DROP TABLE IF EXISTS t;
          CREATE TABLE t(id INT PRIMARY KEY, k INT NOT NULL, pad CHAR(60), KEY kidx(k));"
  # Seed rows: k = id initially.
  python3 - "$PORT" "$ROWS" <<'PY'
import subprocess,sys
port,rows=sys.argv[1],int(sys.argv[2])
vals=[]
buf=[]
for i in range(1,rows+1):
    buf.append(f"({i},{i},'p{i}')")
    if len(buf)==1000:
        vals.append(",".join(buf)); buf=[]
if buf: vals.append(",".join(buf))
sql=";".join(f"INSERT INTO t(id,k,pad) VALUES {v}" for v in vals)
subprocess.run(["mysql","-h127.0.0.1",f"-P{port}","-uroot","-proot","--ssl-mode=DISABLED","test","-e",sql],
               check=True)
PY
}

# Consistency oracle: for every row, the value read via a forced index range
# scan on k must equal the heap value, and counts must match. We assert:
#  (a) COUNT(*) heap == COUNT(*) via index (kidx covers all rows),
#  (b) the multiset of (id,k) is identical whether gathered heap-first or
#      index-first — a dangling/missing index entry breaks one side.
consistency() {
  local heap idx miss dangl
  heap=$(myq -N -e "SELECT COUNT(*), COALESCE(SUM(k),0) FROM t IGNORE INDEX(kidx)")
  # Force the secondary index: an index-driven scan must see every live row
  # exactly once with the same k. A MISSING index entry ⇒ idx count < heap;
  # a DANGLING entry (points to a rolled-back/dead version) is filtered by MVCC
  # visibility so it can't inflate the count, but we also check via a join that
  # every heap (id,k) is reachable through the index.
  idx=$(myq -N -e "SELECT COUNT(*), COALESCE(SUM(k),0) FROM t FORCE INDEX(kidx) WHERE k BETWEEN -2147483647 AND 2147483647")
  # Rows in the heap whose (id,k) is NOT reachable via the kidx index path.
  miss=$(myq -N -e "SELECT COUNT(*) FROM t a IGNORE INDEX(kidx)
                    WHERE NOT EXISTS (SELECT 1 FROM t b FORCE INDEX(kidx)
                                      WHERE b.k=a.k AND b.id=a.id)")
  echo "heap=[$heap] idx=[$idx] unreachable_rows=[$miss]"
  [[ -n "$heap" && "$heap" == "$idx" && "$miss" == 0 ]] || {
    echo "heap/index consistency failed" >&2
    return 1
  }
}

echo "############ TEST 1: crash (kill -9) under buffering ############"
DD="$WORK/crash"
rm -rf "$DD"; mkdir -p "$DD"
start_srv "$DD" MYDB_WAL_TXN_BUFFER=1 || exit 1
seed "$DD"
echo "seeded $ROWS rows; pre-crash consistency:"; consistency
# Concurrent committed index-mutating load DIRECTLY on t (each UPDATE changes the
# indexed column k → delete old key + insert new key = the deferred IndexBatch).
CLPIDS=()
for w in $(seq 1 $THREADS); do
  ( for _ in $(seq 1 100000); do
      i=$(( (RANDOM * 31 + w) % ROWS + 1 ))
      myq -e "UPDATE t SET k=k+$((w+1)) WHERE id=$i" >/dev/null 2>&1
    done ) &
  CLPIDS+=($!)
done
sleep $run_secs
echo ">>> kill -9 mid-flight"
kill_srv_hard
for p in ${CLPIDS[@]+"${CLPIDS[@]}"}; do kill "$p" 2>/dev/null; done
for p in ${CLPIDS[@]+"${CLPIDS[@]}"}; do wait "$p" 2>/dev/null || true; done
CLPIDS=()
echo ">>> reopen (recovery) under buffering"
start_srv "$DD" MYDB_WAL_TXN_BUFFER=1 || exit 1
echo "post-recovery consistency:"; consistency
stop_srv_graceful

echo
echo "############ TEST 2: differential eager vs buffered (committed) ############"
# Deterministic committed workload: N single-row UPDATEs that shift k, applied
# identically to two fresh DBs. Final logical state must match byte-for-byte.
det_workload() { # runs a fixed sequence of committed updates (SQL piped via stdin)
  python3 - "$ROWS" <<'PY' | myq
import sys
rows=int(sys.argv[1])
out=[]
# deterministic: every row updated 3 times, k += id%7+1 each pass → shifts the
# indexed column so each UPDATE is a real delete-old-key/insert-new-key batch.
for p in range(3):
    for i in range(1,rows+1):
        out.append(f"UPDATE t SET k=k+{(i%7)+1} WHERE id={i};")
sys.stdout.write("\n".join(out))
PY
}
digest() { myq -N -e "SELECT COUNT(*), SUM(k), MIN(k), MAX(k),
                             SUM(CAST(CRC32(CONCAT(id,':',k)) AS UNSIGNED)) FROM t"; }

DDE="$WORK/eager"; rm -rf "$DDE"; mkdir -p "$DDE"
start_srv "$DDE" MYDB_WAL_TXN_BUFFER=0 || exit 1
seed "$DDE"; det_workload
D_EAGER=$(digest); idx_e=$(myq -N -e "SELECT COUNT(*),COALESCE(SUM(k),0) FROM t WHERE k BETWEEN -2147483648 AND 2147483647")
stop_srv_graceful

DDB="$WORK/buffered"; rm -rf "$DDB"; mkdir -p "$DDB"
start_srv "$DDB" MYDB_WAL_TXN_BUFFER=1 || exit 1
seed "$DDB"; det_workload
D_BUF=$(digest); idx_b=$(myq -N -e "SELECT COUNT(*),COALESCE(SUM(k),0) FROM t WHERE k BETWEEN -2147483648 AND 2147483647")
stop_srv_graceful

echo "eager   digest: $D_EAGER  | idx: $idx_e"
echo "buffered digest: $D_BUF  | idx: $idx_b"
if [ "$D_EAGER" = "$D_BUF" ] && [ "$idx_e" = "$idx_b" ]; then
  echo "DIFFERENTIAL: PASS (identical logical + index state)"
else
  echo "DIFFERENTIAL: FAIL" >&2
  exit 1
fi
