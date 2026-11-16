#!/usr/bin/env bash
# Generate the TPC-DS SF=1 expected result set + a PostgreSQL performance
# reference, from the local PG 18 instance (see CLAUDE.md / memory for the
# connection). Run after the data is loaded (schema_pg.sql + load_pg.sql +
# indexes_pg.sql + ANALYZE). Two passes:
#
#   golden : parallelism OFF (deterministic, reproducible output) -> writes
#            scripts/tpcds/expected/sf-<SF>/qNN.tsv  (tab-separated, no header,
#            NULL printed as the literal "NULL" to match mydb's mysql --batch).
#   perf   : work_mem=64MB, max_parallel_workers_per_gather=2 (the documented
#            mydb-comparison config), warm runs, output discarded -> per-query
#            server-side "Time: N ms" captured into pg_query_times.csv.
#
# Env: PGHOST/PGPORT/PGUSER/PGDB override the connection; SF overrides scale tag.
set -uo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@18/bin}"
PGHOST="${PGHOST:-127.0.0.1}"; PGPORT="${PGPORT:-15432}"
PGUSER="${PGUSER:-postgres}";   PGDB="${PGDB:-tpcds_sf1}"
SF="${SF:-1}"

SUITE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"   # scripts/tpcds
QDIR="${SUITE_DIR}/pg/queries"
OUT_DIR="${SUITE_DIR}/expected/sf-${SF}"
RESULT_DIR="${TPCDS_PG_RESULT_DIR:-/tmp/mydb/tpcds-pg-results/sf-${SF}}"
mkdir -p "$OUT_DIR" "$RESULT_DIR"

PSQL=("$PGBIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDB" -X)
QUERIES="${TPCDS_QUERIES:-$(seq 1 99)}"
PASS="${TPCDS_PG_PASS:-both}"          # both | golden | perf
PERF_SKIP=" ${TPCDS_PERF_SKIP:-} "     # query nums to skip in the perf pass (e.g. pathological q04/q11)

# LC_ALL=C forces psql's \timing line to the English "Time: N ms" the perf
# parser greps for (a localized client prints e.g. "时间：N ms" and the parse
# silently yields 0). Numeric *data* output is locale-independent in psql.
now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }

# ---- Pass 1: golden result set (deterministic) ----
if [[ "$PASS" == both || "$PASS" == golden ]]; then
echo "[golden] writing ${OUT_DIR}/qNN.tsv (parallel off, work_mem=256MB)"
echo "query_num,status,rows,elapsed_ms" > "${RESULT_DIR}/golden_status.csv"
ok=0; fail=0
GOLDEN_OPTS="-c work_mem=256MB -c max_parallel_workers_per_gather=0 -c statement_timeout=600000 -c jit=off"
for q in $QUERIES; do
  qn="$(printf '%02d' "$q")"
  qfile="${QDIR}/q${qn}.sql"
  [[ -f "$qfile" ]] || { echo "  Q${qn}  SKIP (no file)"; continue; }
  out="${OUT_DIR}/q${qn}.tsv"
  err="${RESULT_DIR}/q${qn}_golden_err.txt"
  s="$(now_ms)"
  if LC_ALL=C PGOPTIONS="$GOLDEN_OPTS" "${PSQL[@]}" -v ON_ERROR_STOP=1 -At -F $'\t' -P null=NULL \
        -f "$qfile" > "$out" 2> "$err"; then
    e="$(now_ms)"; rows="$(wc -l < "$out" | tr -d ' ')"
    printf '  Q%-2s  OK    rows=%-6s %6d ms\n' "$qn" "$rows" "$((e - s))"
    echo "${q},ok,${rows},$((e - s))" >> "${RESULT_DIR}/golden_status.csv"
    rm -f "$err"; ok=$((ok + 1))
  else
    e="$(now_ms)"
    printf '  Q%-2s  FAIL  %s\n' "$qn" "$(head -1 "$err")"
    echo "${q},fail,0,$((e - s))" >> "${RESULT_DIR}/golden_status.csv"
    rm -f "$out"; fail=$((fail + 1))
  fi
done
echo "[golden] ok=${ok} fail=${fail}"
fi   # end golden pass

# ---- Pass 2: performance reference (warm, parallel=2, work_mem=64MB) ----
if [[ "$PASS" == both || "$PASS" == perf ]]; then
# One persistent session per iteration with \timing; parse "Time: N ms".
PERF_OPTS="-c work_mem=64MB -c max_parallel_workers_per_gather=2 -c effective_cache_size=2GB -c statement_timeout=600000 -c jit=off"
run_perf_iter() {   # $1 = output csv
  local csv="$1"; echo "query_num,elapsed_ms" > "$csv"
  for q in $QUERIES; do
    local qn qfile t
    qn="$(printf '%02d' "$q")"; qfile="${QDIR}/q${qn}.sql"
    [[ -f "$qfile" ]] || continue
    if [[ "$PERF_SKIP" == *" $q "* ]]; then echo "${q},-1" >> "$csv"; continue; fi
    # \timing prints "Time: N.NNN ms" for the statement; sum all such lines
    # (a query file is one statement here) and discard the result rows.
    t="$(LC_ALL=C PGOPTIONS="$PERF_OPTS" "${PSQL[@]}" -q -A -t -o /dev/null \
           -c '\timing on' -f "$qfile" 2>/dev/null | grep -i '^Time:' \
           | awk '{s+=$2} END{printf "%.0f", s}')"
    [[ -z "$t" ]] && t=-1   # query errored
    echo "${q},${t}" >> "$csv"
  done
}
# The golden pass already touched every table, so the cache is warm; skip the
# extra warm-up unless explicitly requested.
if [[ "${TPCDS_PG_PERF_WARMUP:-0}" == "1" ]]; then
  echo "[perf] warm-up iteration (parallel=2, work_mem=64MB)..."
  run_perf_iter "${RESULT_DIR}/pg_query_times_warmup.csv" >/dev/null
fi
echo "[perf] measured iteration (parallel=2, work_mem=64MB, warm)..."
run_perf_iter "${RESULT_DIR}/pg_query_times.csv"
echo "[perf] times -> ${RESULT_DIR}/pg_query_times.csv"
fi   # end perf pass
echo "done."
