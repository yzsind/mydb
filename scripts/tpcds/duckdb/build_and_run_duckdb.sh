#!/usr/bin/env bash
# End-to-end: build a DuckDB TPC-DS reference database at a given scale factor
# from the cached .dat files mydb uses, then generate the expected result set.
# One command per scale factor.
#
#   scripts/tpcds/duckdb/build_and_run_duckdb.sh [SF]      # SF default 1; e.g. 0.1
#
# Loads /tmp/mydb/tpcds-dbgen-cache/sf-<SF> into a DuckDB file, runs the *exact*
# mydb query files (transformed only for DuckDB dialect by gen_duckdb_artifacts.py),
# and writes scripts/tpcds/expected-duckdb/sf-<SF>/qNN.tsv (tab-separated, no
# header, NULL printed as the literal "NULL" to match mydb's mysql --batch and
# the canonicalization in scripts/tpcds_check_results.py).
#
# This is a SECOND, independent oracle alongside the PostgreSQL one under
# scripts/tpcds/expected/ — it does not touch that set. Point the checker at
# either dir to cross-validate mydb.
#
# Env: TPCDS_DSDGEN_CACHE_ROOT (default /tmp/mydb/tpcds-dbgen-cache),
#      TPCDS_QUERIES (subset, e.g. "1 5 22"; default 1..99),
#      TPCDS_SKIP_BUILD=1 (reuse an already-loaded .duckdb file; just run queries),
#      DUCKDB (duckdb binary, default `duckdb` on PATH),
#      TPCDS_DUCKDB_DB (db file path), TPCDS_DUCKDB_RESULT_DIR (status/report dir).
set -uo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

SF="${1:-1}"
DUCKDB="${DUCKDB:-duckdb}"
CACHE="${TPCDS_DSDGEN_CACHE_ROOT:-/tmp/mydb/tpcds-dbgen-cache}/sf-${SF}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"     # scripts/tpcds/duckdb
SUITE="$(cd "$HERE/.." && pwd)"                          # scripts/tpcds
OUT_DIR="${SUITE}/expected-duckdb/sf-${SF}"
DB="${TPCDS_DUCKDB_DB:-/tmp/mydb/tpcds-duckdb/sf-${SF}.duckdb}"
RESULT_DIR="${TPCDS_DUCKDB_RESULT_DIR:-/tmp/mydb/tpcds-duckdb-results/sf-${SF}}"
QDIR="${HERE}/queries"
QUERIES="${TPCDS_QUERIES:-$(seq 1 99)}"

# Per-query session setup, prepended to every query so DuckDB's ORDER BY lines
# up with mydb (and the PostgreSQL oracle):
#   - default_null_order: NULLs sort as the largest value (last under ASC, first
#     under DESC). Matches mydb's run_tpcds.sh default (MYDB_ORDER_BY_NULLS=last)
#     so the 11 ROLLUP queries with NULL-keyed super-aggregate rows line up.
#   - default_collation NOCASE: case-insensitive string ordering, matching mydb's
#     default utf8mb4_0900_ai_ci collation (and PG's locale collation). Without
#     it DuckDB's binary codepoint order puts e.g. 'NY Metro' before 'North …'.
#     TPC-DS data is ASCII English so case-insensitivity is the only axis that
#     matters (accents never appear); WHERE filters use exact-case literals that
#     match the data, so this does not change row selection.
SESSION_SET="SET default_null_order='nulls_last_on_asc_first_on_desc'; SET default_collation='NOCASE';"

# 24 benchmark tables (column order = dsdgen output); dbgen_version is a
# generator artifact and is neither loaded nor queried.
TABLES="income_band reason ship_mode warehouse store call_center web_site web_page promotion catalog_page household_demographics customer_address customer_demographics customer item date_dim time_dim inventory web_returns catalog_returns store_returns web_sales catalog_sales store_sales"

mkdir -p "$OUT_DIR" "$RESULT_DIR" "$(dirname "$DB")"

echo "=== TPC-DS SF=${SF} -> DuckDB ${DB} (cache ${CACHE}) ==="
command -v "$DUCKDB" >/dev/null 2>&1 || { echo "error: duckdb binary '${DUCKDB}' not found" >&2; exit 1; }
[[ -d "$CACHE" ]] || { echo "error: cache dir ${CACHE} not found" >&2; exit 1; }

python3 "$HERE/gen_duckdb_artifacts.py" >/dev/null   # ensure schema/queries are current

if [[ "${TPCDS_SKIP_BUILD:-0}" != "1" ]]; then
  echo "[build] (re)creating db ${DB}"
  rm -f "$DB"

  echo "[build] schema"
  "$DUCKDB" "$DB" -bail < "$HERE/schema_duckdb.sql" >/dev/null

  echo "[build] COPY load (delimiter '|', NULL marker \\N)"
  load_sql="$(mktemp)"
  for t in $TABLES; do
    [[ -f "${CACHE}/${t}.dat" ]] || { echo "error: ${CACHE}/${t}.dat missing" >&2; exit 1; }
    printf "COPY %s FROM '%s/%s.dat' (DELIMITER '|', NULL '\\\\N', HEADER false, QUOTE '');\n" "$t" "$CACHE" "$t" >> "$load_sql"
  done
  time "$DUCKDB" "$DB" -bail < "$load_sql" >/dev/null
  rm -f "$load_sql"

  echo "[build] row counts:"
  for t in store_sales catalog_sales web_sales inventory store_returns customer date_dim item; do
    printf '  %-16s %s\n' "$t" "$("$DUCKDB" "$DB" -noheader -list -c "SELECT count(*) FROM ${t}")"
  done
fi

# ---- golden result set (deterministic) ----
echo "[golden] writing ${OUT_DIR}/qNN.tsv"
echo "query_num,status,rows,elapsed_ms" > "${RESULT_DIR}/golden_status.csv"
ok=0; fail=0
now_ms() { python3 -c 'import time;print(int(time.time()*1000))'; }
for q in $QUERIES; do
  qn="$(printf '%02d' "$q")"
  qfile="${QDIR}/q${qn}.sql"
  [[ -f "$qfile" ]] || { echo "  Q${qn}  SKIP (no file)"; continue; }
  out="${OUT_DIR}/q${qn}.tsv"
  err="${RESULT_DIR}/q${qn}_err.txt"
  run_sql="$(mktemp)"
  printf '%s\n' "$SESSION_SET" > "$run_sql"
  cat "$qfile" >> "$run_sql"
  s="$(now_ms)"
  if "$DUCKDB" "$DB" -bail -list -noheader -separator $'\t' -nullvalue 'NULL' \
        -f "$run_sql" > "$out" 2> "$err"; then
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
  rm -f "$run_sql"
done
echo "[golden] ok=${ok} fail=${fail}"

# ---- REPORT.md ----
{
  echo "# TPC-DS SF=${SF} expected result set (DuckDB oracle)"
  echo
  echo "Generated by \`scripts/tpcds/duckdb/build_and_run_duckdb.sh ${SF}\` from the"
  echo "cached \`.dat\` files under \`${CACHE}\` (the same data mydb loads), running the"
  echo "mydb query files transformed for DuckDB dialect (\`gen_duckdb_artifacts.py\`)."
  echo "DuckDB version: \`$("$DUCKDB" --version)\`. NULL order: PG-equivalent"
  echo "(\`nulls_last_on_asc_first_on_desc\`)."
  echo
  echo "- queries OK: **${ok}**, FAIL: **${fail}**"
  echo
  echo "## Fact / dimension row counts"
  echo
  echo "| table | rows |"
  echo "|---|---|"
  for t in store_sales catalog_sales web_sales inventory store_returns catalog_returns web_returns customer date_dim item; do
    printf '| %s | %s |\n' "$t" "$("$DUCKDB" "$DB" -noheader -list -c "SELECT count(*) FROM ${t}")"
  done
  echo
  echo "## Per-query status (rows, elapsed)"
  echo
  echo "| query | status | rows | ms |"
  echo "|---|---|---|---|"
  tail -n +2 "${RESULT_DIR}/golden_status.csv" | while IFS=, read -r qnum st rws ms; do
    printf '| q%02d | %s | %s | %s |\n' "$qnum" "$st" "$rws" "$ms"
  done
} > "${OUT_DIR}/REPORT.md"

echo "=== SF=${SF} done -> ${OUT_DIR}/ (qNN.tsv + REPORT.md) ==="
[[ "$fail" -eq 0 ]]
