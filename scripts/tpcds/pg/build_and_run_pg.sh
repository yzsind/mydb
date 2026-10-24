#!/usr/bin/env bash
# End-to-end: build a PostgreSQL TPC-DS reference database at a given scale
# factor from the cached .dat files mydb uses, then generate the expected result
# set + performance reference. One command per scale factor.
#
#   scripts/tpcds/pg/build_and_run_pg.sh [SF]      # SF default 1; e.g. 0.1
#
# Loads /tmp/mydb/tpcds-dbgen-cache/sf-<SF> into PG db tpcds_sf<tag> (tag = SF
# with '.' -> '_'), builds the same indexes mydb uses, ANALYZEs, and runs both
# passes via run_pg_expected.sh -> scripts/tpcds/expected/sf-<SF>/.
#
# Env passthrough: TPCDS_PERF_SKIP (perf-pass skip list), TPCDS_PG_PASS,
# TPCDS_SKIP_BUILD=1 (reuse an already-loaded db; just run the passes).
set -euo pipefail

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb

SF="${1:-1}"
DBTAG="tpcds_sf${SF//./_}"
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@18/bin}"
PGHOST="${PGHOST:-127.0.0.1}"; PGPORT="${PGPORT:-15432}"; PGUSER="${PGUSER:-postgres}"
CACHE="${TPCDS_DSDGEN_CACHE_ROOT:-/tmp/mydb/tpcds-dbgen-cache}/sf-${SF}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Per-SF result dir so concurrent/sequential scale runs don't clobber each other.
export TPCDS_PG_RESULT_DIR="${TPCDS_PG_RESULT_DIR:-/tmp/mydb/tpcds-pg-results/sf-${SF}}"

TABLES="income_band reason ship_mode warehouse store call_center web_site web_page promotion catalog_page household_demographics customer_address customer_demographics customer item date_dim time_dim inventory web_returns catalog_returns store_returns web_sales catalog_sales store_sales"

psql_db() { LC_ALL=C "$PGBIN/psql" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$DBTAG" -X "$@"; }

echo "=== TPC-DS SF=${SF} -> PG db ${DBTAG} (cache ${CACHE}) ==="
[[ -d "$CACHE" ]] || { echo "error: cache dir ${CACHE} not found" >&2; exit 1; }

if [[ "${TPCDS_SKIP_BUILD:-0}" != "1" ]]; then
  python3 "$HERE/gen_pg_artifacts.py" >/dev/null   # ensure schema/indexes/queries are current

  echo "[build] (re)creating db ${DBTAG}"
  LC_ALL=C "$PGBIN/dropdb"   -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" --if-exists "$DBTAG"
  LC_ALL=C "$PGBIN/createdb" -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" "$DBTAG"

  echo "[build] schema"
  psql_db -v ON_ERROR_STOP=1 -q -f "$HERE/schema_pg.sql"

  echo "[build] COPY load (NULL marker \\N)"
  load_sql="$(mktemp)"
  for t in $TABLES; do
    [[ -f "${CACHE}/${t}.dat" ]] || { echo "error: ${CACHE}/${t}.dat missing" >&2; exit 1; }
    printf "COPY %s FROM '%s/%s.dat' WITH (FORMAT text, DELIMITER '|', NULL '\\\\N');\n" "$t" "$CACHE" "$t" >> "$load_sql"
  done
  time psql_db -v ON_ERROR_STOP=1 -q -f "$load_sql"
  rm -f "$load_sql"

  echo "[build] indexes"
  time psql_db -v ON_ERROR_STOP=1 -q -c "SET maintenance_work_mem='512MB';" -f "$HERE/indexes_pg.sql" >/dev/null

  echo "[build] ANALYZE"
  psql_db -q -c "ANALYZE;"

  echo "[build] row counts:"
  for t in store_sales catalog_sales web_sales inventory store_returns customer date_dim item; do
    printf '  %-16s %s\n' "$t" "$(psql_db -Atc "SELECT count(*) FROM ${t}")"
  done
fi

echo "[run] expected + perf passes"
SF="$SF" PGDB="$DBTAG" bash "$HERE/run_pg_expected.sh"
python3 "$HERE/summarize_pg_results.py" "$TPCDS_PG_RESULT_DIR" "$SF"
echo "=== SF=${SF} done -> scripts/tpcds/expected/sf-${SF}/ ==="
