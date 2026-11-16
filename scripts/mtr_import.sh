#!/usr/bin/env bash
#
# mtr_import.sh — fetch upstream MySQL test files into the MTR-lite triage
# corpus so they can be run through the harness and categorised.
#
# The corpus (`tests/mydb-test/triage/{t,upstream}/`) is git-ignored: it holds
# un-adapted GPL-licensed upstream files used only as a local compatibility
# oracle. Adapted cases and their recorded expectations also stay in the
# git-ignored `tests/mydb-test/t/` and `tests/mydb-test/r/` local suite.
#
# Usage:
#   scripts/mtr_import.sh [suite/]name ...
#
#   name          a main-suite test (fetched from mysql-test/t/<name>.test)
#   suite/name    a suite test        (mysql-test/suite/<suite>/t/<name>.test)
#   inc:<path>    a subroutine/guard include — fetched from mysql-test/<path>
#                 into triage/<path> (mirrors the mysql-test/ layout so the
#                 harness's `--source <path>` resolves it; M8 vendoring)
#   data:<path>   a data fixture — fetched from mysql-test/<path> and mirrored
#                 under triage/ (for example std_data/loaddata_utf8.dat)
#
# Examples:
#   scripts/mtr_import.sh func_math type_decimal json/json_no_table
#   scripts/mtr_import.sh inc:include/func_in.inc inc:suite/json/inc/json_functions.inc
#   scripts/mtr_import.sh data:std_data/loaddata_utf8.dat
#   scripts/mtr_import.sh $(cat scripts/mtr_import.list)
#
# Env:
#   MTR_REF   git ref/branch on mysql/mysql-server
#             (default: pinned commit below for reproducible imports)

set -uo pipefail

DEFAULT_REF="d229bb760c49b65e19ec28342236961ad961d7fe"
REF="${MTR_REF:-${DEFAULT_REF}}"
BASE="https://raw.githubusercontent.com/mysql/mysql-server/${REF}/mysql-test"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TRIAGE_DIR="${ROOT}/tests/mydb-test/triage"
T_DIR="${TRIAGE_DIR}/t"
U_DIR="${TRIAGE_DIR}/upstream"

if [ "$#" -eq 0 ]; then
  echo "usage: $0 [suite/]name ..." >&2
  exit 2
fi

fetch() {
  # $1 = url, $2 = dest
  local url="$1" dest="$2"
  mkdir -p "$(dirname "$dest")"
  if curl -sf --max-time 30 "$url" -o "$dest"; then
    return 0
  fi
  return 1
}

ok=0; miss=0
for spec in "$@"; do
  if [[ "$spec" == data:* ]]; then
    datapath="${spec#data:}"
    if [[ -z "$datapath" || "$datapath" == /* || "/$datapath/" == *"/../"* ]]; then
      echo "  INVALID data path ${datapath}" >&2
      miss=$((miss+1))
    elif fetch "${BASE}/${datapath}" "${TRIAGE_DIR}/${datapath}"; then
      echo "  fetched data ${datapath}"
      ok=$((ok+1))
    else
      echo "  MISSING data ${datapath} (${BASE}/${datapath})" >&2
      miss=$((miss+1))
    fi
    continue
  fi
  if [[ "$spec" == inc:* ]]; then
    # Subroutine/guard include: mirror mysql-test/<path> → triage/<path>.
    # Includes mirror the mysql-test/ root (the harness resolves `--source
    # <path>` against triage_root, since the triage runner passes triage/ as
    # test_root — see mtr/mod.rs). So `include/foo.inc` → triage/include/foo.inc.
    incpath="${spec#inc:}"
    if fetch "${BASE}/${incpath}" "${TRIAGE_DIR}/${incpath}"; then
      echo "  fetched inc ${incpath}"
      ok=$((ok+1))
    else
      echo "  MISSING inc ${incpath} (${BASE}/${incpath})" >&2
      miss=$((miss+1))
    fi
    continue
  fi
  if [[ "$spec" == */* ]]; then
    suite="${spec%%/*}"; name="${spec##*/}"
    test_url="${BASE}/suite/${suite}/t/${name}.test"
    res_url="${BASE}/suite/${suite}/r/${name}.result"
    rel="${suite}/${name}"
  else
    name="$spec"
    test_url="${BASE}/t/${name}.test"
    res_url="${BASE}/r/${name}.result"
    rel="main/${name}"
  fi

  if fetch "$test_url" "${T_DIR}/${rel}.test"; then
    fetch "$res_url" "${U_DIR}/${rel}.result" || true
    echo "  fetched ${rel}"
    ok=$((ok+1))
  else
    echo "  MISSING ${rel} (${test_url})" >&2
    miss=$((miss+1))
  fi
done

echo "mtr_import: ${ok} fetched, ${miss} missing → ${T_DIR}"
echo "next: cargo nextest run --run-ignored only --no-capture mtr_triage"
