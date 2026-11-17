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

set -euo pipefail

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

# Validate the entire plan before writing anything, including both destinations
# of a test/result pair and existing symlinks beneath the corpus root.
if ! python3 - "$TRIAGE_DIR" "$@" <<'PY_VALIDATE'
from pathlib import Path
import re
import sys
root = Path(sys.argv[1]).resolve()
for descriptor in sys.argv[2:]:
    kind, separator, path = descriptor.partition(':')
    if not separator:
        kind, path = 'test', descriptor
    parts = path.split('/')
    if (kind not in ('test', 'inc', 'data') or not path
            or any(part in ('', '.', '..') or not re.fullmatch(r'[A-Za-z0-9_.+-]+', part) for part in parts)
            or (kind == 'test' and len(parts) > 2)):
        sys.exit('invalid import descriptor: ' + repr(descriptor))
    if kind == 'test':
        relative = path if len(parts) == 2 else 'main/' + path
        destinations = ['t/' + relative + '.test', 'upstream/' + relative + '.result']
    else:
        destinations = [path]
    for destination in destinations:
        if not (root / destination).resolve().is_relative_to(root):
            sys.exit('import destination escapes corpus through a symlink: ' + descriptor)
PY_VALIDATE
then
  exit 2
fi

mkdir -p /tmp/mydb
partial=""
trap 'if [[ -n "$partial" ]]; then rm -f -- "$partial"; fi' EXIT

fetch() {
  local url="$1" dest="$2"
  mkdir -p "$(dirname "$dest")" || return 1
  partial="$(mktemp /tmp/mydb/mtr-import.XXXXXX)" || return 1
  if curl -sf --max-time 30 "$url" -o "$partial" && mv -f -- "$partial" "$dest"; then
    partial=""
    return 0
  fi
  rm -f -- "$partial"
  partial=""
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
    if fetch "$res_url" "${U_DIR}/${rel}.result"; then
      echo "  fetched ${rel}"
      ok=$((ok+1))
    else
      echo "  MISSING result ${rel} (${res_url})" >&2
      miss=$((miss+1))
    fi
  else
    echo "  MISSING ${rel} (${test_url})" >&2
    miss=$((miss+1))
  fi
done

echo "mtr_import: ${ok} fetched, ${miss} missing → ${T_DIR}"
echo "next: cargo nextest run --run-ignored only --no-capture mtr_triage"

(( miss == 0 ))
