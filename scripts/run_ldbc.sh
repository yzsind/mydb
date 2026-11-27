#!/usr/bin/env bash
set -euo pipefail

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
result_root=/tmp/mydb/ldbc-results
latest_cold="$result_root/latest-cold"
queries_only="${LDBC_QUERIES_ONLY:-0}"
skip_build="${LDBC_SKIP_BUILD:-0}"
snapshot="${LDBC_SNAPSHOT:-}"
explicit_snapshot=0
validation="${LDBC_VALIDATION:-}"
capture="${LDBC_CAPTURE_DIR:-}"
native_library="${LDBC_NATIVE_LIBRARY:-}"
result_dir="${MYDB_LDBC_RESULT_DIR:-}"
workers="${LDBC_WORKERS:-4}"
repeat="${LDBC_REPEAT:-}"
warmup="${LDBC_WARMUP_PASSES:-0}"
python="${PYTHON_BIN:-python3}"

usage() {
  cat <<'EOF'
Usage: run_ldbc.sh [options]

Run LDBC SNB Interactive v1 SF=1 full validation from a stopped mydb
snapshot. The default run clones the snapshot, executes durable updates,
and validates all 138,747 operations.

  --snapshot DIR        Set the stopped mydb snapshot (LDBC_SNAPSHOT).
  --validation FILE     Set the official SF=1 oracle file (LDBC_VALIDATION).
  --result-dir DIR      Use a new output directory under /tmp/mydb
                        (MYDB_LDBC_RESULT_DIR). Defaults to a timestamped
                        directory in /tmp/mydb/ldbc-results.
  --skip-build          Reuse a compiled verifier from the last successful
                        cold run, or use LDBC_NATIVE_LIBRARY. Env: LDBC_SKIP_BUILD=1.
  --queries-only [DIR]  Rebuild the initial indexes once and replay a verified
                        capture without database writes. Uses the last
                        successful cold run if DIR and LDBC_CAPTURE_DIR are unset.
                        Env: LDBC_QUERIES_ONLY=1.
  --workers N           Use N verification workers (LDBC_WORKERS, default: 4).
  --repeat N            Run N measured replays with --queries-only
                        (LDBC_REPEAT, default: 3).
  --warmup              Run one unmeasured replay with --queries-only
                        (LDBC_WARMUP_PASSES accepts a pass count).
  -h, --help            Show this help.

Without explicit paths, the script uses the existing local SF=1 fixture:
  /tmp/mydb/ldbc-snb-sf1-opt/data
  /tmp/mydb/ldbc-snb-sf1-20261127/validation/validation_params-sf1.csv

Exit status 3 means validation passed but exceeded the applicable time limit.
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

while (($#)); do
  case "$1" in
    --snapshot|--validation|--result-dir|--workers|--repeat)
      [[ $# -ge 2 && -n "$2" && "$2" != --* ]] || fail "$1 requires a value"
      case "$1" in
        --snapshot) snapshot="$2"; explicit_snapshot=1 ;;
        --validation) validation="$2" ;;
        --result-dir) result_dir="$2" ;;
        --workers) workers="$2" ;;
        --repeat) repeat="$2" ;;
      esac
      shift
      ;;
    --queries-only)
      queries_only=1
      if [[ $# -ge 2 && "$2" != -* ]]; then
        capture="$2"
        shift
      fi
      ;;
    --skip-build) skip_build=1 ;;
    --warmup) warmup=1 ;;
    -h|--help) usage; exit 0 ;;
    *) fail "Unknown option: $1 (see --help)" ;;
  esac
  shift
done

[[ "$queries_only" =~ ^[01]$ ]] || fail "LDBC_QUERIES_ONLY must be 0 or 1"
[[ "$skip_build" =~ ^[01]$ ]] || fail "LDBC_SKIP_BUILD must be 0 or 1"
[[ "$workers" =~ ^[1-9][0-9]*$ ]] || fail "--workers must be a positive integer"
[[ "$warmup" =~ ^(0|[1-9][0-9]*)$ ]] || fail "LDBC_WARMUP_PASSES must be a nonnegative integer"
command -v "$python" >/dev/null || fail "Cannot find Python interpreter: $python"

last_capture=
if [[ -f "$latest_cold" ]]; then
  IFS= read -r last_capture < "$latest_cold" || fail "Cannot read $latest_cold"
fi

validation="${validation:-/tmp/mydb/ldbc-snb-sf1-20261127/validation/validation_params-sf1.csv}"
[[ -f "$validation" && -r "$validation" ]] || fail "Validation file is unavailable: $validation. Set --validation FILE."
result_dir="${result_dir:-$result_root/$(date +%Y%m%d-%H%M%S)-$$}"
[[ ! -e "$result_dir" && ! -L "$result_dir" ]] || fail "Result directory already exists: $result_dir"

args=(--validation "$validation" --work-dir "$result_dir" --workers "$workers" --compact)
if [[ "$queries_only" == 1 ]]; then
  [[ "$explicit_snapshot" == 0 ]] || fail "--snapshot applies to cold validation. Use --queries-only DIR."
  capture="${capture:-$last_capture}"
  [[ -n "$capture" ]] || fail "Run cold validation first or specify --queries-only DIR."
  [[ -r "$capture/summary.json" ]] || fail "Cannot read a capture summary in $capture"
  repeat="${repeat:-3}"
  [[ "$repeat" =~ ^[1-9][0-9]*$ ]] || fail "--repeat must be a positive integer"
  args+=(--queries-only "$capture" --repeat "$repeat" --warmup-passes "$warmup")
  printf 'LDBC SNB Interactive v1 SF=1 replay (%s workers, %s measured passes)\n' "$workers" "$repeat"
  printf 'Verified capture %s\n' "$capture"
else
  [[ -z "$repeat" && "$warmup" == 0 ]] || fail "--repeat and --warmup require --queries-only"
  snapshot="${snapshot:-/tmp/mydb/ldbc-snb-sf1-opt/data}"
  [[ -d "$snapshot" ]] || fail "Snapshot is unavailable: $snapshot. Set --snapshot DIR."
  printf 'LDBC SNB Interactive v1 SF=1 cold validation (%s workers)\n' "$workers"
  printf 'Stopped snapshot %s\n' "$snapshot"
  args+=(--snapshot "$snapshot")
  if [[ "$skip_build" == 1 ]]; then
    if [[ -z "$native_library" && -n "$last_capture" && -r "$last_capture/summary.json" ]]; then
      native_library="$("$python" -c 'import json, sys; print(json.load(sys.stdin).get("native_library", ""))' < "$last_capture/summary.json")"
    fi
    [[ -n "$native_library" && -r "$native_library" ]] ||
      fail "--skip-build needs a compiled verifier. Run without --skip-build or set LDBC_NATIVE_LIBRARY."
    args+=(--native-library "$native_library")
    printf 'Verifier library %s\n' "$native_library"
  fi
fi

mkdir -p /tmp/mydb
export TMPDIR=/tmp/mydb
export PYTHONDONTWRITEBYTECODE=1
printf 'Results %s\n' "$result_dir"

status=0
"$python" -u "$root_dir/scripts/ldbc_snb_validation/run.py" "${args[@]}" || status=$?
if [[ "$queries_only" == 0 && "$status" == 0 ]]; then
  mkdir -p "$result_root"
  marker="$(mktemp "$result_root/.latest-cold.XXXXXX")"
  (cd "$result_dir" && pwd -P) > "$marker"
  mv -f "$marker" "$latest_cold"
fi
if [[ "$status" == 3 ]]; then
  printf 'Validation passed. The time limit was exceeded. See %s/summary.json\n' "$result_dir"
fi
exit "$status"
