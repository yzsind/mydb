#!/usr/bin/env bash
# ThreadSanitizer (TSAN) run for the B-tree concurrency paths.
#
# Purpose: gate the Phase 2b "flip" (dropping the tree-global SMO write lock,
# starting with the shared-lock in-place write fast path,
# `MYDB_BTREE_CONCURRENT_SMO=1`). TSAN instruments every memory access and
# flags data races that the deterministic oracle/storm/crash tests can miss
# because they only observe *outcomes*, not the races that produced them.
#
# TSAN needs a sanitizer-instrumented std, so we build std from source with
# `-Zbuild-std` (requires the nightly toolchain + the `rust-src` component:
#   rustup toolchain install nightly
#   rustup component add rust-src --toolchain nightly
# ). The first build is slow (recompiles std + the whole crate instrumented);
# subsequent runs are incremental.
#
# Usage:
#   scripts/run_tsan_btree.sh                 # concurrent SMO + OLC + merge-race tests
#   scripts/run_tsan_btree.sh <filter>...     # custom test name filters
#
# Env:
#   MYDB_TSAN_TARGET   override the build-std target triple (default: host)
#   TSAN_OPTIONS       passed through (default: halt_on_error=1)
set -euo pipefail

cd "$(dirname "$0")/.."

# Default target triple = host (build-std must be per-target).
host_triple="$(rustc -vV | awk '/^host:/{print $2}')"
TARGET="${MYDB_TSAN_TARGET:-$host_triple}"

# Concurrency tests that exercise the shared-lock write path, the optimistic
# read path, and the split/merge freed-page race. Override by passing args.
if [[ $# -gt 0 ]]; then
  FILTERS=("$@")
else
  FILTERS=(
    concurrent_smo
    olc_reads_stay_consistent_under_smo_storm
    olc_disabled_pessimistic_path_matches
    btree_concurrent_reads_do_not_descend_into_freed_merge_pages
  )
fi

export RUSTFLAGS="${RUSTFLAGS:-} -Zsanitizer=thread"
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1}"
# Enable the shared-lock write path so TSAN sees it; the coarse-lock oracle
# tests still run (they just ignore the flag).
export MYDB_BTREE_CONCURRENT_SMO="${MYDB_BTREE_CONCURRENT_SMO:-1}"

echo "== TSAN B-tree concurrency run =="
echo "target      : $TARGET"
echo "filters     : ${FILTERS[*]}"
echo "TSAN_OPTIONS: $TSAN_OPTIONS"
echo "MYDB_BTREE_CONCURRENT_SMO=$MYDB_BTREE_CONCURRENT_SMO"
echo

# `cargo +nightly test` (not nextest) — nextest's per-test process model does
# not compose with -Zbuild-std as cleanly, and TSAN reports go to stderr which
# `cargo test` streams verbatim. libtest accepts one positional filter, so run
# every audited concurrency filter explicitly; the instrumented build is reused.
for filter in "${FILTERS[@]}"; do
  cargo +nightly test \
    -Zbuild-std \
    --target "$TARGET" \
    -p mydb --lib \
    -- "$filter" --nocapture
done
