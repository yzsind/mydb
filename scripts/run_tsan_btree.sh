#!/usr/bin/env bash
# ThreadSanitizer (TSAN) run for the B-tree concurrency paths.
#
# Exercise concurrent mutations, split/merge reclamation, and cached frame
# publication. Every selected filter must match and execute real tests.
#
# TSAN needs a sanitizer-instrumented std, so we build std from source with
# `-Zbuild-std` (requires the nightly toolchain + the `rust-src` component:
#   rustup toolchain install nightly
#   rustup component add rust-src --toolchain nightly
# ). The first build is slow (recompiles std + the whole crate instrumented);
# subsequent runs are incremental.
#
# Usage:
#   scripts/run_tsan_btree.sh                 # current B-tree/page-cache regressions
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

# Keep these names aligned with the live concurrency regressions. Override by
# passing libtest substring filters; empty selections are always errors.
if [[ $# -gt 0 ]]; then
  FILTERS=("$@")
else
  FILTERS=(
    storage::core::btree_bug_audit_tests::bug8_concurrent_rw_invariants_hold
    storage::core::tests::btree_prepared_split_revalidates_leaf_after_concurrent_delete
    storage::core::tests::btree_concurrent_reads_do_not_descend_into_freed_merge_pages
    storage::core::tests::atomic_frame_hint_concurrent_replacement_preserves_identity_and_pin_release
  )
fi

export RUSTFLAGS="${RUSTFLAGS:-} -Zsanitizer=thread"
export TSAN_OPTIONS="${TSAN_OPTIONS:-halt_on_error=1}"
mkdir -p /tmp/mydb
run_dir="$(mktemp -d /tmp/mydb/tsan.XXXXXX)"
mkdir "$run_dir/tmp"
export TMPDIR="$run_dir/tmp"

echo "== TSAN B-tree concurrency run =="
echo "target      : $TARGET"
echo "filters     : ${FILTERS[*]}"
echo "TSAN_OPTIONS: $TSAN_OPTIONS"
echo "output      : $run_dir"
echo

# mimalloc's C allocator is not instrumented by RUSTFLAGS. Its cross-thread
# frees and block reuse are invisible to TSan, causing false races between an
# old Arc's final decrement and a new allocation at the same address. Disable
# fast-alloc for this run, retaining the other default protocol features.
# libtest accepts one positional filter. The first listing builds the same
# instrumented test binary reused by every subsequent listing and execution.
CARGO_TEST=(cargo +nightly test --locked -Zbuild-std --target "$TARGET" -p mydb --lib
  --no-default-features --features tls,zstd-compression)
for filter in "${FILTERS[@]}"; do
  if [[ -z "$filter" ]]; then
    echo "TSAN test filter must not be empty" >&2
    exit 1
  fi
  listing="$("${CARGO_TEST[@]}" -- "$filter" --list)"
  matches="$(printf '%s\n' "$listing" | awk '
    { gsub(/\033\[[0-9;]*m/, "") }
    /: test$/ { count++ }
    END { print count + 0 }
  ')"
  if [[ "$matches" -eq 0 ]]; then
    echo "TSAN filter '$filter' matches no tests; update the selection" >&2
    exit 1
  fi
done

group=0
for filter in "${FILTERS[@]}"; do
  group=$((group + 1))
  log="$run_dir/group-$group.log"
  echo "== TSAN group $group: $filter =="
  "${CARGO_TEST[@]}" -- "$filter" --nocapture 2>&1 | tee "$log"
  if ! awk '
    { gsub(/\033\[[0-9;]*m/, "") }
    /^test result: ok\. [1-9][0-9]* passed; 0 failed;/ { passed = 1 }
    END { exit !passed }
  ' "$log"; then
    echo "TSAN filter '$filter' executed no successful tests; inspect $log" >&2
    exit 1
  fi
done
