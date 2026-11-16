#!/usr/bin/env bash
# Run the persistent-format decoders against the release corpus and mutations.
# All build output, generated inputs and crash artifacts stay under /tmp/mydb.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p /tmp/mydb
fuzz_root="$(mktemp -d /tmp/mydb/storage-format-fuzz.XXXXXX)"
mkdir -p "$fuzz_root/fuzz_targets" "$fuzz_root/corpus"
python3 - "$repo_root" "$fuzz_root" <<'PY'
import json, pathlib, sys
repo, target = map(pathlib.Path, sys.argv[1:])
(target / 'Cargo.toml').write_text('''[package]
name = "mydb-storage-format-fuzz"
version = "0.0.0"
edition = "2021"
publish = false
[package.metadata]
cargo-fuzz = true
[workspace]
[dependencies]
libfuzzer-sys = "0.4"
mydb = { path = ''' + json.dumps(str(repo)) + ''', default-features = false, features = ["fuzzing"] }
[[bin]]
name = "storage_formats"
path = "fuzz_targets/storage_formats.rs"
test = false
doc = false
bench = false
''')
(target / 'fuzz_targets/storage_formats.rs').write_text('''#![no_main]
use libfuzzer_sys::fuzz_target;
fuzz_target!(|bytes: &[u8]| mydb::fuzzing::storage_formats(bytes));
''')
PY
cp "$repo_root/Cargo.lock" "$fuzz_root/Cargo.lock"
cp "$repo_root"/tests/fixtures/storage-format-v1/*.bin "$fuzz_root/corpus/"
export TMPDIR=/tmp/mydb
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-/tmp/mydb/storage-format-fuzz-target}"
fuzz_profile=()
if [[ "${MYDB_FUZZ_DEV:-0}" == 1 ]]; then fuzz_profile+=(--dev); fi
cargo "+${MYDB_FUZZ_TOOLCHAIN:-nightly}" fuzz run "${fuzz_profile[@]}" --codegen-units 16 --fuzz-dir "$fuzz_root" \
  storage_formats "$fuzz_root/corpus" -- -max_total_time=60 -timeout=5 \
  -rss_limit_mb=2048 "-artifact_prefix=$fuzz_root/" "$@"
