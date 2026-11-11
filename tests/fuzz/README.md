# Fuzz Testing

This directory is a standalone `mydb-fuzz` Cargo package. It depends on the repository-root mydb crate through the `../..` path with the `fuzzing` feature enabled.

After installing a nightly toolchain and cargo-fuzz, run from the repository root:

```bash
cargo install cargo-fuzz
cargo +nightly fuzz list --fuzz-dir tests/fuzz
cargo +nightly fuzz build --fuzz-dir tests/fuzz
cargo +nightly fuzz run --fuzz-dir tests/fuzz mysql_packet -- -runs=1000
```

Available targets: `mysql_packet`, `row_codec`, `jsonb`, `wal_frame`, `expression_differential`, `native_repl_protocol`, `sql_parser`, `sql_lex`, `parser_roundtrip`, and `parser_fast_path_differential`. Replace `mysql_packet` in the run command with the target you want.

The CI workflow `.github/workflows/deep-safety.yml` builds targets from this directory and runs smoke tests for the protocol, row codec, JSONB, and WAL. `corpus/`, `artifacts/`, `target/`, and `coverage/` are locally generated directories excluded by the `.gitignore` in this directory.

## SQL parser targets

`sql_parser` exercises statement, batch and expression entry points and then
checks that a fresh parse still succeeds. `sql_lex` checks cursor progress and
UTF-8-safe ranges. `parser_roundtrip` checks scalar AST encode/decode identity.
`parser_fast_path_differential` compares supported fast paths with canonical
conversion. Each accepts a two-byte little-endian SQL-mode prefix followed by
UTF-8 SQL (at most 65,536 SQL bytes). Invalid UTF-8 belongs to protocol tests.

Generate seeds from the retained audit observations and general grammar cases:

```bash
python3 tests/fuzz/make_parser_corpus.py --output /tmp/mydb/parser-fuzz
cargo +nightly fuzz run --dev --fuzz-dir tests/fuzz \
  --target-dir /tmp/mydb/parser-fuzz/build sql_parser \
  /tmp/mydb/parser-fuzz/corpus/sql_parser -- \
  -dict=tests/fuzz/parser.dict -max_total_time=60 -max_len=4096 \
  -rss_limit_mb=2048 -artifact_prefix=/tmp/mydb/parser-fuzz/artifacts/sql_parser/
```

Replace `sql_parser` in all three positions to run each other target. Keep
corpora, artifacts and build outputs under `/tmp/mydb`. Save a minimized finding
as a deterministic Rust regression before fixing it. These smoke runs use the development profile with AddressSanitizer; omit
`--dev` for optimized longer runs. Instrumented smoke runs supplement, rather than replace, longer sanitizer runs and execution tests.
The ordinary `fuzzing::parser_smoke` test also runs deterministic malformed-input
and SQL-mode seeds without requiring a nightly compiler.
