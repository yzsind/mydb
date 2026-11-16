# Fuzz Testing

This directory is a standalone `mydb-fuzz` Cargo package. It depends on the repository-root mydb crate through the `../..` path with the `fuzzing` feature enabled.

After installing a nightly toolchain and cargo-fuzz, run from the repository root:

```bash
cargo install cargo-fuzz
cargo +nightly fuzz list --fuzz-dir tests/fuzz
cargo +nightly fuzz build --fuzz-dir tests/fuzz
cargo +nightly fuzz run --fuzz-dir tests/fuzz mysql_packet -- -runs=1000
```

Available targets (15): `mysql_packet`, `row_codec`, `schema_row_codec`,
`jsonb`, `wal_frame`, `storage_formats`, `expression_differential`,
`sql_execution_differential`, `native_repl_protocol`, `mysql_binlog`,
`sql_parser`, `sql_lex`, `parser_roundtrip`,
`parser_fast_path_differential`, and `storage_model` (stateful model
differential over insert/update/delete, savepoints, WAL durability modes,
checkpoint/reopen and LOB chains). Replace `mysql_packet` in the run command
with the target you want. The Cargo manifest is the authoritative target list.

The working-tree workflows `.github/workflows/safety.yml` and
`.github/workflows/deep-safety.yml` use `scripts/run_fuzz.py` to build and run
every declared target with ASan. Safety declares 10 seconds per target for
PR/push and 300 seconds daily; deep-safety declares 60 seconds for tag runs
and accepts a per-target seconds input for manual runs. The runner replays and
minimizes every crash artifact it finds (`minimized-*` files) so findings can
be converted into deterministic regressions. The `jepsen` job of `safety.yml`
also executes the in-crate deterministic fuzz regressions via
`cargo nextest run --features fuzzing fuzz`. Workflow declarations do not
establish that CI has run successfully.
`corpus/`, `artifacts/`, `target/`, and `coverage/` are locally generated
directories excluded by the `.gitignore` in this directory.

Run all targets with generated seeds, logs and crash artifacts under `/tmp/mydb`:

```bash
python3 scripts/run_fuzz.py --output /tmp/mydb/fuzz-smoke --seconds 10
```

The runner defaults to the development profile; add `--optimized` for longer
optimized ASan runs. Repeated `--target NAME` options select a subset. Parser
seeds, structured row/binlog seeds and storage fixtures supplement generic
byte inputs. Each input has a 30-second timeout, except
`sql_execution_differential`, whose setup and repeated SQL oracles allow 180
seconds. `--input-timeout SECONDS` overrides that limit for the selected targets
and their crash replay/minimization. The default wall limit also allows the
initial corpus to finish before libFuzzer checks its campaign time limit;
`--wall-timeout SECONDS` overrides the campaign wall limit.
A successful smoke run does not establish semantic or branch
coverage. In particular, execution configuration changes do not prove that
parallel execution or spilling occurred, and frame decoding does not test
transaction recovery or replication application.

Local development audits belong in the Git-ignored `docs-internal/` directory.
The local `fuzz-jepsen-coverage-plan-2026-11-15.md` records each target's
oracle, limits, missing stateful tests and acceptance criteria, together with
the Jepsen fault matrix; it is not distributed with a clean checkout.

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
