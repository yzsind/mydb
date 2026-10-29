# mydb-cli

An independent Rust CLI for MyDB, with an interactive monitor and batch SQL execution.
It uses the repository's maintained MySQL protocol driver without depending on the
server crate. Build from the repository checkout so both vendored driver crates are
available:

```sh
cargo build --locked --release --manifest-path clients/mydb-cli/Cargo.toml
./clients/mydb-cli/target/release/mydb-cli -h127.0.0.1 -P8738 -uroot -p test
```

See [the client manual](../../docs/mydb-cli.md) for parameters, configuration, TLS,
SQL scripts, output formats and supported boundaries.

Run isolated client tests from this directory:

```sh
mkdir -p /tmp/mydb
TMPDIR=/tmp/mydb cargo nextest run --locked --config-file .config/nextest.toml --no-fail-fast
```

Run the process and PTY acceptance suite from the repository root:

```sh
python3 clients/mydb-cli/tests/acceptance.py \
  --cli clients/mydb-cli/target/debug/mydb-cli \
  --server-binary target/debug/mydb --output /tmp/mydb/cli-acceptance
```

The crate is not published to crates.io. Its lockfile and release dependency
inventory are maintained separately from the server's.
