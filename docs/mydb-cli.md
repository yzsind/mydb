# MyDB CLI

`mydb-cli` is MyDB's Rust command-line client. It provides an interactive SQL
monitor, script execution, TLS, completion and formatted results over the MySQL
classic protocol. The client is an independent crate and does not link the server
engine. The source build and packaging tools include it; packages published before
this addition may contain only the server and backup tool.

## Build and connect

Build from the repository root with Rust 1.95 or newer:

```sh
cargo build --locked --release --manifest-path clients/mydb-cli/Cargo.toml
./clients/mydb-cli/target/release/mydb-cli -h127.0.0.1 -P8738 -uroot -p test
```

On Windows, use `clients\mydb-cli\target\release\mydb-cli.exe`. The standalone
manifest uses sibling `vendor/mysql` and `vendor/mysql_common` directories from
the checkout. The binary itself does not require the checkout or a MySQL client.
For a local installation, `cargo install --locked --path clients/mydb-cli` installs
`mydb-cli` in Cargo's binary directory. crates.io installation is not supported.

Defaults are TCP host `127.0.0.1`, port `8738`, user `root`, no selected database,
and an empty password. `-p` reads a hidden password from the controlling terminal;
it never consumes SQL from standard input. `-p test` selects database `test`.
Inline passwords must be attached: `-pVALUE` or `--password=VALUE`.
`--password=` explicitly selects an empty password.

## Interactive use

Interactive mode requires both stdin and stdout to be terminals, without `-e` or
`-B`. SQL can span lines and ends with `;`, `\g`, or `\G`. The last form displays
that statement vertically. The welcome banner includes the full server version
reported by `VERSION()`; `status` shows the same value. The prompt shows the current database. SQL is sent
unchanged after removing the client delimiter.

```sql
SELECT 1 AS answer;
SELECT * FROM orders LIMIT 5\G
USE reporting;
```

Tab completes SQL keywords, client commands, databases, tables and columns. Object
names are cached at connect and after database changes; `rehash` refreshes the
cache after DDL. `--skip-auto-rehash` disables automatic metadata queries. Metadata
runs on a separate connection, preserving the main session's warnings, transaction
and `ROW_COUNT()`. Missing metadata privileges only reduce completion coverage.

| Command | Short form | Behavior |
| --- | --- | --- |
| `help` | `\h`, `\?`, `?` | Show local command help |
| `quit`, `exit` | `\q` | Exit the client |
| `clear` | `\c` | Clear the unfinished SQL buffer |
| `print` | `\p` | Print the SQL buffer |
| `source FILE` | `\. FILE` | Execute a UTF-8 SQL file in the same session |
| `delimiter TEXT` | `\d TEXT` | Change the statement delimiter |
| `status` | `\s` | Show endpoint, connection id, database and TLS state |
| `rehash` | `\#` | Refresh completion names |
| — | `\g`, `\G` | Execute the buffer, normally or vertically |

Long commands are recognized on their own input line when the SQL buffer is empty;
an optional trailing `;` is accepted
except that `delimiter` takes its argument literally. Quoted text and comments do
not execute local commands. Delimiters are 1–32 non-whitespace ASCII bytes without
quotes, backslashes or leading comment markers. For stored programs:

```sql
delimiter $$
CREATE PROCEDURE p()
BEGIN
  SELECT 1;
  SELECT 2;
END$$
delimiter ;
CALL p();
```

Source files support quoted paths with spaces. Nested paths resolve relative to
the containing script; cycles and nesting beyond 16 levels produce errors.
Delimiter changes inside a source file remain active after it returns.

Ctrl+C while editing clears the buffer. During execution, the first Ctrl+C requests
`KILL QUERY` through a separate connection with the same credentials and TLS
policy; another Ctrl+C exits immediately with status 130. A completed cancellation
cannot target the next statement. Cancellation does not automatically roll back
previously completed changes. Ctrl+D exits, reporting and discarding unfinished SQL.

History retains at most 1,000 complete interactive entries in `~/.mydb-cli_history`.
Use `--history-file=PATH` to choose another file or `--no-history` to disable it.
Entries starting with whitespace and recognized password/secret/token/credential
statements are excluded. Other SQL, including data literals, may be recorded. Unix
history files use permissions `0600`. Batch execution never writes history.

## Scripts and output

```sh
mydb-cli -uroot -p test -e 'SELECT 1; SELECT 2;'
mydb-cli -uroot -p test < schema.sql
mydb-cli -uroot -p test -BN -e 'SELECT id, name FROM customers' > customers.tsv
mydb-cli -uroot -p test --table -e 'SELECT 1 AS answer'
```

Repeated `-e` options execute in order in one connection. A complete final SQL
statement does not require a delimiter in batch mode. Unclosed quotes/comments
and invalid UTF-8 input fail with a source location. Output formats are:

- **Table:** default interactively, or `-t`. Columns use Unicode display widths;
  values are complete and never silently truncated. Formatted rows exceeding
  approximately 8 MiB spill to an automatically removed system temporary file
  before the aligned table is printed. This bounds accumulated rows, not the size
  of an individual field or protocol packet.
- **TSV:** default for batch mode. No banner, prompt or row-count summary. Column
  names appear unless `-N` is set. Tabs, newlines, carriage returns, NUL and
  backslashes are escaped; `-r` emits raw field bytes instead.
- **Vertical:** `-E` for every result, or `\G` for one statement. Results stream.
- **Quick:** `-q` streams table rows using widths from the header and first row;
  later longer values remain complete and can exceed the initial column width.
  TSV already streams regardless of this option.

SQL NULL is printed as `NULL`. Invalid UTF-8 becomes a hex value in escaped output;
raw TSV retains the original bytes. `--binary-as-hex[=true|false]` controls hex
rendering for binary columns and defaults to true only in interactive mode. Numeric
text, including DECIMAL and unsigned 64-bit integers, is not converted through
floating point. VECTOR columns decode the protocol FLOAT32 coordinates into array
text such as `[1,-2.5,3]`, across all server vector storage datatypes; raw TSV
retains their wire bytes. Table and vertical output preserve embedded newlines,
tabs, carriage returns and literal backslashes, matching mysql human-readable
output; NUL bytes display as spaces. Other terminal control characters are escaped.

All result sets are consumed, including stored-procedure results. Interactive
execution reports affected rows, row counts, elapsed time and warning counts.
`--show-warnings` additionally runs `SHOW WARNINGS` after a statement with warnings.

Batch SQL errors stop execution by default. `-f/--force` continues after recoverable
SQL errors but still returns status 1. Input, connection, output and temporary-file
errors stop execution even with `--force`. Diagnostics go to stderr and include
server code, SQLSTATE and input location when available. No SQL is retried after a
lost connection; the outcome of a write can be unknown.

| Exit status | Meaning |
| --- | --- |
| 0 | Successful batch, or normal interactive exit |
| 1 | SQL, authentication, connection, input or output failure |
| 2 | Invalid parameters or configuration |
| 130 | Interrupted batch or forced interrupt |

## TLS and local files

`--ssl-mode=REQUIRED` is the default: encryption is mandatory, and the server
certificate is not verified. This works with MyDB's default ephemeral self-signed
certificate. Authentication or TLS failure never triggers a plaintext retry.

| Mode | Meaning |
| --- | --- |
| `DISABLED` | Explicit plaintext TCP |
| `REQUIRED` | Require encryption without certificate verification |
| `VERIFY_CA` | Require encryption and a certificate chain trusted by `--ssl-ca` |
| `VERIFY_IDENTITY` | Also verify that the certificate covers the requested host |

```sh
mydb-cli -hdb.example.com -uapp -p appdb \
  --ssl-mode=VERIFY_IDENTITY --ssl-ca=/path/to/ca.pem
```

`--ssl-cert` and `--ssl-key` must appear together for client certificate
identification. TLS certificate options conflict with `DISABLED`; `--ssl-ca`
requires a verification mode. `--connect-timeout` defaults to 10 seconds;
`--read-timeout` and `--write-timeout` are optional positive seconds. Query
cancellation uses separate three-second control-connection timeouts.

`LOAD DATA LOCAL INFILE` is disabled by default and produces an explicit client
error if requested. Enable it with `--local-infile=1`. The handler opens the
requested local path and streams its actual contents; file errors fail execution.
Relative upload paths are resolved from the client's working directory.

## Configuration and parameter reference

The client reads only its dedicated INI file: `~/.mydb-cli.cnf` on Unix, or
`%APPDATA%\mydb-cli\config.ini` on Windows. `--config PATH` replaces that path;
`--no-config` disables file loading. A missing default file is allowed; an explicit
missing file is an error. The only accepted section is `[mydb-cli]`:

```ini
[mydb-cli]
host=127.0.0.1
port=8738
user=app
database=appdb
ssl-mode=VERIFY_IDENTITY
ssl-ca=/path/to/ca.pem
connect-timeout=10
```

Precedence is defaults, configuration file, `MYDB_CLI_*` environment variables,
then command line. Environment names uppercase the long option and replace `-`
with `_`, for example `MYDB_CLI_PORT`, `MYDB_CLI_PASSWORD`, and
`MYDB_CLI_SSL_MODE`. Unknown/duplicate keys, unknown sections and malformed values
fail. Values may be single/double quoted; quotes are removed without shell
expansion or backslash interpretation. `#` and `;` introduce comments only at the
start of a line, so passwords retain these characters.

| Options | Purpose |
| --- | --- |
| `-h/--host`, `-P/--port`, `-u/--user` | TCP endpoint and user |
| `-D/--database`, positional `DATABASE` | Select a database; mutually exclusive |
| `-p`, `-pVALUE`, `--password[=VALUE]` | Prompt or supply a password |
| `-e/--execute` | Execute SQL; repeatable |
| `-B/--batch` | Disable interactive behavior |
| `-N/--skip-column-names`, `-r/--raw` | TSV header/escaping controls |
| `-t/--table`, `-E/--vertical`, `-q/--quick` | Result display |
| `--binary-as-hex[=true|false]` | Binary value display |
| `-f/--force`, `--show-warnings`, `--delimiter` | Script behavior |
| `--connect-timeout`, `--read-timeout`, `--write-timeout` | Timeouts in seconds |
| `--ssl-mode`, `--ssl-ca`, `--ssl-cert`, `--ssl-key` | TLS policy and files |
| `--local-infile=0|1` | Local upload permission |
| `--no-history`, `--history-file`, `--skip-auto-rehash` | Interactive behavior |
| `--config`, `--no-config` | Configuration source |
| `-?/--help`, `-V/--version` | Help and version |

Configuration and environment settings support the connection, output, execution,
TLS and interactive options above, excluding `execute`, config selection, help and
version. Boolean options accept `--option` or `--option=true|false`.

## Compatibility boundaries

The initial client connection uses `utf8mb4`. SQL mode changes are tracked through
server response metadata, including `ANSI_QUOTES` and `NO_BACKSLASH_ESCAPES`.
Keep session tracking enabled for correct splitting after SQL-mode changes.
This is a MySQL classic-protocol client, not MySQL Shell or a dump/backup tool.
The first version does not implement MySQL option files, automatic reconnection,
Unix sockets, named pipes, pager, tee, external editors, system commands, protocol
compression options or alternative input encodings. Unsupported options and local
commands produce errors. SQL support remains determined by the server.
