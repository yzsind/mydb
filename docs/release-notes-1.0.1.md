# MyDB 1.0.1 — Public Preview

MyDB 1.0.1 improves MySQL compatibility, SQL correctness, and replication
recovery. It adds an optional loopback HTTP listener alongside REST and MCP
HTTPS, and low disk space protection for persistent instances on Linux and
macOS. It also improves query planning, scans, vector processing, and prepared
writes. These notes cover changes since
[MyDB 1.0.0](https://github.com/yzsind/mydb/releases/tag/v1.0.0).

**Release maturity** — 1.0.1 is a public preview for feature validation,
compatibility testing, and evaluation in controlled environments.

## Moving from 1.0.0 to 1.0.1

**Use a fresh data directory.** The WAL format advances from 40 to 41 for the
native replication visibility fix. A 1.0.1 binary cannot open a 1.0.0 data
directory or restore its physical backups or archived WAL.

- **Native MyDB replication** moves from protocol v3 to v4. Both endpoints
  require matching protocol and physical formats. Rebuild replicas from the
  migrated 1.0.1 primary.
- **MySQL-to-MyDB replication** moves from relay record format v1 to v2. Set up
  fresh channels and relay logs on 1.0.1. Initialize each channel from a
  consistent copy with the corresponding source file and position or GTID
  state. Keep old relay files with the 1.0.0 instance.

Keep the original data directory, keys, and a verified backup with the 1.0.0
binary. Export schema and data from the old instance, then reload into a fresh
1.0.1 instance and validate data, indexes, permissions, and representative
application queries before switching writes. Recreate instance objects such as
users and grants as needed. Plan rollback before cutover, because writes accepted
by 1.0.1 cannot be replayed back to 1.0.0 using physical WAL.
See the [version change guide](changing-versions.md) and
[storage format registry](storage-format-versions.md).

Review the new disk space requirements before first startup. Check schema
definitions and driver collation settings for unsupported UCA 9.0 variants
before importing or connecting. Also check callers that depend on string types
from `SELECT @@variable`, permissive variable assignments, or whitespace when
invoking built-in functions.

## MySQL compatibility

- **Function names and precision.** Keyword functions such as `CURRENT_USER`,
  `CURRENT_TIMESTAMP`, and `UTC_TIMESTAMP` now resolve in bare and parenthesized
  forms where MySQL permits both. Whitespace, comments, quoting, and schema
  qualification determine whether lexer-sensitive names resolve as native
  functions, identifiers, or stored routines. Temporal precision is validated
  during parsing, including `PREPARE`.
- **Connection SQL mode.** The MySQL `CLIENT_IGNORE_SPACE` capability enables
  `IGNORE_SPACE` at login and on change-user. Resetting the connection restores
  the global SQL mode. The `ANSI` mode also implies `IGNORE_SPACE`.
- **System variable results.** `SELECT @@variable` returns SQL values with the
  corresponding numeric or text type, including prepared statement metadata.
  For example, boolean variables return `0` or `1` in expressions.
  `SHOW VARIABLES` continues to return display text. Variable definitions also
  enforce their supported scopes, value ranges, defaults, and privileges more
  closely.
- **Session settings.** New or reset sessions inherit applicable global
  settings. `long_query_time` and `min_examined_row_limit` have session values
  used by the slow query log. Restricted session values require the applicable
  variable administration privilege. `default_collation_for_utf8mb4` now sets
  the default connection collation for `SET NAMES utf8mb4` when `COLLATE` is
  omitted. `SET @user_variable` resolves identifiers as SQL expressions and
  preserves temporal fractional precision.
- **Binlog variable checks.** Binlog settings validate scope and restrictions in
  transactions, routines, and sessions with open temporary tables where
  applicable. MySQL replication still requires `ROW` and `FULL`.
- **Character sets and collations.** `character_set_client=binary` preserves
  input bytes in SQL literals and prepared statements. The implemented UCA 9.0
  collations are `utf8mb4_0900_ai_ci` and `utf8mb4_0900_bin`. Other registered
  UCA 9.0 variants now raise error 1235 when selected by name or handshake ID
  and are marked as unavailable in collation metadata.
- **Prepared execution.** A failed `COM_STMT_EXECUTE` discards the accumulated
  long data for that statement. Prepared reads with blocking functions such as
  `SLEEP` and `GET_LOCK` run away from the connection reactor, including when
  those functions occur inside table expressions. Prepared inserts rebind
  positional parameters after a schema change and revalidate cached plans
  against the current session settings.
- **Connection attributes.** MySQL retains up to 512 bytes and 64 name/value
  pairs per connection under its memory budget. Truncation is reported by
  `Performance_schema_session_connect_attrs_lost`.

See [MySQL 8.0 compatibility](mysql80-compatibility.md) and
[SQL syntax capabilities](sql-parser-capabilities.md).

## SQL correctness and connection fixes

- `ALTER TABLE ... MODIFY COLUMN` and `CHANGE COLUMN` can update a column
  comment in the same atomic operation when a supported table copy is needed.
  The column change and comment are published together. [Issue #3](https://github.com/yzsind/mydb/issues/3)
- Index nested-loop joins retain the correct projected columns after columns are
  inserted with `FIRST` or `AFTER`, moved, or dropped. This fixes spurious
  unknown-column errors on affected plans. [Issue #5](https://github.com/yzsind/mydb/issues/5)
- An unqualified `GROUP BY` name can bind to a uniquely selected source column
  when the joined inputs have more than one column with that name. Genuine
  ambiguity still raises an error. [Issue #6](https://github.com/yzsind/mydb/issues/6)
- Client disconnects on Linux and macOS can signal cancellation even while the
  connection is executing an inline query. When the statement unwinds, the
  session rolls back uncommitted work and releases locks.
  [Issue #8](https://github.com/yzsind/mydb/issues/8)
- SQL result handling preserves the bound decimal scale for division and certain
  temporal arithmetic, and preserves fractional precision when coercing temporal
  `CASE` and `IF` results. Decimal division also uses the declared result scale
  at `CAST`, `CONCAT`, `UNION ALL`, and `SELECT INTO OUTFILE` boundaries.
- SQL comparisons and results are consistent across more execution paths. Hash
  joins between character and binary values preserve the character source bytes.
  Row-based and streaming grouping respect the column collation. Fast filters
  on `DOUBLE` avoid double rounding of `DECIMAL` constants.
- Predicate pushdown and equality inference use the correct schema and aliases
  for derived tables and CTEs, including names that shadow base tables. `GROUP BY`
  retains the derived column name in result metadata.
- `ROUND` and `TRUNCATE` convert binary strings as numeric text.
  `CAST('0' AS YEAR)` returns 2000, while numeric `CAST(0 AS YEAR)` retains zero.
- Index-range writes and locking reads handle candidates affected by uncommitted
  deletes or key changes. Unique-key locking reads honor `NOWAIT` and
  `SKIP LOCKED` when another transaction holds the key reservation.
- Parallel `LOAD DATA` enforces row-level security `INSERT WITH CHECK`.
  Non-ASCII `LOAD DATA` syntax is handled without a parser panic, and aliases
  ending in `x` are recognized when checking for hex literal syntax.

## REST and MCP

- Set `rest_loopback_http_addr` (or `MYDB_REST_LOOPBACK_HTTP_ADDR`) to a local
  IP and port such as `127.0.0.1:8739` or `[::1]:8739`. This adds a plaintext
  listener while the primary listener remains in `rest_tls_mode=required`.
  It works with REST SQL, MCP, or both. The listeners share resource limits.
  Persistent SQL sessions stay on their original listener and API key, and
  authenticated operations still honor global and account TLS requirements.
  [Issue #4](https://github.com/yzsind/mydb/issues/4)
- When MCP has completed a statement but cannot retain its result rows, the
  response marks the rows as omitted and preserves the execution outcome,
  transaction state, and request handle. Clients can use that outcome to decide
  whether a retry is safe. [Issue #32](https://github.com/yzsind/mydb/issues/32)

See [REST listener configuration](rest-api.md#serve-local-http-alongside-https)
and [MCP](mcp.md).

## Query and vector performance

- Join planning avoids repeated work when comparing candidate join orders and
  binding predicates. Join costs better reflect the width of projected data.
  Validation also reuses collation analysis within complex expressions.
- Eligible repeated full heap scans can reuse buffer pool pages without evicting
  frequently accessed pages as aggressively. Sequential read-ahead drops requests
  the scan has already passed. Eligible B-tree sweeps batch page reads, and
  background Zone Map maintenance admits pages without promoting them to hot
  entries.
- IVF centroid training uses SIMD accumulation on ARM64 and x86_64 while
  preserving the order of additions for each coordinate. Native distance
  kernels improve decoding of half-precision vector blocks, including blocks
  with unaligned storage.
- Eligible prepared inserts bind parameters directly to full rows. Prepared
  point updates reuse the existing row lock and parse the row layout once per
  patch. Small inserts can reuse routed B-tree pages while preserving the
  order in which uniqueness conflicts are handled.
- With a zero group-commit window, the first committer can run an idle WAL
  flush directly, saving a thread handoff. Other committers still wait for their
  WAL position to become durable. Serial statements also defer allocating shared
  rows-examined counters until execution needs them.
- On macOS, native replica initialization groups device flushes while building
  the staged base backup and synchronizes it before installing it.

## Disk space protection

- Persistent read-write instances on Linux and macOS reserve 1 GiB of physical
  recovery space by default. The data and WAL directories must share a filesystem
  that supports physical allocation. `disk_recovery_reserve_bytes` controls the
  reserve size (`0` disables protection), and `disk_write_min_free_bytes`
  controls write headroom. The default headroom is two WAL segments plus 32 MiB.
- The server accounts for free blocks, free inodes, and pending dirty pages.
  When headroom is low, it rejects new ordinary writes before admission, blocks
  native replica apply, and constrains query spills on the protected volume.
  Consistent reads and completion of admitted transactions remain
  available while the storage is healthy. An administrator can purge a single
  recycle-bin entry or permanently drop an eligible table when the operation
  fits the available cleanup budget. Binlogged cleanup synchronizes the binary
  log before committing the referencing WAL record, and startup can reconcile
  an interrupted binlog rotation.
- Startup can release the reserve before WAL recovery and keep writes paused
  until the reserve is rebuilt. `SHOW GLOBAL STATUS` exposes
  `Mydb_disk_write_limited`, `Mydb_disk_available_bytes`, and
  `Mydb_disk_recovery_reserve_armed`.
- A fail-stopped server accepts authorized `KILL`, `SHUTDOWN`, and `RESTART`
  commands and preserves the existing recovery point by skipping the final
  checkpoint. Make space available before restarting after a disk-full failure.

See [low disk space configuration](configuration.md#low-disk-space).

## Crash recovery and replication

- Startup recovers valid WAL segments left pending by rotation in relaxed
  durability mode. Archive and recovery bookkeeping use the last valid record
  boundary of a preallocated WAL segment, so unused space does not prevent
  startup under an archive lease.
- Native replicas can advance transaction visibility across abandoned commit
  sequence numbers after source recovery or a fenced promotion. The source
  records the visibility information in WAL so a replica can expose already
  applied commits without waiting for new transactions. Ordinary successful
  commits require no extra WAL record for this change.
- When native replica apply retries a commit after a metadata lock conflict,
  it also retries catalog publication and refreshes users and grants.
- Native base backup sends progress every five seconds during capture, allowing
  capture to run beyond the replica's 60-second silence timeout. Disconnects
  cancel capture at the next cancellation check. Protocol v4 streams large file
  manifests in bounded frames, allowing the full manifest to exceed the 64 MiB
  per-frame limit.
- MySQL replica recovery matches a row-transaction receipt against both the
  source UUID and the transaction boundary before skipping replay. This keeps
  receipts from a previous source from suppressing rows from its replacement.
  `START REPLICA UNTIL SQL_AFTER_GTIDS` stops at the requested boundary, including
  when it falls within a batch of pending relay records.
- The MySQL receiver releases table-map metadata at statement boundaries. It
  allows up to 4,096 active table maps per statement, subject to the shared
  memory budget, and reports a limit error if admission fails.

See [backup and recovery](backup-and-recovery.md),
[native replication](native-replication.md), and
[MySQL 8.0 compatibility](mysql80-compatibility.md).

## Compatibility and evaluation boundaries

- The default isolation level is `READ COMMITTED`. `REPEATABLE READ` uses
  snapshot isolation without InnoDB gap or next-key locks. Evaluate application
  transaction semantics before migrating.
- Native replication requires external fencing before promotion. Time travel
  requires retained undo history, and the default retention window is zero.
- MySQL statement-based and mixed replication, `MINIMAL` row images, and MySQL
  multithreaded replica apply remain unsupported. The MySQL X Protocol, Group
  Replication, Router, Shell/X, plugin ABI, `HANDLER`, `RESOURCE GROUP`, and
  `TABLESPACE` are outside the preview's supported scope.
- Windows evaluation targets foreground operation on local NTFS. See the
  [platform requirements](hardware-and-os-requirements.md) for details.
- The reported intermittent stalls during sustained single-connection bulk
  inserts on Linux remain under investigation. Follow
  [issue #7](https://github.com/yzsind/mydb/issues/7) for updates.

## Benchmark result validation

The source-tree TPC-H and TPC-DS checkers automatically load an expected
directory's `comparison.json` unless an explicit manifest is supplied. TSV
contracts can declare separate result-set boundaries and column types, with
explicit numeric or text normalization for historical fixtures. The standalone
TPC-DS checker reports malformed results per query, checks the remaining
queries, and exits with a failure status. The historical TPC-H references
compare decimal and floating-point values at four decimal places and trim text
under their declared contracts.
See the [result checker documentation](../scripts/README.md).

## Release validation

The release is built from the `v1.0.1` source tag using Rust 1.95.0,
locked dependencies, the default server features, and the repository's release
profile. The release checks cover server and CLI CI, recovery and replication
safety tests, and native verification of extracted packages. Package checks
cover file hashes, binary versions and help, required TLS, typed SQL,
transaction rollback, crash recovery, and secondary index reads. Linux packages
also require the glibc 2.17 ABI check. The
[GitHub Release](https://github.com/yzsind/mydb/releases/tag/v1.0.1) records the
source commit and validation results.

## Release packages

The GitHub Release provides the following archives and `SHA256SUMS`.

| Platform | Archive | Native validation environment |
| --- | --- | --- |
| macOS Apple Silicon | `mydb-1.0.1-macos-arm64.tar.gz` | macOS ARM64 |
| Linux x86_64 (glibc 2.17 baseline) | `mydb-1.0.1-linux-x86_64.tar.gz` | Ubuntu 24.04 x86_64 |
| Windows x64 | `mydb-1.0.1-windows-x86_64.zip` | GitHub `windows-2022` x64 runner |

Each runtime archive includes `mydb`, `mydb-cli`, `mydb-backup`,
the Apache-2.0 license, third-party license texts, and required corresponding
MPL source archives. Windows executables use the `.exe` suffix and require
compatible Visual C++ and Universal CRT runtimes.

## Documentation, feedback, and license

- [Documentation](README.md) and the new [Simplified Chinese project README](../readme_cn.md)
- [Feature overview](feature-overview.md) and [issue reporting guide](reporting-issues.md)
- [Source changes since 1.0.0](https://github.com/yzsind/mydb/compare/v1.0.0...v1.0.1)
- [Report an issue](https://github.com/yzsind/mydb/issues) with the exact version,
  OS, relevant logs, and reproducible SQL or scripts.
- [Apache License 2.0](../LICENSE)
