# MyDB 1.0.2 — Public Preview

## Downloads

| Platform | Archive |
| --- | --- |
| macOS Apple Silicon | [mydb-1.0.2-macos-arm64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-macos-arm64.tar.gz) |
| Linux x86_64 (glibc 2.17 baseline) | [mydb-1.0.2-linux-x86_64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-linux-x86_64.tar.gz) |
| Windows x64 | [mydb-1.0.2-windows-x86_64.zip](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-windows-x86_64.zip) |

[Download SHA256SUMS](https://github.com/yzsind/mydb/releases/download/v1.0.2/SHA256SUMS) to verify the archives.

Each archive includes `mydb`, `mydb-cli`, `mydb-backup`, the Apache-2.0
license, third-party notices, and required corresponding MPL source archives.
Windows executables require compatible Visual C++ and Universal CRT runtimes.

## Overview

MyDB 1.0.2 delivers substantial improvements in **Zone Map pruning, HNSW vector
search, and transaction throughput, including TPC-C workloads**. It also improves
MySQL compatibility, SQL correctness, and recovery. These notes cover changes
since [MyDB 1.0.1](https://github.com/yzsind/mydb/releases/tag/v1.0.1).

- **Zone Maps:** more selective filters and lower maintenance overhead accelerate
  filtered scans and reduce foreground write work.
- **HNSW:** faster graph navigation, filtering, and reranking improve vector
  retrieval, especially when combined with business filters.
- **Transactions:** general-purpose scheduling, locking, and WAL improvements
  substantially improve TPC-C performance and concurrent OLTP throughput.

The release groups changes into **6 enhancements, 61 bug-fix groups, and 24
performance optimizations**; fixes and performance improvements can overlap.

**Release maturity** — 1.0.2 remains a public preview for feature validation,
compatibility testing, and controlled-environment evaluation.

## MySQL compatibility and SQL correctness

- Hexadecimal and bit literals retain numeric semantics through expressions,
  comparisons, conditional functions, subqueries, and prepared parameters.
  `_binary` literals retain byte-string semantics.
- Temporal numeric conversions, unsigned casts, `GREATEST`/`LEAST`, NULL week
  modes, and user-variable result types behave consistently.
- Parsing and binding fixes cover `BETWEEN`, interval expressions, window frames,
  decimal aliases, duplicate table aliases, derived columns, parenthesized
  INSERT sources, and recursive CTE limits.
- `TEXT(M)` sizing respects the effective character set. Invalid client-encoded
  text literals return an encoding error; raw bytes remain supported through
  `_binary` and hex literals. PyMySQL callers binding bytes should enable
  `binary_prefix=True`.
- Unsupported cast targets and foreign-key `MATCH FULL`/`MATCH PARTIAL` return
  explicit errors instead of silently changing semantics.
- MySQL `GROUP BY ... WITH ROLLUP` returns no rows on empty input. Native
  `ROLLUP`, `CUBE`, and `GROUPING SETS` preserve explicit empty grouping sets.

See [MySQL compatibility](mysql80-compatibility.md) and
[SQL syntax capabilities](sql-parser-capabilities.md).

## Views, routines, and table definitions

- View CHECK OPTION checks the final row after BEFORE triggers and generated
  columns. REPLACE rejects deletion of conflicting rows outside the view.
- Computed view columns remain readable while real columns can be updated.
  Multi-table DML accepts non-updatable views used only as read sources.
- Stored procedures retry only the blocked SQL child, preserving earlier work.
  PREPARE, cursors, typed assignments, TIMESTAMP results, and routine diagnostics
  receive correctness fixes.
- Read-only stored functions work on physical replicas without failing the
  instance. Routine status commands enumerate visible routines across schemas.
- DDL fixes cover CHECK names, visible columns, reserved and generated index
  names, index prefixes, expression defaults, NULL validation, and spatial indexes.
- Foreign-key column renames preserve enforcement and cascades. Partition
  exchanges requiring unsupported cross-generation relocation are rejected
  before changing the catalog.

## Query, Zone Map, and vector performance

- Compiled ordered-range plans reuse bound execution paths; LIMIT-bounded index
  reads stream candidates without materializing the full range.
- Index nested-loop joins filter candidates before decoding output and use
  secondary-index intersections when beneficial. Compiled UPDATE and eligible
  COPY operations avoid repeated expression and row-decoding work.
- Zone Map membership uses complete binary values, improved Bloom layouts, and
  bounded DML deltas. Background rebuilding budgets work and revisits hot zones
  while retaining MVCC-safe pruning.
- Filtered vector Top-K can use scalar indexes to narrow scoring. HNSW combines
  predicate reads and exact reranking, reduces unnecessary graph-distance work,
  and uses compact row selections.
- SQ8 distance kernels reuse query preparation and use AVX-512 VNNI on capable
  x86_64 CPUs. Wide-vector accumulation and high-degree graph construction
  preserve correct distances and connectivity.
- Vector results honor SQL ORDER BY in every iterative mode. EXPLAIN exposes
  scalar access paths, navigation work, and reranking metrics.

See [optimizer and execution](optimizer-and-operators.md),
[vector search](vector-search.md), and [vector indexes](vector-indexes.md).

## Transaction concurrency, recovery, and replication

- Catalog waits no longer stall sibling connections and respond to cancellation.
  Statistics publication preserves concurrent DDL and allows unrelated reads.
- Transaction completion can progress under execution-admission pressure.
  Snapshot registration, metadata locks, and row-lock bookkeeping reduce shared
  contention. Admission also accounts for concurrent analytical work.
- WAL/group-commit coordination reduces notifications and speculative segment
  preparation I/O. Relaxed commits drain WAL to the OS page cache before
  acknowledgement; OS-crash and power-loss durability still require synchronization.
- XA state checks, administrative implicit commits, and `sql_log_bin` transaction
  restrictions preserve transaction boundaries.
- Recovery cleans orphaned vector files, retains retryable purge work, and
  preserves replication slot retention floors.

See [MVCC and locking](mvcc-and-locking.md),
[backup and recovery](backup-and-recovery.md), and
[native replication](native-replication.md).

## Protocol, security, and full-text search

- Compressed protocol limits apply to logical messages within a block. Connection
  idle timers respect changed deadlines.
- Credential redaction handles leading comments and credential-bearing errors.
  New Unix audit files use mode `0600`; audit write failures are reported.
- Keyring calls require global EXECUTE. FLUSH PRIVILEGES resets temporary
  failed-login locks; FLUSH HOSTS leaves account locks intact.
- Boolean full-text parsing and index hints better match MySQL behavior.
  Read-only system variables expose defaults captured by new full-text indexes.
  SHOW VARIABLES values retain text metadata for client decoding.
- Index retirement removes failure sidecars. Temporary spill exhaustion has a
  structured diagnostic, and result-checking tools support explicit relative
  floating-point tolerance.

See [full-text search](fulltext-search.md) and
[security deployment](security-deployment.md).

## Compatibility and evaluation boundaries

The default isolation level remains READ COMMITTED. REPEATABLE READ uses snapshot
isolation without InnoDB gap or next-key locks. MySQL replication requires ROW/FULL;
native promotion requires external fencing. Other existing preview boundaries
remain documented in [MySQL compatibility](mysql80-compatibility.md) and
[feature scope](feature-overview.md).

## Release validation

The `v1.0.2` tag points to
[`6975dae`](https://github.com/yzsind/mydb/commit/6975dae73b3b255dcaa68474f7f079f11dfc3fd1).
All three packages use Rust 1.95.0, locked dependencies, and the repository's
release profile. Extracted packages passed native checks for hashes, versions,
help, TLS, typed SQL, rollback, crash recovery, and secondary-index reads.
Linux binaries passed the glibc 2.17 ABI check.

[Packaging and native verification](https://github.com/yzsind/mydb/actions/runs/44896680713)
passed. The code baseline also passed
[server CI](https://github.com/yzsind/mydb/actions/runs/44893485332),
[CLI CI](https://github.com/yzsind/mydb/actions/runs/44893485329), and
[Safety tests](https://github.com/yzsind/mydb/actions/runs/44893485348);
the release commit adds only documentation to that baseline.

## Documentation, feedback, and license

- [Documentation](README.md) and [issue reporting](reporting-issues.md)
- [Source changes since 1.0.1](https://github.com/yzsind/mydb/compare/v1.0.1...v1.0.2)
- [Report an issue](https://github.com/yzsind/mydb/issues)
- [Apache License 2.0](../LICENSE)

## Moving from 1.0.1 to 1.0.2

**Use a fresh data directory.** The Zone Map file format advances from v6 to v8.
A 1.0.2 binary cannot reuse a 1.0.1 data directory or physical backup. Native
replication does not provide a cross-format upgrade path.

Keep the original binary, keys, and a verified backup. Export logical schema and
data, load them into a fresh 1.0.2 instance, and validate application queries,
transactions, indexes, and privileges before switching writes. Recreate native
replicas from the migrated primary and plan rollback before accepting new writes.

See [changing versions](changing-versions.md) and the
[storage format registry](storage-format-versions.md).

## Critical availability fix

- **HNSW maintenance interrupted by session cancellation.** In earlier versions,
  `KILL`, `KILL QUERY`, or a statement timeout during post-commit HNSW graph/index
  maintenance could stop the entire database from serving requests.
  **The process remained running, but a database restart was required to restore
  service.** MyDB 1.0.2 isolates required post-commit maintenance from statement
  cancellation and timeouts. Thanks to [**eSun-ws**](https://github.com/eSun-ws)
  for reporting this issue.
- **Slow-disk bulk writes blocking logins and queries.** On very slow disks, a
  session writing large amounts of data could also block new connection logins
  and queries from other sessions. **Service recovered only after the blocking
  disk writes completed.** MyDB 1.0.2 improves catalog locking and network
  scheduling to prevent these I/O waits from stalling unrelated connections.
  Writes themselves remain subject to disk performance. Thanks to
  [**hottotti999-cloud**](https://github.com/hottotti999-cloud) for reporting this
  issue.
