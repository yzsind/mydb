# MyDB 1.0.2 — Public Preview

## Release highlights

MyDB 1.0.2 delivers substantial performance improvements in analytical scans,
vector retrieval, and transaction processing.

| Change category | Documented change groups |
| --- | ---: |
| Features and enhancements | **6** |
| Bug fixes | **61** |
| Performance optimizations | **24** |

Counts group related changes by capability, corrected behavior, or optimization
mechanism; they are not counts of commits, issues, or tests. The six enhancements
include five product capabilities or observability improvements and one
developer-tool enhancement. A fix may also improve performance, so category
counts are not additive. Retained features and documentation-only changes are
excluded.

The three main advances are:

- **Faster Zone Map pruning.** More selective binary membership filters,
  improved Bloom layouts, and bounded maintenance reduce unnecessary heap
  scanning and foreground write overhead. These changes accelerate filtered
  queries while preserving MVCC visibility and exact SQL predicate checks.
  See [query execution and Zone Maps](#query-execution-and-zone-maps).
- **Faster HNSW vector search.** Improved graph navigation, fused filtering and
  reranking, and optimized distance kernels reduce candidate-processing work.
  Scalar indexes also narrow eligible filtered Top-K searches, improving
  retrieval performance when vector similarity is combined with business
  filters. See [vector search and index lifecycle](#vector-search-and-index-lifecycle).
- **Higher transaction throughput, including TPC-C.** This release substantially
  improves TPC-C performance through general-purpose improvements to transaction
  scheduling, lock release, catalog access, and WAL/group-commit coordination.
  These improvements reduce contention and commit overhead for concurrent OLTP
  applications. See [transaction concurrency and durability](#transaction-concurrency-catalog-access-and-durability).

## Overview

MyDB 1.0.2 improves SQL correctness, transaction concurrency, MySQL protocol
handling, query execution, and filtered vector search. It also introduces a
new Zone Map format with more selective binary membership filters and bounded
foreground maintenance.

These notes cover changes from the `v1.0.1` source tag through commit
[`c644d7a`](https://github.com/yzsind/mydb/commit/c644d7ab2de06a03b8f2e1e91068b9a032f0082b),
the source baseline used to prepare the `1.0.2` branch on November 21, 2026.
See the [1.0.1 release notes](release-notes-1.0.1.md) for the previous release.

**Release maturity:** 1.0.2 remains a public preview for feature validation,
compatibility testing, and evaluation in controlled environments.

**Upgrade requirement:** the Zone Map file format changes from v6 to v8.
Existing 1.0.1 data directories, physical backups, and native replicas are not
physically compatible with this release. Use a fresh data directory and a
validated logical schema/data transfer. See [Upgrading from 1.0.1](#upgrading-from-101).

## SQL expressions, parsing, and character sets

- **Hexadecimal and bit literals.** Numeric contexts now preserve their unsigned
  numeric meaning through arithmetic, comparisons, `IN`, `BETWEEN`, `CASE`,
  `IF`, `NULLIF`, boolean tests, and numeric function arguments. Wrapped
  expressions, scalar subqueries, and prepared bounds retain the distinction
  between a numeric literal and an ordinary binary string. For example,
  `0xFF + 1` returns 256, and `0xFF BETWEEN 200 AND 300` is true.
- **Binary-string operations.** `_binary` literals retain string semantics;
  bitwise operators operate on bytes and reject incompatible operand lengths.
  Character-set introducers preserve their source encoding during conversion,
  including `CONVERT(_latin1 X'E9' USING utf8mb4)`.
- **Temporal and numeric conversion.** Temporal function results such as
  `DATE_ADD` and `ADDTIME` convert to calendar numbers in numeric contexts.
  Negative floating-point values cast to `UNSIGNED` through signed integer
  conversion. `GREATEST` and `LEAST` handle temporal operands and explicit
  collations consistently. `WEEK` and `YEARWEEK` accept a NULL mode using their
  default-mode behavior. User-variable assignments expose the assigned value's
  result type.
- **Predicate and interval parsing.** `BETWEEN ... AND ... IS ...` binds the
  boolean test to the completed predicate. Invalid lower-bound expressions,
  stray `NOT NULL`, misplaced select modifiers, malformed date intervals, and
  reversed window-frame categories produce explicit errors. Decimal literals
  immediately followed by aliases retain every projection, including wide
  select lists.
- **Cast targets.** `CAST` and typed `CONVERT` reject column-only targets such
  as `TEXT`, `VARCHAR`, `VARBINARY`, and `BLOB`, along with other unsupported
  targets, rather than silently mapping them to a different type. Use supported
  targets such as `CHAR`, `BINARY`, and `SIGNED`. `INT`, `INTEGER`, and `BIGINT`
  remain native signed 64-bit aliases; native vector casts remain available.
- **Query scopes and names.** Duplicate table references and aliases are checked
  in their query scope, including CTE references and nested joins. Derived
  outputs may have repeated names: positional results and unambiguous
  projections work, while an ambiguous column reference returns error 1052.
- **Query forms.** Parenthesized `INSERT ... SELECT`, scalar subqueries in
  `VALUES` and `INSERT ... SET`, and `VALUES ... ORDER BY` preserve their query
  semantics. Recursive CTEs honor their own `LIMIT` and `OFFSET`, including
  zero limits, and enforce recursion depth across multiple recursive branches.
  Standalone `OFFSET`, optional derived-table aliases, `VALUES` rows without
  `ROW`, leading empty statements, and plural `EXTRACT` units remain supported
  extensions.
- **Text encoding.** Invalid client-encoded text literals return an encoding
  error instead of losing invalid bytes or becoming binary implicitly. Inert
  comments do not require valid client-charset encoding; NUL inside a line
  comment is rejected. Use `_binary` or hex literals for raw octets. PyMySQL
  text-protocol callers binding `bytes` or `bytearray` should set
  `binary_prefix=True`.
- **Text type sizing and session charset.** `TEXT(M)` chooses its storage class
  using the effective character set, including inherited defaults, temporary
  tables, and `ALTER TABLE`. `SET CHARACTER SET` derives the connection charset
  and collation from the current database. Combined charset assignments and
  dotted user-variable names are handled correctly.
- **Empty grouping sets.** MySQL-compatible `GROUP BY ... WITH ROLLUP` returns
  no rows for empty input. Native `ROLLUP(...)`, `CUBE(...)`, and
  `GROUPING SETS (...)` preserve each explicit empty grouping set, with
  `COUNT(*) = 0` and `SUM(...) IS NULL`.

See [MySQL compatibility](mysql80-compatibility.md),
[SQL syntax capabilities](sql-parser-capabilities.md), and
[operators and expressions](operators-and-expressions.md).

## Views, stored routines, and diagnostics

- **View checks use the final row.** `WITH CHECK OPTION` evaluates the image
  after BEFORE triggers and generated-column evaluation, including
  `INSERT IGNORE` filtering. `REPLACE` checks every conflicting unique row and
  rejects deletion of rows outside the view with error 3059, preserving
  statement atomicity.
- **Computed view columns.** Computed projections remain readable in DML
  predicates; attempts to assign to them return error 1348. An explicit insert
  list containing only real columns can use base-table defaults even when the
  view also projects computed columns. This is a supported extension.
- **Read-side views and maintenance.** Multi-table UPDATE/DELETE can materialize
  aggregate or otherwise non-updatable views used only as read sources.
  `CHECK TABLE` validates view dependencies; `ANALYZE`, `OPTIMIZE`, and `REPAIR`
  report that a view is not a base table. `FLUSH TABLES` accepts view names.
  View expansion has explicit limits of 32 expanded view levels and 16
  residual non-mergeable query levels.
- **Routine lock waits.** A blocked atomic SQL child inside `CALL` is retried
  without repeating already completed children. This applies in autocommit and
  explicit transactions.
- **Prepared SQL and cursors.** SQL `PREPARE` inside procedures uses session-level
  semantic validation and result metadata. Cursor names and FETCH variables
  are checked at routine creation. FETCH arity is checked before consuming a
  row, including when the cursor is empty.
- **Routine values and visibility.** Parameter/local assignments use their
  declared types and applicable SQL mode. Routine result sets localize
  TIMESTAMP values using the session time zone. Read-only stored functions run
  on physical replicas; attempted writes return a read-only error without
  failing the instance. `SHOW PROCEDURE STATUS` and `SHOW FUNCTION STATUS`
  enumerate visible routines across schemas independently of the current
  database.
- **Error handling.** `SIGNAL` preserves an explicitly supplied error number
  and message; `RESIGNAL`, origin fields, and handled warnings retain their
  diagnostic context. Procedural parsing rejects block nesting beyond its
  separate 64-level guard.

## Table definitions, constraints, and partition operations

- CHECK names are matched case-insensitively for ALTER/DROP operations.
  Persistent and temporary tables must retain at least one visible column;
  a compound ALTER can drop the last visible column when it also adds one.
- Secondary indexes cannot use the reserved name `PRIMARY`. Automatically
  generated index names avoid reserved names and collisions, including multiple
  additions in one statement. Prefix lengths are validated when creating an
  index and when narrowing an indexed column.
- Expression defaults reject self references and invalid forward dependencies
  on expression defaults. Valid default chains retain evaluation order.
  Changing a populated column to NOT NULL reports invalid NULL data explicitly.
- Invalid object names, floating-point precision declarations, oversized table
  options, and unknown full-text parsers produce errors. Temporary geometry
  indexes retain their spatial index kind.
- `CHANGE COLUMN` updates foreign-key column references on both parent and child
  tables, preserving enforcement and cascades after restart. Compound changes
  that cannot atomically restore cross-table foreign-key metadata are rejected
  before publication. Foreign keys implement `MATCH SIMPLE`; `MATCH FULL` and
  `MATCH PARTIAL` return error 1235.
- `EXCHANGE PARTITION` preserves the storage identities required by native
  replay. Exchange between different physical generations is rejected with
  error 1235 before catalog mutation because durable physical relocation is
  not implemented.
- Eligible physical COPY operations avoid decoding unchanged external-value
  columns when the source generation can prove that no overflow data exists.
  Sources requiring external values retain the full MVCC-aware path.

## Transaction concurrency, catalog access, and durability

- **Connection scheduling.** Catalog contention is admitted asynchronously so
  a waiting command does not pause sibling connections on its reactor. Catalog
  waits observe KILL and disconnect cancellation. Ordinary SQL does not consume
  outbound HTTP admission merely because catalog classification is waiting.
  Eligible compiled autocommit writes can execute inline when their reactor
  has exclusive ownership and spare reactor capacity remains.
- **Transaction completion.** COMMIT, ROLLBACK, and prepared transaction-control
  commands can reach a worker without queuing behind new lock-taking
  statements. Under automatic admission, completion can return its execution
  permit after row-lock release if it first reserves a completion slot. The
  combined worker budget retains capacity for lock-releasing work. Explicit
  `MYDB_TXN_ADMISSION` limits continue through completion.
- **Mixed workloads.** Transaction admission accounts for active parallel reads
  and a bounded grace period between analytical statements. Session parallelism
  budgets constrain hints and cached plans as well as fresh planning decisions.
- **Statistics publication.** Durable statistics updates allow ordinary table
  reads and unrelated descriptor-cache fills to continue. Publication
  serializes with conflicting DDL and rejects stale revisions, preventing lost
  schema changes or resurrection of dropped tables. Cold reads retain the
  stable descriptor until publication completes.
- **MVCC and metadata locks.** Snapshot registration, transaction-slot cleanup,
  metadata-lock access, and row-lock bookkeeping reduce shared contention
  while retaining the snapshot horizon and rollback undo needed by readers.
- **WAL and group commit.** Durability coordination reduces unnecessary worker
  notifications; Linux can coalesce wakes for waiters covered by one barrier.
  WAL segment preparation bounds speculative zero-fill I/O so it competes less
  with the active log, and rollover uses the prepared segment size.
- **Relaxed commits.** A successful relaxed commit drains WAL to the OS page
  cache before acknowledgement, including when commit work is deferred. It
  survives a server process crash, including `SIGKILL`; OS crashes and power
  loss still require the stronger guarantees of an appropriate synchronized
  durability configuration.
- **XA and administrative boundaries.** An IDLE XA branch permits table-free
  queries but rejects table access, including nested subqueries. XA ROLLBACK
  requires XA END first. Server-rolled-back branches retain their rollback
  cause and can release their XID. Replica-management statements perform their
  implicit commit before execution and reject incompatible XA states.
  Changing `sql_log_bin` inside an active transaction or XA branch returns
  error 1694 without changing the setting.
- The default connection reactor count remains four times available CPU
  parallelism, with a minimum of 16. This does not set query parallelism.

See [MVCC and locking](mvcc-and-locking.md),
[startup configuration](configuration.md), and
[WAL resources](configuration-resources.md).

## Query execution and Zone Maps

- **Ordered index ranges.** Eligible ordered range queries reuse compiled plans
  and rebind bounds on each execution. LIMIT-bounded reads stream forward or
  backward through index leaf windows while preserving snapshot visibility,
  reducing repeated planning and unnecessary candidate materialization.
- **Joins and row decoding.** Index nested-loop joins can evaluate selective
  predicates before decoding output columns and use secondary-index
  intersections when their estimated benefit justifies the extra scan. Runtime
  filters retain all columns needed by the SQL residual.
- **Compiled DML and vectorized predicates.** Eligible UPDATE assignments use
  compiled expressions while preserving current-row assignment order and
  savepoints. Typed temporal filters retain microsecond boundaries and
  initialized outputs. Calendar values whose reserved encodings do not match
  scalar ordering use the scalar comparison path.
- **Complete binary membership.** Zone Map equality fingerprints cover the
  entire binary value independently of the bounded prefix used for range
  envelopes. Values sharing long prefixes can therefore be distinguished
  without treating a prefix as an equality identity.
- **Bloom layout and maintenance.** Block-based Bloom filters use improved
  address mixing, occupancy-aware compaction, and capacity based on local
  distinct values. Bounded exact DML deltas limit foreground filter growth;
  background rebuild scheduling budgets physical work and revisits hot zones.
  Partial heap batches are merged before membership compaction.
- **Conservative pruning.** Collation and temporal comparison compatibility
  are checked before exclusion. Tagged calendar values retain their zones when
  ordered bounds cannot represent them safely. DML, rollback, restart, and
  older snapshots continue to use heap residual and MVCC validation.

The Zone Map file version is now **8**, with standard page version **3**
(`zone=8/3`). See the [format registry](storage-format-versions.md) and
[optimizer reference](optimizer-and-operators.md).

## Vector search and index lifecycle

- **Exact filtered Top-K.** The planner can use scalar point/range access and
  covered composite-index bounds to restrict vector scoring. Cost estimates use
  the selected access path's row count. Eligible cosine-distance queries with
  a validated fixed probe can use the exact filtered path without an ANN index.
  Nullable vectors and unsupported probe shapes retain ordinary SQL evaluation.
- **Filtered HNSW.** Candidate selection can fuse heap predicate evaluation
  with exact reranking. Mapped navigation avoids distance work for rejected
  payloads and uses bounded connector traversal. Candidate expansion is bounded
  near convergence while retaining the final SQL predicate and ordering checks.
  Dense page-based row selections reduce membership lookup overhead when they
  are more compact than sparse row IDs or per-page maps.
- **Distance kernels and graph construction.** Mapped SQ8 scoring reuses query
  preparation and supports runtime-dispatched x86_64 AVX-512 VNNI kernels on
  capable CPUs. Wide-vector squared-distance accumulation avoids intermediate
  overflow. Chunked adjacency retains complete high-degree neighbor lists, and
  connectivity repair preserves graph reachability.
- **Result order.** ANN candidate-widening modes, including `relaxed_order`,
  return rows in SQL `ORDER BY` order, including transaction-local vectors.
- **Index syntax and hints.** Vector `WITH (...)` options remain attached to
  their HNSW/IVF index when comments or visibility options intervene. An invalid
  `HNSW_EF(0)` hint emits a warning rather than silently taking effect.
- **Maintenance and recovery.** Cancellation of a statement does not cancel
  vector maintenance already required by its committed writes. Navigation-cache
  rebuilds allow DML to proceed and reject stale epochs at publication. Startup
  removes orphan unpublished manifests and generation directories; index drop
  preserves unrelated directory entries. Purge retains and requeues work after
  resource failures.
- **Explicit failure handling.** Unfinished index maintenance can use the
  supported query fallback; corrupt persistent objects still produce errors.
  Temporary spill exhaustion has the structured diagnostic
  `MYDB-DIAG-TEMP-DISK-FULL`, avoiding classification by message text.
- **EXPLAIN diagnostics.** Exact filtered plans identify their scalar index.
  Analysis reports navigation visits and distances, predicate skips, connector
  work, heap/fused reranks, quality rounds, and exact fallbacks.

See [vector search](vector-search.md) and [vector indexes](vector-indexes.md).
These changes do not establish a release-wide throughput or recall multiplier;
measure with representative data, filters, and search settings.

## MySQL protocol, security, and full-text search

- Compressed protocol blocks can contain several individually valid logical
  messages even when the block exceeds one message's `max_allowed_packet`.
  Limits apply to the assembled logical message. Truncated compressed bodies
  fail before allocating their declared output buffer; protocol allocations
  remain subject to memory admission.
- Connection idle waits reuse their reactor timer and honor both shorter and
  later deadlines. The protocol compatibility matrix has refreshed source
  evidence for this revision.
- Credential statements are recognized behind leading comments and executable
  comment wrappers for log redaction. Credential-bearing parse errors are
  redacted in audit output. Newly created Unix audit files use mode `0600`;
  audit serialization/write failures are counted and the first failure is
  reported, while query execution remains fail-open.
- `KEYRING_*` calls require global `EXECUTE`; a schema-level grant is
  insufficient. `FLUSH PRIVILEGES` resets failed-login counters and temporary
  account locks. `FLUSH HOSTS` leaves account locks intact because MyDB has no
  host cache to clear.
- Boolean full-text search accepts whitespace after leading signs, terminates
  prefix operands at internal `*`, and tolerates an unclosed quoted phrase
  through end of input. Invalid repeated signs still fail. `AGAINST` accepts a
  string literal or prepared parameter and rejects arbitrary expressions and
  implicit numeric/NULL coercions.
- Natural-language MATCH retains its required FULLTEXT index under
  `IGNORE INDEX`. Boolean mode honors JOIN-scope index hints and reports 1191
  when they exclude that index; GROUP BY/ORDER BY hints do not exclude it.
- Read-only global tokenizer variables expose defaults captured by new
  full-text indexes: `innodb_ft_min_token_size`, `innodb_ft_max_token_size`,
  `innodb_ft_enable_stopword`, and `ngram_token_size`. `ft_min_word_len` and
  `ft_max_word_len` expose the same defaults. Existing index tokenizers do not
  change when process defaults change.
- Full-text documentation clarifies partition-postings write costs, visible-text
  heap-oracle searches, and the exclusive barrier used by offline ADD FULLTEXT.
  Concurrent readers and writers may wait or time out during that build.
- `SHOW VARIABLES` values use text collation so clients decode them as strings.
  Index retirement also removes `.failed` and `.corrupt` sidecars for full-text
  and ordinary B-tree indexes. View authorization errors identify the view
  instead of exposing internal base-table privilege details.

See [full-text search](fulltext-search.md),
[security deployment](security-deployment.md), and
[protocol compatibility](mysql80-protocol-compat-matrix.json).

## Replication, validation tools, and packaging

- Native replication preserves persisted replication-slot retention floors
  during storage startup before the slot registry opens. An empty slot directory
  does not impose a floor. Partition-exchange and replica routine regressions
  are covered by the source tests described above.
- TPC-H/TPC-DS result contracts can opt into relative floating-point tolerance
  for ordered, explicitly typed results. Non-floating cells, NULLs, row counts,
  and result-set metadata remain checked. Contracts reject incompatible
  tolerance, rounding, and ordering combinations.
- DuckDB-derived reference queries use native `ROLLUP(...)` to preserve their
  explicit empty grouping-set semantics. Reference manifests and multi-result
  boundaries are updated accordingly. This is validation-tool behavior; the
  engine uses general SQL semantics.
- Server and standalone CLI package versions are `1.0.2`. The runtime archive
  layout continues to include `mydb`, `mydb-cli`, `mydb-backup`, the Apache-2.0
  license, third-party notices, and required corresponding MPL source archives.

## Downloads

| Platform | Runtime archive |
| --- | --- |
| macOS Apple Silicon | [mydb-1.0.2-macos-arm64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-macos-arm64.tar.gz) |
| Linux x86_64, glibc 2.17 baseline | [mydb-1.0.2-linux-x86_64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-linux-x86_64.tar.gz) |
| Windows x64 | [mydb-1.0.2-windows-x86_64.zip](https://github.com/yzsind/mydb/releases/download/v1.0.2/mydb-1.0.2-windows-x86_64.zip) |

Use [SHA256SUMS](https://github.com/yzsind/mydb/releases/download/v1.0.2/SHA256SUMS)
to verify downloads. The [1.0.2 release page](https://github.com/yzsind/mydb/releases/tag/v1.0.2)
includes platform build manifests, locked dependency files, the Linux ABI report,
and package validation evidence.
Windows executables have the `.exe` suffix and require compatible Visual C++
and Universal CRT runtimes.

## Upgrading from 1.0.1

1. Record the old binary/checksum, configuration, SQL modes, keys, grants, and
   format registry. Preserve a full backup and verify that the original binary
   can restore it.
2. Export supported logical schema and data from a consistent source. Preserve
   unsigned integers, DECIMAL, raw bytes, temporal precision, JSON, geometry,
   and native vectors. Recreate users, grants, routines, events, and policies
   deliberately; there is no universal logical export command covering every
   native feature.
3. Start 1.0.2 with a **fresh data directory**. Do not reopen a 1.0.1 directory,
   restore its physical image into 1.0.2, remove Zone Map files to bypass format
   checks, or edit format markers. Native replication is not a cross-format
   migration mechanism; establish new compatible peers from the new instance.
4. Load schema/data and validate counts, deterministic digests, application
   queries, transactions, indexes, and privileges. Pay particular attention to
   empty-input ROLLUP, cast targets, encoded literals, view CHECK OPTION,
   keyring privileges, and XA behavior described above.
5. Stop source writes for final transfer and validation, then switch the
   application endpoint. Enable scheduled events only on the intended writer.
   Preserve the old instance and binary for rollback planning. Writes accepted
   by the new instance require a separately validated reconciliation path before
   returning to the old copy.

Automatic physical upgrade/downgrade and rolling cross-format replacement are
not supported. See [changing versions](changing-versions.md),
[backup and recovery](backup-and-recovery.md), and
[native replication](native-replication.md).

## Compatibility and evaluation boundaries

- The default isolation level is `READ COMMITTED`. `REPEATABLE READ` uses
  snapshot isolation without InnoDB gap or next-key locks.
- MySQL replication supports `ROW` with `FULL` row images; statement-based,
  mixed, `MINIMAL`, and MySQL multithreaded replica apply remain unsupported.
  Native replica promotion requires external fencing.
- MySQL X Protocol, Group Replication, Router, Shell/X, plugin ABI, `HANDLER`,
  `RESOURCE GROUP`, and `TABLESPACE` remain outside the supported scope.
- Time travel requires retained undo history; default retention is zero.
  RRF remains a technical preview. Windows evaluation targets foreground
  operation on local NTFS.

See [feature scope](feature-overview.md),
[MySQL compatibility](mysql80-compatibility.md), and
[platform requirements](hardware-and-os-requirements.md).

## Release validation and feedback

The change set adds focused regressions for SQL expressions and parsing, view
and routine behavior, DDL atomicity, catalog contention, transaction admission,
compressed packets, WAL rollover, Zone Map pruning, and vector lifecycle and
ordering. Added test coverage does not by itself establish that every release
gate has passed for a packaged binary.

Release verification uses locked dependencies and the repository's release
profile with Rust 1.95 or newer. The relevant gates include server and CLI CI,
recovery/replication safety tests, and native checks of extracted packages:
hashes, binary versions/help, required TLS, typed SQL, rollback, crash recovery,
and secondary-index reads. Linux packaging also checks the glibc 2.17 ABI.
Publish source identity and actual gate results with the release artifacts;
historical 1.0.1 or README test counts are not 1.0.2 certification.

The source baseline `c644d7a` passed
[server CI](https://github.com/yzsind/mydb/actions/runs/44893485332),
[Safety tests](https://github.com/yzsind/mydb/actions/runs/44893485348), and
[CLI CI](https://github.com/yzsind/mydb/actions/runs/44893485329).
The release branch adds documentation to that baseline. Per-platform package
results are attached to the release and identify the exact source commit and
archive checksum.

- [Documentation](README.md) and [reporting issues](reporting-issues.md)
- [Issues and feedback](https://github.com/yzsind/mydb/issues)
- [Apache-2.0 license](../LICENSE)
