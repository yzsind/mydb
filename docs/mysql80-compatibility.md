# mydb vs MySQL 8.0.46 Feature and Compatibility Comparison

This document uses MySQL Community Server **8.0.46** as the fixed comparison
baseline and systematically describes the coverage and differences of the
mydb across the classic protocol, SQL, data types,
transactions, replication, metadata, and operations. See the
[MySQL 8.0 Reference Manual](https://dev.mysql.com/doc/refman/8.0/en/) and
[MySQL 8.0.46 Release Notes](https://dev.mysql.com/doc/relnotes/mysql/8.0/en/news-8-0-46.html)
for the official manual and release notes. A non-comparative capability map of
mydb itself is available in [`feature-overview.md`](feature-overview.md).


## Conventions in This Document

mydb describes every capability with a unified rubric so that readers can
judge "is it usable, is it identical to MySQL, and can its physical effects be
relied upon":

| Marker | Meaning |
|---|---|
| **✅ Compatible** | The feature is genuinely implemented and the main observable semantics align with MySQL 8.0.46 |
| **◑ Partially compatible** | Core capabilities work, but semantic, scope, performance, or operational differences explicitly listed in this document exist |
| **△ Metadata compatible** | Parsed, persisted, or echoed for migration/tool compatibility without producing the corresponding physical effect; must be accompanied by a warning |
| **➕ mydb extension** | Capabilities offered by mydb in addition to what MySQL 8.0.46 provides |
| **❌ Not supported** | Returns a clear error (usually 1235/1064/1231); never a silent no-op |

> Compatibility markers describe supported behavior and known boundaries;
> they are not a certification of passing the full official MySQL test suite.
> A registered function name or system table does not imply that every
> parameter form and producer is fully implemented.
> Unsupported SQL/protocol must return a clear error; the metadata-compatibility
> and pure-hint exceptions must remain diagnosable.

Errors and diagnostics use **MySQL 8.0.46** as the fixed baseline. See
[Error Codes and Diagnostics](error-codes.md) for the complete
errno/symbol/SQLSTATE catalog, the MYDB reserved ranges, `SIGNAL/RESIGNAL`
rules, and the catalog refresh process.

The mydb database core is implemented in Rust (2021 edition, `tokio`). Its
goal is compatibility with MySQL clients and common SQL, not a byte-for-byte
replica of InnoDB, the plugin ABI, or the complete MySQL operations ecosystem.
It is still in development toward its first product release and is not yet a
production-grade MySQL replacement.

### Overall Conclusions

| Area | Compatibility Level | Key Conclusion |
| --- | --- | --- |
| Classic protocol, TLS, authentication | ✅/◑ | Common MySQL CLI/JDBC paths work; no X Protocol |
| Common DDL/DML/SELECT | ✅/◑ | Common paths work; composite ALTER, view DML, and locking reads have shape restrictions |
| Data types, functions, character sets | ✅/◑ | Mainstream types and functions work; GIS and edge-case coercion still have documented gaps |
| InnoDB physical/locking semantics | ◑ | Native ACID engine, incompatible with the InnoDB file format; RR is snapshot isolation |
| Optimizer and executor | ➕/◑ | CBO, parallelism, vectorization, and spilling implemented; coverage depends on operators and expressions, and plans are not guaranteed to match MySQL |
| System schemas and SHOW | ◑ | Common surfaces with real producers work; some registered tables honestly return empty |
| MySQL replication | ◑ | ROW/FULL source-replica pipeline works; no SBR/MIXED, MINIMAL, or MTS |
| MySQL operations ecosystem | ◑/❌ | Common configuration/logging/privilege surfaces work; no Group Replication, Router, Shell/X, or plugin ABI |
| mydb extensions | ➕ | VECTOR/HNSW/IVF, Zone Map, time travel, sequences, DML RETURNING, physical replication, and more |

---

## 1. Client Protocol and Connections

**✅/◑ Core paths of the MySQL classic protocol v10.**

### Authentication Plugins

| Plugin | Status | Notes |
|---|---|---|
| `caching_sha2_password` | ✅ | Default plugin. Persistent credentials use the MySQL `$A$005$` + 20-byte salt + SHA-256 crypt 5000 rounds format; after a successful full authentication, stage2 is cached and subsequent connections take the nonce-bound fast authentication path; RSA/TLS full auth supported |
| `mysql_native_password` | ✅ | Classic scramble path |
| `sha256_password` | ✅ | RSA public key exchange when not using TLS |

- **TLS / mTLS**: advertised by default; `REQUIRE X509 / SUBJECT / ISSUER / CIPHER` supported. cert/key are used when provided, otherwise a temporary self-signed certificate is generated (providing only one of them fails at startup).
- **Authentication transport security**: `caching_sha2_password` and `sha256_password` full authentication require RSA-encrypted passwords on non-TLS connections; plaintext full authentication is accepted only over TLS.
- Account matching follows the MySQL 8.0.46 Host-first order for literal IP/hostname, CIDR, netmask, wildcard, and anonymous accounts; `CURRENT_USER()` returns the authorization ID actually matched.
- **Prepared statements**: strict single-statement PREPARE, full parameter typing, the global `max_prepared_stmt_count`, long data, server-side read-only cursors (process-level registry, throttled by `MYDB_CURSOR_*`), and prepared CALL with multiple result sets/OUT parameters. Input that cannot be prepared or contains multiple statements fails explicitly at the PREPARE stage and is never executed via text substitution.
- **`CLIENT_OPTIONAL_RESULTSET_METADATA`**: a real session-only `resultset_metadata={FULL|NONE}`; both PREPARE_OK and text/binary result sets carry `metadata_follows`.
- **Capability negotiation**: client flags are intersected with the server's advertisement; FOUND_ROWS, INTERACTIVE, MULTI_RESULTS, PS_MULTI_RESULTS, QUERY_ATTRIBUTES, and SESSION_TRACK are supported. Multi-factor authentication is not implemented, so the MFA bit is not advertised.
- **Protocol compression**: zlib and feature-gated zstd are supported; the default `mysql-replica` feature also enables `zstd-compression`, so both are available in the default build. The master switch is `MYDB_COMPRESS_PROTOCOL` and the per-algorithm switches are `MYDB_COMPRESS_ZLIB` / `MYDB_COMPRESS_ZSTD`; when disabled, the corresponding capability is not advertised, and when both algorithms are negotiated, zstd is preferred.
- **Query attributes**: text/binary command parsing carries typed statement-level attributes readable via `MYSQL_QUERY_ATTRIBUTE_STRING(name)`; each new command clears the previous attributes.
- **Session tracking**: schema, configured system variables, and the state-change channel have real notifications; `session_track_gtids` / `session_track_transaction_info` accept only `OFF` and provide no GTID or transaction state/characteristics notifications.
- **Multi-statement responses**: with `CLIENT_MULTI_STATEMENTS` (including dynamic enablement via `COM_SET_OPTION`), statements execute one by one and return results with consecutive packet sequence numbers, and intermediate results set `SERVER_MORE_RESULTS_EXISTS`; affected rows, insert ID, warnings, and transaction state are preserved statement by statement. Syntax or execution errors keep the responses of already-succeeded statements and stop subsequent ones; mixes of DML, SELECT, CALL, and LOCAL INFILE are supported, and semicolon splitting respects quotes, comments, stored-program statement bodies, and the session `sql_mode`.
- **Utility commands**: `COM_RESET_CONNECTION`, `COM_STATISTICS`, `COM_PROCESS_INFO`, `COM_REFRESH`, `COM_DEBUG`, and strict `COM_SET_OPTION` are wired up; unsupported refresh bits return 1235.
- **COM_BINLOG_DUMP / GTID**: supported (as a binlog source).
- **Connection management**: `MYDB_MAX_CONNECTIONS` (default 1000), `MYDB_LISTEN_BACKLOG`, `MYDB_HANDSHAKE_TIMEOUT`, `MYDB_MAX_ALLOWED_PACKET` (64 MiB, capped at 1 GiB), `MYDB_WAIT_TIMEOUT` / `NET_WRITE_TIMEOUT`; startup tries to raise `nofile` and clamps the effective connection count when the kernel hard cap is too low.
- **Large input**: LOCAL INFILE stages in governor-managed memory within 1 MiB, then migrates to a streaming spill file governed by the unified temporary-disk quota.

❌ **Not supported**: `mysqlsh` X Protocol.

---

## 2. Data Definition Language (DDL)

### Table DDL

**✅ Common `CREATE` / `ALTER` / `DROP TABLE` actions.**

- **InnoDB-style table options** (`ROW_FORMAT` / `STATS_*` / `KEY_BLOCK_SIZE` / `MAX_ROWS` …): △ **accepted + persisted + echoed + physically ignored**. The `Row_Format` column of `SHOW TABLE STATUS` is backfilled from the persisted option (no longer hardcoded).
- **CTAS** (`CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [(columns/constraints)] ... AS SELECT`): ✅ full target-table options/partitioning are never lost; inferred columns stay faithful to catalog origin and outer-join nullability follows the join shape; persistent tables/temporary tables, declared columns, IGNORE/REPLACE, CTEs/set operations/derived tables load in unified chunks, and CTAS no longer actively materializes the entire result (query operators themselves may still materialize); unknown string upper bounds use the full LOB range. Persistent CTAS achieves crash atomicity via intent + deferred DD generation, and binlog puts the parsed CREATE and WRITE_ROWS into the same GTID/XID transaction.
- **Naming and size limits**: table/column identifiers and indexes declared in CREATE/ALTER TABLE are validated up to 64 characters (errno 1059); standalone CREATE INDEX currently has no equivalent index-name length guard. Tables have at most 1017 live columns (errno 1117). Tables, views, and sequences share one relational-name namespace, and concurrent CREATEs are double-checked under the catalog write lock. See [limits and enforcement gaps](limits.md).
- **Generated columns** (`GENERATED ALWAYS AS`, VIRTUAL / STORED): ✅
- **CHECK constraints**: ✅ all actions
- **Foreign keys (FK)**: ✅ all four referential actions, including cascades
- **Online CHECK / FK ALTER**: ➕ constraint-only ALTERs allow concurrent reads and writes by default, including CHECK add/enable/disable, FK add and DROP+ADD replacement, and atomic publication of multiple constraints. Preparation and publication use short write barriers; `LOCK=SHARED` allows reads and blocks writes, `LOCK=EXCLUSIVE` retains exclusive semantics. Enabling a not-yet-enabled CHECK, disabling, and dropping constraints do not scan existing data; weak lock requests in mixed column/index/partition actions fail explicitly.
  See [ALTER TABLE](statements/ddl/alter-table.md) for supported syntax and boundaries.
- **`ALTER TABLE ALGORITHM={INSTANT|INPLACE|COPY|DEFAULT}`**: ✅
  ➕ Column type changes within the supported range are online by default and also accept `COPY, LOCK=NONE`; strict mode covers LOBs, partitioning, and vector indexes under on-disk storage — see [Online Column Type Change](online-column-type-change.md).
  Algorithms are validated against the actual work: adding generated columns or columns with enforced CHECKs requires COPY, and dropping a column whose index rebuilds cascade requires INPLACE; encoding-compatible constraint tightening only scans for validation under strict mode. Composite actions that cannot yet be published atomically return 1846 before modification. See [ALTER TABLE](statements/ddl/alter-table.md) for supported combinations.
- **Online BTREE `ADD/CREATE INDEX` and `ADD PRIMARY KEY`**: ✅ Ordinary, unique, and primary key indexes allow concurrent DML during the Building phase by default, retaining sorted/bulk-load plus incremental catch-up; preparation and publication use read-compatible write barriers.
  Multi-index ALTERs that only add these indexes are also supported; `LOCK=SHARED` allows reads and blocks writes during the scan phase, and `LOCK=EXCLUSIVE` keeps exclusive access.
  The initiating statement always waits for Public or fails, regardless of `MYDB_DDL_ASYNC`, and never returns OK early.
  Primary key candidates are published together with NOT NULL after final validation; DML still runs under the old constraints during the build;
  when NULL/duplicate keys cause creation to fail, already committed business data is retained. mydb keeps the heap and does not rebuild a clustered table,
  and never converts NULL to a default value regardless of SQL mode. Old snapshots hide new indexes that do not yet cover themselves, uniqueness,
  and NOT NULL optimizer facts, and keep reading old data; other DDL boundaries such as physical generation switches may still return 1412.
- **DDL read concurrency**: ➕ ADD COLUMN without heap rewrite, encoding-compatible MODIFY, same-name
  CHANGE, and NOT NULL changes that only scan for validation under strict mode can run in parallel with existing ordinary reads and not-yet-consumed
  streaming cursors; FIRST/AFTER within the supported range and column changes are published atomically.
  Single-action column comment/default/visibility, index visibility, and table options/storage attributes/auto-increment options likewise use
  read-compatible barriers. Explicit `LOCK=EXCLUSIVE` still excludes readers.
  Column/generated-column changes that require a physical rewrite, rebuilds, and some partition publications still wait for readers, so not every
  `ALTER TABLE` can be called read-nonblocking. See [ALTER TABLE](statements/ddl/alter-table.md).
- `information_schema.mydb_ddl_jobs` keeps the most recent 64 COPY jobs in COMMITTED/FAILED/CANCELLED terminal states, so background errors no longer vanish into logs; online change-log capacity is charged to
  `DdlOnlineLog`, and budget failure aborts the DDL instead of allowing ungoverned memory growth.
- **Parallel B-tree and FULLTEXT `ADD/CREATE INDEX`**: ➕ supports `CREATE /*+ PARALLEL(n) */ INDEX`
  and `ALTER /*+ PARALLEL(n) */ TABLE ... ADD INDEX`; this hint is a mydb extension,
  `NO_PARALLEL` can force serial execution. Persistent offline FULLTEXT builds also support
  parallel scanning, tokenization, sorting, spill consolidation, and leaf writes;
  explicit parallel requests for other index types return 1235.
- **`RENAME TABLE`**: ✅ cross-database renames supported
- **Atomic recovery**: CREATE/DROP/TRUNCATE/COPY/partition reorganization and RENAME TABLE/INDEX all use
  fsync intents; RENAME startup recovery rolls file moves forward or back based on the stable table identity in the DD.
- **Temporary table DDL**: CREATE/DROP TEMPORARY do not implicitly commit, but the object operations are not rollbackable (ROLLBACK
  warnings 1751/1752); ALTER/CREATE INDEX/TRUNCATE do implicitly commit.
- **Temporary table scope**: session-private and taking precedence over same-name persistent tables, supporting LIKE/CTAS, generated columns, CHECK,
  B-tree/functional indexes, transactional DML, and SAVEPOINTs; temporary-table FKs and partitioning are explicitly rejected.
  By default each session is limited to 1024 tables, 1 GiB allocated heap/LOB/index space, and 64 MiB undo,
  configured via `MYDB_SESSION_TEMP_{TABLE,STORAGE,UNDO}_LIMIT`; exceeding the limits returns 1114.
  Rollback may retain reusable pages; TRUNCATE/DROP releases the storage quota.
  `ON COMMIT {PRESERVE ROWS|DELETE ROWS|DROP}` is a mydb extension.
- **ENGINE**: only `mydb`/`InnoDB` registered in `SHOW ENGINES` are accepted; other names return
  1286, and unimplemented physical-engine promises are no longer downgraded to metadata-only.
- **`ANALYZE TABLE t1, t2, ...`**: ✅ multi-table ordinary sampling returns status per table;
  `ANALYZE TABLE t PARTITION (p0,p1)` supports specifying partitions; UPDATE/DROP HISTOGRAM is limited to a single table.
- **COMMENT length hard validation**: ✅ table 2048 / column 1024 / index 1024 / partition 1024 characters, independent of sql_mode (errno 1628/1629/1688/1793)

### Index Types

| Type | Status |
|---|---|
| B-tree (including unique, composite, covering INCLUDE, prefix, descending, functional/expression, and invisible indexes) | ✅/➕ |
| JSON multi-valued index | ✅ |
| FULLTEXT (BM25 + boolean mode) | ✅ |
| SPATIAL (MBR B-tree + derived R-tree) | ✅ |
| HNSW vector index (`USING HNSW`) | ➕ see [§9](#9-vector-search) |
| IVF vector index (`USING IVF`) | ➕ see [§9](#9-vector-search) |

LIKE access paths follow the column collation: pure literals/fixed prefixes can
use B-trees (including composite, prefix, and functional indexes) and string
Zone Maps, always with residual rechecking. An existing single-column
`FULLTEXT ... WITH PARSER ngram` index can safely prefilter prefixless LIKE
when `like_ngram_prefilter=on`; ordinary FULLTEXT is not treated as a LIKE
substring index. `MATCH` AND/OR/NOT, zero-score comparisons, multiple MATCH
occurrences, and prepared `AGAINST(?)` all retain full boolean semantics.

Partitioned and historical FULLTEXT reads use visible-text heap oracles rather
than current inverted postings. They preserve document identity and historical
membership at the cost of a corpus scan. BM25 scores and normalization do not
promise full MySQL ranking/collation equivalence; see
[Full-Text Search](fulltext-search.md) for operators, configuration, and limits.

Immutable backup queries support FULLTEXT filtering and score projection using
MVCC-visible text, including when the base contains unresolved prepared XA
changes. See [FULLTEXT backup queries](backup-and-recovery.md#51-fulltext-queries).

### OPTIMIZE TABLE

`OPTIMIZE [LOCAL | NO_WRITE_TO_BINLOG] TABLE t [, ...]` performs a synchronous
online physical rebuild that reclaims heap, B-tree, and LOB space left behind
by DELETE/UPDATE, rebuilds FULLTEXT, SPATIAL, and HNSW/IVF indexes, collapses
the INSTANT row layout, and refreshes statistics and Zone Maps. Ordinary
tables, partitions, and subpartitions are supported; table IDs, definitions,
constraints, and auto-increment counters are preserved. Each successful table
returns `status / OK`; a failure returns the error plus `Operation failed`
while the remaining targets continue; cancellation aborts the statement. Binlog
records only successful targets, LOCAL/NO_WRITE_TO_BINLOG skips logging the
maintenance statement, and shadow-row writes are never replicated as business
rows.

The replication, index-build, and catch-up phases hold a SharedUpgradable MDL
that permits ordinary DML; preparation and publication use short exclusive
barriers. Barrier waits never queue new DML behind the maintenance request;
the final replay has row-count, byte, and time budgets, releasing the barrier
to continue catching up when exceeded and failing explicitly when it cannot
converge. Long transactions may make the switchover wait or time out; RR
snapshots that began on the old physical generation return 1412 when accessing
the table after the switch and should reopen the transaction. Disk sync
duration is not covered by a hard cap in the replay time budget.

Online optimization is currently unsupported for Memory durability,
temporary tables, system tables, and partitioned tables with FULLTEXT indexes;
it reports a clear error instead of degrading into a fully blocking offline
rebuild. Empty tables still retain the necessary page/index metadata, so space
does not drop all the way to zero. Optimization needs extra disk space for the
old and new storage to coexist; concurrent writes may leave a small amount of
new fragmentation in the new generation. `ALTER TABLE ... OPTIMIZE PARTITION`
keeps its separate behavior. See [OPTIMIZE TABLE](statements/admin/optimize-table.md)
for syntax and operational limits.

### Partitioning

**✅ RANGE / LIST / HASH / KEY + COLUMNS variants.**

- Partition column/expression types are always strictly validated under MySQL 8.0 rules; no runtime switch that would change persisted routing semantics is provided anymore.
- Storage IDs are assigned only to physical leaf partitions; RANGE/LIST parents of two-level partitioning are pure logical nodes and create no empty heap. See [§11](#11-storage-engine-and-persistence) for the current format versions.
- KEY supports `ALGORITHM=1/2` (default 2) and LINEAR bucket mapping; write routing and equality pruning share the same column hash. `KEY()` automatically picks the primary key, or the first all-NOT-NULL unique key when there is no primary key.
- KEY / LINEAR KEY use mydb's own deterministic column hash, so concrete physical partition placement is not guaranteed to match MySQL. Logical data restored across engines is re-routed by the target engine; programs relying on explicit `PARTITION(pN)` targeting must not assume identical physical buckets between the two engines.
- RANGE / RANGE COLUMNS write routing uses binary search over ordered boundaries; equality, NULL-safe equality, IN, range, AND/OR, and contradictory predicates share the pruning path, and an empty partition set opens no heap. A parent partition and its HASH/KEY subpartition can be pruned simultaneously down to one physical leaf under a full equality key. Integer `DIV` is allowed in partition expressions; `/`, bitwise operators, and LIST `DEFAULT` are explicitly rejected.
- Repartitioning DDL is **crash safe**: streaming build-then-swap (COALESCE / REORGANIZE / REPARTITION / REMOVE PARTITIONING / ADD PARTITIONS), with the persistence phase fixed at `INTENT / SNAPSHOT_BUILD / CATCH_UP / QUIESCE / SWAPPED / CLEANUP / DONE|FAILED`; startup recovery idempotently rolls forward or back according to the physical generation in the catalog. `CANCEL DDL JOB <job_id>` is accepted only before the swap; status and progress are in `information_schema.mydb_ddl_jobs`.
- `REBUILD PARTITION` rewrites only the target physical leaves into a new storage generation, compacting heap/LOB and rebuilding local indexes in bulk; `OPTIMIZE PARTITION` additionally analyzes only the target leaves — unselected leaves are not scanned; `CHECK PARTITION` validates row-routing membership and heap/B-tree equivalence.
- `ANALYZE PARTITION` persists per-leaf row counts, heap pages, column/index sketches, and check status; the CBO merges costs over the pruned leaf set. Partition-wise hash join is enabled when the topology is identical and the equality join covers the full partition key, and partition-wise aggregate when `GROUP BY` covers the full partition key.
- Partition bulk DML, physical DDL, and statistics builds are charged to the unified governor categories `PartitionDmlBatch`, `PartitionDdl`, and `PartitionStats` respectively; current values and rejection counts are exposed via `SHOW STATUS`, `information_schema.mydb_memory_advisor`, and the memory summary views.
- Per-partition physical options: `ENGINE` accepts only the `mydb`/`InnoDB` current-engine aliases and is not persisted; `DATA/INDEX DIRECTORY`, `TABLESPACE`, `MAX_ROWS`, `MIN_ROWS`, and `NODEGROUP` return explicit not-supported errors and are never silently ignored.
- FULLTEXT, SPATIAL, HNSW, and IVF can be used on partitioned tables; this is a mydb extension enabled by default that no longer requires a runtime switch.

### Views (VIEW)

**✅ `CREATE` / `OR REPLACE` / `ALTER` / `DROP VIEW`.**

- Column aliases, `IF NOT EXISTS`, view expansion, cycle detection, and depth limits.
- **Semantic definition persistence**: the query AST parsed at CREATE/ALTER is saved to the catalog and shared via immutable objects; execution uses that AST. The SHOW CREATE text is for display only. Quoting/escaping semantics from creation survive SQL mode changes and restarts; the view dependency key includes a digest of the query semantics.
- **Encoding resource bounds**: semantic query encoding is capped at 64 MiB, 1,000,000 JSON containers, and 4,000,000 scalars. Scalar expression depth is 1,024 and SELECT nesting is 63; semantic query depth is `1,024 + 63 = 1,087`, and encoded JSON depth is `4 × (1,024 + 1,087) = 8,444`. The compiled-expression depth limit of 64 is separate: deeper admitted expressions use the stack-extending scalar evaluator. Exceeding the encoding limits returns a clear error before the catalog write; corrupted or unknown-version semantic queries fail at catalog read time instead of being replaced with an empty query. Encoding capacity is charged to SystemMetadata; decode scratch space and stack segments are charged to ParserTransient.
- **ALGORITHM tri-state**: `MERGE` / `UNDEFINED` / `TEMPTABLE` (genuinely materialized under `no_merge`).
- **SQL SECURITY DEFINER / INVOKER**: ✅ the read path switches the privilege account.
- **WITH CHECK OPTION** (LOCAL / CASCADED): ✅ genuinely enforced (post-DML image evaluation, failure 1369). Single-table views with computed projection columns are currently rejected for CHECK OPTION with error 1368; use a direct-column view for checked DML.
- **Updatable views**: single-table INSERT / UPDATE / DELETE ✅, including nested views with wildcards and renamed columns; hidden base-table columns cannot be accessed via DML/locking reads.
- **Non-recursive CTE views**: multiple references share the materialized result; recursive CTE views still fail explicitly with 1235.
- **Locking reads**: simple updatable views and their joins with base tables support real row locks and NOWAIT/SKIP LOCKED; complex locking-join shapes involving aggregation, window functions, NATURAL/USING, LATERAL, etc. are still explicitly rejected. Lock clauses inside a view definition fail explicitly with 1235 at CREATE/ALTER instead of being silently ignored.
- **Dependency revalidation**: ALTER/REPLACE VIEW propagates invalidation and rebinds dependent views; DROP DATABASE invalidates cross-database dependencies; missing dependencies report 1356.
- **Normalized definition echo**: `SHOW CREATE VIEW` / `IS.views.VIEW_DEFINITION` echo the normalized definition (identifier backquoting, `db.tbl` qualification, `*` expansion, adding `AS alias`); constructs that cannot round-trip (subqueries/windows/CTE/MATCH/GROUP_CONCAT) conservatively fall back to the original text.
- **Column types frozen at CREATE**: expression column types and nullability are determined when the view is created.
- **Join-view DML**: single-base-table UPDATE over INNER/CROSS JOIN views with direct column projections is supported, preserving the join and WHERE conditions; duplicate matches modify each target row only once; cross-base-table assignments report 1393 and DELETE reports 1395. Single-base-table INSERT with an explicit column list remains supported. Outer joins, NATURAL/USING, computed columns, extra outer joins, and CHECK OPTION/RETURNING on join views — shapes not yet supported — are explicitly rejected.
- **View definition and dependency behavior**: TRIM special syntax round-trips through normalization; wildcard metadata excludes INVISIBLE columns so the view column set is stable after ALTER; multi-object DROP VIEW deletes in a single catalog transaction after unified validation; dependent views are revalidated after function rebuilds; source views of INSERT…SELECT go through expansion and privilege validation; CHECK OPTION on non-updatable views reports 1368.
- **Retained differences**: SHOW CREATE VIEW keeps the correct full output column-name list so explicit column names are not lost on rebuild; FOR UPDATE on aggregate views remains explicitly rejected (1288) rather than disguising unimplemented base-table row locking as success.

### Other Objects

| Object | Status | Highlights |
|---|---|---|
| **DATABASE / SCHEMA** | ◑ | Database creation supports character sets, collations, and `[DEFAULT] ENCRYPTION='N'` (including the MySQL version comment); `'Y'` returns an explicit not-supported error — no at-rest storage encryption yet |
| **Stored procedures / functions** | ✅ | `CREATE/ALTER/DROP`, `CALL`, full procedural language (see [§7](#7-stored-routines-triggers-and-events)) |
| **Triggers** | ✅ | BEFORE/AFTER × INSERT/UPDATE/DELETE, FOR EACH ROW |
| **Event scheduler** | ✅ | `CREATE/ALTER/DROP EVENT`, background scheduler thread |
| **Sequences / SEQUENCE** | ➕ | PostgreSQL-style CREATE/ALTER/DROP, SHOW SEQUENCES, `nextval/currval/lastval/setval` |
| **TABLESPACE** | ❌ | DDL and named-tablespace options return 1235; no name catalog or dedicated SHOW/IS echo is registered |
| **RESOURCE GROUP** | ❌ | CREATE/ALTER/DROP/SET return 1235; no resource-group catalog is registered |

### Data Types

| Type Family | mydb Status | Relationship to MySQL 8.0.46 |
| --- | --- | --- |
| TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT | ✅ | Includes UNSIGNED, ZEROFILL metadata, AUTO_INCREMENT |
| FLOAT/DOUBLE/DECIMAL | ✅/◑ | DECIMAL covers `(65,30)` and exact expressions never fall back to floating point; mixed string/scientific-notation/FLOAT/DOUBLE coercion still follows approximate-number rules and continues to be diffed |
| CHAR/VARCHAR, BINARY/VARBINARY | ✅ | Character sets, collations, PAD SPACE/NO PAD, and the column `BINARY` attribute participate in comparisons and indexes |
| TEXT/BLOB four tiers | ✅ | LOBs can be externalized with optional LZ4; the physical format is not InnoDB |
| DATE/TIME/DATETIME/TIMESTAMP/YEAR | ✅/◑ | fsp 0–6, sql_mode, named time zones; TIMESTAMP extends through 9999-12-31 23:59:59.999999 UTC |
| ENUM/SET/BIT | ✅ | Enum ordinals, SET bitmasks, and binary bit aggregates are supported |
| JSON | ✅/◑ | Binary JSONB, JSON_TABLE/MVI/functions; the internal format is not identical to InnoDB binary JSON |
| GEOMETRY family | ◑ | SRID+WKB, common functions, and spatial indexes work; see GIS boundaries |
| VECTOR(n) | ➕ | MySQL 8.0.46 Community Server has no equivalent type |

BOOLEAN is a TINYINT-compatible alias. mydb does not implement the physical
row formats of the MySQL storage engines; `ENGINE=InnoDB` is a compatibility
alias for the native mydb engine.

### Expressions and Operators

- ✅ Parsing follows the MySQL 8.0 `expr / bool_pri / predicate / bit_expr / simple_expr`
  hierarchy; covers right-recursive `BETWEEN`, `LIKE/REGEXP/IN/IS` predicate
  constraints, logical `XOR`, bitwise or/and/shift, `^`, `DIV/MOD`, `!`, `~`,
  and mixed precedence with the user-variable `:=`.
- ✅ The parsing semantics of `PIPES_AS_CONCAT`, `HIGH_NOT_PRECEDENCE`, and `ANSI`
  enter the parser/plan-cache key; switching the session mode after PREPARE does
  not reinterpret the original AST.
- ✅ `IGNORE_SPACE` (also implied by `ANSI`) controls whitespace between a native
  function name and `(`; schema-qualified stored functions are exempt.
- ✅ `||` is fixed at parse time as either logical `OR` or string `CONCAT`;
  persistent expressions such as views, CHECK, generated
  columns, and partitions use mode-independent normalized SQL.
- ✅ `!`, `&&`, default-mode `||`, and in-expression `:=` produce the MySQL 8.0.46
  1287 warning, preserving source order; SQL/protocol PREPARE emits it once only
  when preparation succeeds.
- ✅ Migrated expressions generate a fixed-width result-type descriptor at binding
  time; numeric/DECIMAL, signed/unsigned control flow, temporal FSP, fixed system
  functions, and plain vs. DISTINCT/vectorized aggregates share the same type
  algebra. Compiled expressions retain the descriptor after constant folding, and
  prepared statements, empty results, and CTAS no longer infer types backward from
  the values returned at runtime. `div_precision_increment` and `group_concat_max_len`
  are part of the plan-cache semantic key.

---

## 3. Data Manipulation Language (DML)

### INSERT Family

**✅ Multi-row INSERT, `IGNORE`, `REPLACE`, `ON DUPLICATE KEY UPDATE` + 8.0.19 row aliases, and the `PARTITION` clause.** ODKU with at least 64 rows and no partitioning/AUTO_INCREMENT/FK/triggers/RETURNING batches writes at the earliest conflict boundary by default; `MYDB_BATCH_ODKU=0` falls back to the row-by-row oracle; when the safety gate is not met, the old path is taken automatically.

AUTO_INCREMENT uses a MySQL 8.0-style persisted tiering: an atomic in-memory
counter per table performs allocation, high-water changes are written as
compact redo, and checkpoints merge into `mysql.innodb_dynamic_metadata`;
recovery merges the DD baseline followed by subsequent redo. There is no
per-table `.ai` file, and ROLLBACK does not reclaim allocated values.

Closing the loop on MySQL 8.0 semantics:

- **sql_mode-aware numeric/string coercion**: strict out-of-range 1264 / overlong 1406 hard errors; non-strict saturating clamp/truncation + warnings 1264/1265. Numeric, string, and temporal writes follow consistent conversion rules. Session and global default `sql_mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO`; an explicitly empty mode and new sessions inheriting an empty global mode both execute in non-strict mode.
- **Omitting a NOT NULL column without default**: strict reports 1364; non-strict stores the implicit default + warning 1364 (distinct from 1048 for an explicit NULL).
- **Explicit NULL into NOT NULL**: strict mode reports 1048; non-strict multi-row INSERT and
  INSERT…SELECT are fixed to store the type's implicit default with warning 1048, while single-row VALUES/SET still reports 1048,
  aligning with MySQL's statement-shape distinction.
- **OK packet info text**: `Records: N Duplicates: D Warnings: W` (multi-row VALUES / with modifiers / INSERT…SELECT).
- **`NO_AUTO_VALUE_ON_ZERO`**: ✅ genuinely effective (an explicit 0 is stored as-is; only NULL triggers auto-increment).
- **`LAST_INSERT_ID(expr)`**: sets the session last_insert_id side effect.
- Column charset representability is strongly validated at INSERT with 1366.
- **`INSERT DELAYED`**: △ accepted + warning 1287.

### REPLACE (MySQL 8.0 Compatible)

**✅** Requires both INSERT + DELETE privileges; `REPLACE … ON DUPLICATE KEY UPDATE` reports 1064 (no silent degradation). The proactive-probe fast path deletes conflicting rows before inserting, and auto-increment advances only once (aligned with MySQL). Multiple unique keys delete multiple rows at once; deleting conflicting rows fires DELETE triggers + FK parent-side cascades; conflicts on partitioned tables disambiguate RowIds with a single-partition prune set.

### LOAD DATA / LOAD XML

**✅/◑ `LOAD DATA [LOW_PRIORITY|CONCURRENT] [LOCAL] INFILE`**:

- `CHARACTER SET` input transcoding, column lists containing `@user_var`, `SET col={expr|DEFAULT}`, the disabled semantics of `ESCAPED/ENCLOSED BY ''`, sql_mode-aware coercion, column-count mismatch warnings 1261/1262, and the OK packet `Records/Deleted/Skipped/Warnings`.
- Retained integrity policy: LOCAL input does not implicitly replace undecodable bytes with `?`, skip foreign-key violations, or skip rows outside an explicit `PARTITION` set. These cases return 1366, 1452, or 1748 and roll back the statement. Correct the source data or partition selection before retrying.
- `local_infile` OFF gating with ER 3948; non-LOCAL goes through `secure_file_priv` validation ER 1290 + requires the FILE privilege; LOCAL is exempt from FILE.
- Performance: default strict + no special features take the parallel chunked fast path; only SET/@var/charset/XML usage or a non-strict sql_mode takes the row-by-row general path.
- **✅ `LOAD XML [LOCAL] INFILE ... ROWS IDENTIFIED BY '<tag>'`**: attribute-style / nested / `<field name=>` — three forms.

### UPDATE / DELETE

- **Single-table UPDATE / DELETE + ORDER BY + LIMIT**: ✅ candidates carry the partition `storage_id` throughout; sort-key errors are not demoted to NULL, and huge LIMITs are not preallocated from the user value; unordered small LIMITs stop early while streaming, and ordered paths support forward/reverse indexes, partition-local index merges, bounded TopN, and spillable sorts for large K, writing row by row in candidate order. UPDATE LIMIT counts matched rows and DELETE LIMIT counts successfully deleted rows; both OFFSET spellings are rejected with 1064.
- **Multi-table UPDATE / DELETE**: ✅ inner/cross/comma joins + LEFT JOIN targets on both sides. Only genuinely matched rows on the non-preserving side are writable; NULL-padding virtual rows are skipped; a single two-table `a RIGHT JOIN b` is normalized by flipping to `b LEFT JOIN a` (RIGHT inside a chain still reports 1235).
- **DELETE in both forms**: `DELETE t1,t2 FROM refs` and `DELETE FROM t1,t2 USING refs` are both supported; the `tbl_name.*` target suffix; per-table `PARTITION(...)` pruning.
- **`UPDATE LOW_PRIORITY [IGNORE]`**: ✅ IGNORE covers everything (unique key 1062 / NOT NULL 1048 / FK 1452·1451 / out-of-range 1264 / truncation 1265·1366 / CHECK 3819 converted row-wise to warnings and skipped).
- **True no-op detection for UPDATE**: column-wise post-coercion comparison; no-op rows write nothing to heap/undo/binlog and do not refresh `ON UPDATE CURRENT_TIMESTAMP`.
- **`CLIENT_FOUND_ROWS`**: the OK packet returns matched/changed per the bit + `Rows matched: N Changed: M Warnings: W`.
- **CTE-driven DML**, **`SELECT ... INTO @var`**, **`DO expr`**: ✅
- ➕ **`INSERT / UPDATE / DELETE ... RETURNING`**: a PostgreSQL 18-style extension returning NEW/NEW/OLD respectively by default, with explicit `OLD`/`NEW` references and `WITH (OLD AS ..., NEW AS ...)` aliases. Supports plain, ODKU, single-target JOIN DML, prepared statements, and updatable views; UPDATE/DELETE RETURNING that genuinely modifies multiple target tables returns 1235.
- ❌ **`HANDLER`**: explicitly 1235.

---

## 4. Queries (SELECT) and Transactions

### SELECT Capabilities

| Feature | Status |
|---|---|
| JOIN (INNER / LEFT / RIGHT / CROSS / USING / NATURAL) | ✅ |
| `FULL [OUTER] JOIN` | ➕ MySQL 8.0 lacks this syntax; NULL padding for unmatched rows on both sides is supported, combinable with Hash Join and CTEs |
| `JOIN LATERAL (SELECT ...)` | ✅/◑; correlated derived tables, per-group TopN, and set-operation sources are supported; supported shapes have explicit restrictions |
| Subqueries + decorrelation | ✅ |
| `UNION` / `INTERSECT` / `EXCEPT` [ALL] | ✅ |
| `MINUS` [ALL] | ➕ set-difference extension, synonymous with `EXCEPT` |
| `SELECT STRAIGHT_JOIN` / `STRAIGHT_JOIN` join operator | ✅ constrains join order per query block or join edge; supports `ON`, `USING`, and multi-table DML; result row order not guaranteed |
| CTE + `RECURSIVE` | ✅ |
| `GROUP BY ... WITH ROLLUP` | ✅ |
| `ROLLUP(...)` / `CUBE(...)` / `GROUPING SETS (…)` | ➕ mydb extensions |
| Row constructor comparisons, `IN (ROW(...))` | ✅ |
| `JSON_TABLE` implicit lateral | ✅/◑ |
| `generate_series(start, stop [, step])` | ➕ mydb extension; integer/exact decimal/temporal families and `WITH ORDINALITY` |
| Window functions + ROWS/RANGE frames | ✅/◑; including exact numeric and temporal INTERVAL bounds and prepared frame rebinding |
| Window frame `GROUPS` / `EXCLUDE` | ➕ mydb extensions |
| `FOR UPDATE` / `FOR SHARE [NOWAIT\|SKIP LOCKED]` | ✅; `OF` alias lists and multiple clauses support per-source lock modes and NOWAIT/SKIP LOCKED; NATURAL/USING, lateral, and grouped locking-join shapes retain explicit restrictions |
| `SELECT ... INTO OUTFILE` / `INTO DUMPFILE` | ✅ |

**Compiled SELECT fast paths**: cover
projection-only point lookups, index range/multi lookups, fused
scan-filter-project-limit, ordered index scans, and a bounded range visitor
for `COUNT/SUM/AVG/MIN/MAX ... WHERE key BETWEEN ? AND ?`. Cross-index OR/AND
supports MySQL-style Index Merge union, sort-union, intersection, and
union-of-intersections; the four `optimizer_switch` toggles and the
`INDEX_MERGE`/`NO_INDEX_MERGE` hints are all part of plan-cache semantics.
When a query does not match a proven shape, execution falls back to the
general physical plan before consuming input; prepared and text execution can
reuse the same compiled memo. See [`executor.md`](executor.md) for the full
execution contract.

The bounds, NULL/step semantics, and currently unsupported shapes of
`generate_series` are documented in
[`generate-series.md`](generate-series.md).

### Window Function Boundaries

Window ordering and peer grouping follow the expression collation; integer/DECIMAL
RANGE uses exact comparison, and temporal RANGE supports calendar INTERVALs
(including month-end handling). Ranking functions, LAG/LEAD, and aggregates run
under their respective frame rules; `SELECT DISTINCT` deduplicates after window
results are produced, then LIMIT applies. Windows may appear in projection
expressions or serve only ORDER BY, and combine with CTEs/derived tables, joins,
and set operations. Fixed-width columnar paths, shared sorts, parallel partition
sorts, and low-budget spills are supported; actual routing depends on
data types, frames, and function shapes.

`GROUPS` / `EXCLUDE` and constant-foldable parameter expressions (for example
`NTILE(2+1)`) are mydb capabilities beyond MySQL 8.0. `IGNORE NULLS` and
`DISTINCT` on window aggregates are explicitly rejected; windows cannot be
used in prohibited contexts such as WHERE/HAVING or generated columns.
For MySQL's restrictions on window frames/parameters see
[Window Function Restrictions](https://dev.mysql.com/doc/refman/8.0/en/window-function-restrictions.html);
for mydb-specific behavior, see [Window Functions](function/aggregate-window/README.md).

### SELECT INTO OUTFILE / DUMPFILE

**✅ MySQL 8.0 server-side file export.**

- All three `INTO` positions are supported — after the select list, before the locking clause, and at the end of the statement — together with CTEs, `TABLE`, VALUES-derived tables, UNION, and prepared statements; the legacy position after the select list produces deprecation warning 1287.
- `OUTFILE` supports `CHARACTER SET`, `FIELDS|COLUMNS TERMINATED/ENCLOSED/ESCAPED`, `LINES STARTING/TERMINATED`, multibyte delimiters, empty escape, fixed-row, NULL/NUL, and binary BLOBs; `DUMPFILE` concatenates single-row fields directly, writing one NUL for NULL.
- Files are created by the server using atomic `create_new` with mode `0640` and never overwrite an existing file (1086); query/cancellation/disk errors keep the partially written file. A bare file name goes into the current schema directory; a relative path with directories resolves against the data directory.
- Both the SELECT and global FILE privileges are validated before file creation; after canonicalizing the parent directory, the `secure_file_priv` empty/directory/NULL policy is enforced, blocking `..` and symlink escapes. `character_set_filesystem` controls the file-name byte encoding.
- Both ordinary and streaming SELECTs write directly to the cursor without passing through the client result set and without full materialization; on success, OK-packet affected rows and `ROW_COUNT()` both equal the exported row count.
- `select_into_buffer_size` (default 131072, 8192–2147479552, 4096-aligned), `select_into_disk_sync` (default OFF), and `select_into_disk_sync_delay` (0–31536000ms) support GLOBAL/SESSION and same-name `SET_VAR(...)` statement-level overrides. The writer buffer is charged to the unified governor's `StorageScratch` by actual capacity and returns to baseline after drop.

### Isolation Levels and Transactions

| Level | Status |
|---|---|
| READ COMMITTED (default) | ◑: statement-level consistent reads; native heap/MVCC and row locks, with no promise of InnoDB's lock layout |
| READ UNCOMMITTED | ◑: dirty reads allowed; native transaction/lock implementation |
| REPEATABLE READ | ◑: consistent reads use a fixed snapshot, without InnoDB gap/next-key locks |
| SERIALIZABLE | ◑: uses table-level shared locks to restrict concurrent writes; granularity differs from InnoDB range locks |

> **Important design decision**: mydb's RR consistent reads use snapshot
> isolation. Repeated consistent reads within a fixed snapshot do not see rows
> inserted later, but locking reads/writes do not establish InnoDB
> gap/next-key/predicate locks, so RR range locking cannot be relied upon to
> stop other transactions from inserting. For "no-insert-in-range" semantics,
> use SERIALIZABLE (table-level S locks). This is an engine design difference.
> MySQL's default isolation level is RR; for its exact locking semantics see
> [InnoDB Transaction Isolation Levels](https://dev.mysql.com/doc/refman/8.0/en/innodb-transaction-isolation-levels.html).

- **XA 2PC**: ✅ cross-session + crash recovery (❌ XA JOIN/RESUME/SUSPEND not supported).
- XA ACTIVE permits branch SQL while IDLE rejects ordinary queries; XID uniqueness covers ACTIVE/IDLE/
  PREPARED, and only one of racing terminations succeeds. Disconnects roll back unprepared branches, while prepared branches are retained for
  cross-session completion; under Strict, terminal states of empty/read-only prepared branches are likewise persisted, and recovery retains no locks of already-terminated
  branches. mydb allows rolling back this session's ACTIVE branch directly — a compatibility difference.
- `SET TRANSACTION` without SESSION/GLOBAL affects only the next transaction, including autocommit
  reads, and restores the session default afterwards; READ ONLY allows FOR SHARE and rejects FOR UPDATE and persistent-table writes.
  Historical validation of transaction/XA boundaries is archived in the project's internal engineering records (transaction-processing-fix-validation-2026-10-02).
- **SAVEPOINT**, **`LOCK TABLES`**: ✅

### EXPLAIN

**✅ `EXPLAIN FORMAT={TRADITIONAL|JSON|TREE} [ANALYZE]`, `EXPLAIN FOR CONNECTION`, `EXPLAIN TABLE t` (8.0.19+).**

- `INSERT` / `REPLACE` / `UPDATE` / `DELETE` all support the three formats. TRADITIONAL
  keeps MySQL's 12-column metadata, unavailable fields for target tables return SQL `NULL`, and partition-pruning results
  go into `partitions`; TREE/JSON show the mutation root node and the real source/candidate access plans.
- Plain DML `EXPLAIN` performs only privilege, object, column, partition, and plan validation: it scans no data, allocates no
  AUTO_INCREMENT, runs no triggers/FK, and writes no WAL/binlog.
- ⚠️ **mydb extension: `EXPLAIN ANALYZE` for the four DML kinds writes real data.**
  Under autocommit it commits normally; inside an explicit transaction it stays uncommitted and honors `COMMIT`/`ROLLBACK`.
  Data, errors, triggers, FK, binlog, warnings, and affected rows are identical to executing the same DML directly
  and happen exactly once. Do not treat it as a side-effect-free plan preview.
- COM_STMT_PREPARE metadata is fixed: static TRADITIONAL 12 columns, ANALYZE
  TRADITIONAL 17 columns, TREE/JSON a single `EXPLAIN` text column.
- `ANALYZE FORMAT=TREE/JSON` carries real per-operator `actual time/rows/loops/IO`.
  TREE's first-row/completion times and rows are per-loop averages; JSON additionally carries cumulative values, query wall time,
  operator-self I/O, and unassigned I/O. CTEs, recursive CTEs, derived tables, windows, and parallel workers share
  the same statement collector; fused nodes report attribution explicitly instead of synthesizing statistics. TREE shows query-level block reads
  (including zeros) in a separate `Query I/O:` header line, and reads that cannot be attributed to a visible operator use the
  `unassigned_*` annotation. Non-zero node I/O is annotated `io_scope=operator_self` (itself) or
  `io_scope=operator_interval_inclusive` (execution interval, including children); statistics of inclusive nodes must not be
  added directly. `physical_reads` / `cache_reads` are buffer-pool misses / hits,
  not row counts or deduplicated page counts; missed reads may still hit the OS cache. The hit ratio is
  `cache_reads / (cache_reads + physical_reads)`; JSON preserves structured query- and node-level statistics.
- `select_type` covers `DEPENDENT SUBQUERY` / `MATERIALIZED` / `UNCACHEABLE SUBQUERY`.
- `FORMAT=JSON`'s `cost_info` includes `query_cost` / `read_cost` / `eval_cost` / `prefix_cost` / `data_read_per_join`.
- Hash-join probe tables are marked `Using join buffer (hash join)`.
- Hash Join text equality keys are encoded with the common collation resolved from both side expressions; explicit
  `utf8mb4_bin` stays case/accent sensitive, and the NULL-safe bit of `<=>` enters the key specification.
  Under low memory it runs Grace spill and recursive repartitioning; hot keys use a bounded chunked
  Hash Join after depth exhaustion instead of silently falling back to a general NLJ. Related runtime metrics are
  exposed by `SHOW ENGINE MYDB STATUS`.
- ➕ The `TRADITIONAL`/`JSON` combinations for `ANALYZE` and ANALYZE over all DML are a deliberate mydb
  superset; MySQL 8.0's ANALYZE surface is narrower.

Query hints (`/*+ ... */` and table-level `USE/FORCE/IGNORE INDEX`) are covered in [docs/hints.md](hints.md).

---

## 5. SET and System Variables

**✅/◑ Common SET surface.**

- `SET [GLOBAL|SESSION|PERSIST|PERSIST_ONLY] var=val`, the `@@persist.x` / `@@persist_only.x` prefixes.
- `RESET PERSIST [IF EXISTS] [var]` + atomic JSON persistence at `<data_dir>/system/mysqld-auto.cnf` + startup `apply_persisted_on_startup` + `performance_schema.persisted_variables`.
- `SET TRANSACTION` (READ ONLY/WRITE + comma chaining), the `SET CHARACTER SET / CHARSET` alias, `SET ROLE ALL EXCEPT (...)`, `SET PASSWORD … REPLACE/RETAIN CURRENT PASSWORD`.
- `SET var = DEFAULT` truly resets; multi-assignment of system variables is atomic (all validations pass before applying), and user variables become visible left to right.
- `sql_log_bin` genuinely gates binlog through a per-statement thread-local guard; toggling it inside the same multi-statement `COM_QUERY` takes effect immediately for subsequent sub-statements.
- `information_schema_stats_expiry` supports GLOBAL/SESSION, connection inheritance, DEFAULT, SHOW/@@, and persisted configuration; default 86400 seconds, range 0–31536000. mydb's information-schema statistics read storage-engine metadata directly on every access without extra caching, so `0` guarantees direct reads and positive values never delay refresh; statistics may still be engine estimates. Out-of-range values return 1231 per mydb's unified integer-variable rules rather than MySQL's truncate-and-warn behavior.
- `show_gipk_in_create_table_and_information_schema` is a GLOBAL/SESSION boolean compatible with the default ON, supporting SET, DEFAULT, SHOW/@@, new-connection inheritance, and persisted configuration. mydb currently generates no GIPK automatically and does not support `sql_generate_invisible_primary_key`; switching to OFF does not hide user-defined primary keys or INVISIBLE columns, including a column named `my_row_id`.
- The plan-cache key includes an sql_mode fingerprint.
- **Configured values vs. actual resource budgets**: some MySQL allocator/buffer tuning items (such as
  `query_alloc_block_size`, `transaction_prealloc_size`, `sort_buffer_size`,
  `join_buffer_size`) are advisory settings that are accepted and echoed; they do not imply
  InnoDB/MySQL allocation behavior. Effective memory limits and spills are governed by the mydb governor, query budget, and the corresponding operators,
  so a successful SET must not be read as an identical physical effect.
- `explicit_defaults_for_timestamp` defaults to ON; GLOBAL/SESSION can switch it OFF, applying the legacy implicit DEFAULT/ON UPDATE rules for the first TIMESTAMP to tables created afterwards, and explicit NULL writes are handled per that session mode as well.
- ❌ `SET RESOURCE GROUP` explicitly returns 1235; no resource group is registered or bound.
- ❌ PG-style `SET TIME ZONE` is rejected with a hint toward the MySQL spelling.
- `init_connect` supports a dynamic GLOBAL string, startup configuration, and persistence; new logins run multiple SQL statements in order under the user's privileges and discard result sets, while administrators (`CONNECTION_ADMIN` / `SUPER`) and password-expired accounts are skipped. On failure the first command returns 1184, after which the connection is closed and uncommitted transactions cleaned up; RESET_CONNECTION/CHANGE_USER do not rerun it. SQL capabilities reuse the ordinary executor — see [startup configuration](configuration.md).
- ❌ `lower_case_table_names` is fixed read-only at `1`; `0` / `2` are not supported.
- Unknown variables return 1193 by default (an explicit `MYDB_SYSVAR_UNKNOWN=warn` enables the compatibility warning mode); `SET STATEMENT FOR` is supported.

### Privilege Gating

- All `SET GLOBAL` / `PERSIST` writes require the real `SYSTEM_VARIABLES_ADMIN` privilege bit (including role closure + SUPER implication); `PERSIST_ONLY` additionally requires `PERSIST_RO_VARIABLES_ADMIN`.
- `mandatory_roles` / `activate_all_roles_on_login` / `default_password_lifetime` are real sysvars.

### validate_password Component

➕/✅ opt-in via `INSTALL COMPONENT 'file://component_validate_password'` (off by default, aligned with MySQL; unknown components report 3529). Once enabled, plaintext passwords in CREATE/ALTER USER and SET PASSWORD are policy-checked (LOW/MEDIUM/STRONG); weak passwords report 1819. `authentication_policy` is settable with format validation (violations report 1231).

---

## 6. Built-in Function Library

**Common functions are implemented; the number of registrations does not imply
compatibility for every input shape.** Category overview
(concrete function names in [§6.1](#61-function-inventory)):

| Category | Coverage |
|---|---|
| Strings | Full common set, collation-aware case handling |
| Math | Common math functions; exact fixed-point arithmetic with half-away rounding |
| Date and time | fsp 0-6 included; named-time-zone `CONVERT_TZ` (uses host zoneinfo); `WEEK` modes 0-7 + `default_week_format` |
| JSON | 28+ (including `JSON_SCHEMA_VALID` / `JSON_SCHEMA_VALIDATION_REPORT`); binary JSONB storage |
| GIS | ◑ 111 official entry points registered; core constructors, accessors, measurements, predicates, aggregates, and spatial indexes work, but full compatibility acceptance has not been reached |
| Vector | L2/COSINE/DOT distances + `VECTOR_NORM`/`L2_NORMALIZE`/`SUBVECTOR`/`MAX_INNER_PRODUCT_DISTANCE` |
| Regex | `REGEXP_*`, linked to MySQL collations |
| Crypto/hash | AES mode matrix `aes-{128,192,256}-{ecb,cbc,cfb1,cfb8,cfb128,ofb,ctr}`; CTR is a mydb extension; SHA, CRC, UUID, INET |
| Locks | The `GET_LOCK` family |
| Aggregates | Common numeric/string/bit/JSON aggregates; `ST_COLLECT` as a plain aggregate; eligible aggregates support `OVER()` — see §4 for window limitations |
| Misc | `COERCIBILITY`, multi-locale `FORMAT(N,D,locale)`, MySQL-compatible `RAND(seed)` sequences, `GTID_SUBSET`/`GTID_SUBTRACT`, unary `BINARY`, `DEFAULT(col)`, `CAST AS YEAR\|REAL\|NCHAR`, `ICU_VERSION` |

> **Expression execution**: expressions without compiled support use scalar
> evaluation with the same SQL semantics.

### 6.1 Function Inventory

The following function names are available in mydb. See the individual function
pages for supported arguments, return types, and limitations.

**Strings**
`ASCII` `BIN` `BIT_LENGTH` `CHAR` `CHAR_LENGTH`/`CHARACTER_LENGTH` `CONCAT` `CONCAT_WS` `CONV` `ELT` `EXPORT_SET` `FIELD` `FIND_IN_SET` `FORMAT` `HEX` `INSERT` `INSTR` `LCASE`/`LOWER` `LEFT` `LENGTH` `LOAD_FILE` `LOCATE` `LPAD` `LTRIM` `MAKE_SET` `MID` `OCT` `OCTET_LENGTH` `ORD` `POSITION` `QUOTE` `QUOTE_IDENTIFIER` `REPEAT` `REPLACE` `REVERSE` `RIGHT` `RPAD` `RTRIM` `SOUNDEX` `SPACE` `STRCMP` `SUBSTR`/`SUBSTRING` `SUBSTRING_INDEX` `TRIM` `UCASE`/`UPPER` `UNHEX` `WEIGHT_STRING` `TO_BASE64` `FROM_BASE64` `REGEXP_LIKE` `REGEXP_INSTR` `REGEXP_REPLACE` `REGEXP_SUBSTR` `CHARSET` `COLLATION` `COERCIBILITY`

The LIKE ESCAPE may be a literal, placeholder, or scalar expression; single-character
validation happens at execution time and charset/collation semantics are preserved.
`REGEXP NULL` returns NULL. `NAME_CONST` accepts only the literal forms MySQL
allows; complex expressions return 1210.

#### `LOAD_FILE()` Server-Local File Reads

`LOAD_FILE(file_name)` reads files on the **mydb server host** per MySQL 8.0.46
semantics and returns a nullable `BLOB` (`binary` charset/collation). It requires
the current effective account or an activated role to hold the global `FILE`
privilege; `SQL SECURITY DEFINER` stored routines use the already-switched definer
security context. Missing privileges, a `NULL` argument, a nonexistent path,
ordinary I/O failures, and `secure_file_priv` prohibitions all return `NULL`;
directories, FIFOs, devices, and other non-regular files also return `NULL`,
checked before opening to avoid blocking. Relative paths resolve against the
mydb data directory, and the file name is encoded by `character_set_filesystem`.

`secure_file_priv=''` allows any location, a directory value allows only
canonical targets inside that directory, and `NULL` forbids reads; the
component-level prefix check blocks both `..` and symlinks escaping the
directory. Files exactly `max_allowed_packet` in size are readable; larger ones
return `NULL` and produce warning 1301. The implementation first intercepts
already-oversized files via metadata, then reads with bounded fixed stack
chunks, so a file that grows during the read cannot trigger unbounded
allocation.

The file result buffer counts toward the query's memory budget; insufficient
required memory returns 1037/HY001. Buffers are released when evaluation finishes,
including NULL, warning, and error results. `LOAD_FILE` is evaluated at execution
time without constant folding or result caching; queries using it execute
serially. Persistent expressions, including generated columns, functional
indexes, CHECK constraints, and defaults, reject it, so DDL does not read files.
Prepared executions re-read the file and recheck privileges and path restrictions.

**Math**
`ABS` `ACOS` `ASIN` `ATAN` `ATAN2` `BIT_COUNT` `CEIL`/`CEILING` `COS` `COT` `CRC32` `DEGREES` `DIV` `EXP` `FLOOR` `LN` `LOG` `LOG10` `LOG2` `MOD` `PI` `POW`/`POWER` `RADIANS` `RAND` `ROUND` `SIGN` `SIN` `SQRT` `TAN` `TRUNCATE`

Integer, floating-point, and DECIMAL overflows uniformly return 1690/22003. The
binding-time result type, rounding, scaling, and `+ - * / MOD` of DECIMAL are
consistent across execution modes; exact arithmetic uses
`i32(P≤9) / i64(P≤18) / i128(P≤38) / BigInt(P≤65)`, and checked overflow only
promotes — never falls back to `f64`. Wide values carry an explicit binary tag
in rows, vectors, aggregates, spills, undo, and B-trees, and the canonical hash
strips trailing zeros, so `1.0 = 1.00` with identical hashes. Results honor the
maximum 65-digit precision and 30-digit scale;
inputs and intermediates materialize at most 130 digits; `DOUBLE` to
`SIGNED`/`UNSIGNED` checks the half-open target interval before conversion.
Unary minus preserves the UNSIGNED type origin, so an unsigned column crossing
the signed domain reports 1690 while an exact numeric literal of the same shape
can still promote to DECIMAL. `DIV` rejects Geometry/Vector operands at binding
time and returns 1210 instead of evaluating an invalid structural value first
and reporting a secondary error. These semantics apply in all three
vectorization modes: `off|on|auto`.

**Date and Time** (including named time zones and localized date names)
`ADDDATE` `ADDTIME` `CONVERT_TZ` `CURDATE` `CURRENT_DATE` `CURRENT_TIME` `CURRENT_TIMESTAMP` `CURTIME` `DATE` `DATEDIFF` `DATE_ADD` `DATE_FORMAT` `DATE_SUB` `DAY` `DAYNAME` `DAYOFMONTH` `DAYOFWEEK` `DAYOFYEAR` `EXTRACT` `FROM_DAYS` `FROM_UNIXTIME` `GET_FORMAT` `HOUR` `LAST_DAY` `LOCALTIME` `LOCALTIMESTAMP` `MAKEDATE` `MAKETIME` `MICROSECOND` `MINUTE` `MONTH` `MONTHNAME` `NOW` `PERIOD_ADD` `PERIOD_DIFF` `QUARTER` `SECOND` `SEC_TO_TIME` `STR_TO_DATE` `SUBDATE` `SUBTIME` `SYSDATE` `TIME` `TIMEDIFF` `TIMESTAMP` `TIMESTAMPADD` `TIMESTAMPDIFF` `TIME_FORMAT` `TIME_TO_SEC` `TO_DAYS` `TO_SECONDS` `UNIX_TIMESTAMP` `UTC_DATE` `UTC_TIME` `UTC_TIMESTAMP` `WEEK` `WEEKDAY` `WEEKOFYEAR` `YEAR` `YEARWEEK`

`TIMESTAMP` columns convert session wall-clock time to a UTC instant on write
and convert back using the current session `time_zone` on read; `DATETIME`
always keeps timezone-less wall-clock time. Supported are `SYSTEM`, MySQL-legal
fixed offsets, and IANA named time zones from the host zoneinfo (including DST
and TZif future recurrence rules). mydb deliberately keeps an extended time
range: nonzero `TIMESTAMP` values span `1970-01-01 00:00:01` through
`9999-12-31 23:59:59.999999` UTC, with fractional precision 0–6. Assignment
checks the rounded session wall time and its UTC instant; zero values keep the
existing SQL-mode rules. A session time zone that would display year 10000
reports a clear error instead of clamping the year. `UNIX_TIMESTAMP`,
`FROM_UNIXTIME`, `CONVERT_TZ`, and `SET timestamp` also support the extended
future range. Exact decimal Unix seconds preserve microseconds; FLOAT/DOUBLE
and the floating-point `SET timestamp` override retain their numeric precision
limits. Heap, WAL recovery, and mydb native physical replication preserve the
full range. The TIMESTAMP2
encoding in mydb binlogs uses **u32 Unix seconds**, covering the 1970–2106 wire
format range; values before 1970 or beyond the u32-second range report 1264
rather than silently wrapping; with `log_bin=ON` and `sql_log_bin=ON`, such
DML fails atomically. Use native WAL replication for the full TIMESTAMP range;
setting `sql_log_bin=OFF` also permits local extended-range writes, which are
absent from the MySQL binlog. Wire-format encodability does not mean the
target MySQL's TIMESTAMP column accepts the extended range; MySQL 8.0 columns
still stop at 2038 — see
[Date and Time Types](https://dev.mysql.com/doc/refman/8.0/en/datetime.html).

**JSON** (binary JSONB storage)
`JSON_ARRAY` `JSON_ARRAY_APPEND` `JSON_ARRAY_INSERT` `JSON_CONTAINS` `JSON_CONTAINS_PATH` `JSON_DEPTH` `JSON_EXTRACT` `JSON_INSERT` `JSON_KEYS` `JSON_LENGTH` `JSON_MERGE` `JSON_MERGE_PATCH` `JSON_MERGE_PRESERVE` `JSON_OBJECT` `JSON_OVERLAPS` `JSON_PRETTY` `JSON_QUOTE` `JSON_REMOVE` `JSON_REPLACE` `JSON_SCHEMA_VALID` `JSON_SCHEMA_VALIDATION_REPORT` `JSON_SEARCH` `JSON_SET` `JSON_STORAGE_FREE` `JSON_STORAGE_SIZE` `JSON_TYPE` `JSON_UNQUOTE` `JSON_VALID` `JSON_VALUE` (aggregates `JSON_ARRAYAGG` / `JSON_OBJECTAGG`)

JSON paths support the MySQL 8.0 array range; JSON_VALUE supports RETURNING and
ON EMPTY/ON ERROR; JSON_TABLE supports implicit lateral, FOR ORDINALITY, and
sibling NESTED PATH union; indexes support composite keys with one MVI part.
Row-based binlog uses `MYSQL_TYPE_JSON` binary JSON, and the replica receiver
supports `PARTIAL_JSON` JSON_DIFF.
See [JSON Functions](function/json/README.md) for supported arguments and limitations.

JSON compatibility differences (deliberately retained): user variables keep the
JSON type instead of implicitly degrading to strings.
For example, after `SET @j=CAST('5' AS JSON)`, `@j='5'` returns 0 while
`(@j) MEMBER OF(CAST('[5]' AS JSON))` returns 1; both use consistent JSON typing
rules. Applications relying on MySQL's user-variable stringification must
convert explicitly.
MEMBER OF distinguishes a JSON DATE from a JSON string with the same text,
consistently whether using a multi-valued index or a scan;
it does not follow MySQL's plan-dependent behavior where a DATE multi-valued
index conflates the two.
Writing to JSON columns requires valid text, explicit JSON, or SQL NULL; numbers
and temporals should be explicitly `CAST(... AS JSON)`.

**GIS / `ST_*`**

The compatibility baseline is fixed at MySQL 8.0.46. mydb supports 111
official function names and aliases, supports the single `SRID + WKB`
storage/protocol format, column-level `SRID n`, strict Geometry coercion,
5152 SRS entries and 47 unit metadata entries, the `ST_Collect` aggregate, and
persistent MBR B-trees with a derived R-tree cache. Planar area, length, and
distance use the project's pure-Rust kernels;
`ST_Buffer_Strategy` tokens match MySQL's 12-byte format. Geographic
area/length/distance, polygon set operations, and Buffer without a reliable
pure-Rust implementation return clear errors — no silent degradation.

"Registered" here does not mean every input shape is accepted. Still missing
are SRS DDL/dynamic privileges and DD persistence, full EPSG projection
transforms, complete geographic/axis-order semantics, all Geometry combinations
for set operations, `ST_Collect` window/native vector channels, incremental
background R-trees, and full compatibility coverage. GIS is
therefore not yet marked "fully compatible". See [Spatial Functions](function/spatial/README.md)
for function-specific input restrictions and errors.

**Vector**
`VECTOR` `VEC_DISTANCE` `DISTANCE` `COSINE_DISTANCE` `L2_DISTANCE` `INNER_PRODUCT` `DOT_PRODUCT` `MAX_INNER_PRODUCT_DISTANCE` `VECTOR_DIM` `VECTOR_NORM` `L2_NORMALIZE` `TO_VECTOR`/`STRING_TO_VECTOR` `FROM_VECTOR`/`VECTOR_TO_STRING` `SUBVECTOR`

`EUCLIDEAN_DISTANCE` is a metric alias accepted by `DISTANCE(a,b,'EUCLIDEAN_DISTANCE')`,
not a scalar function name. See [DISTANCE](function/vector/distance.md) for metric aliases and ordering semantics.

**Aggregates**
`SUM` `COUNT` `AVG` `MIN` `MAX` `GROUP_CONCAT` `STD` `STDDEV` `STDDEV_POP` `STDDEV_SAMP` `VAR_POP` `VAR_SAMP` `VARIANCE` `BIT_AND` `BIT_OR` `BIT_XOR` `JSON_ARRAYAGG` `JSON_OBJECTAGG` `GROUPING`

`ANY_VALUE(expr)` is currently scalar identity. It does not create an aggregate row
or exempt an otherwise ungrouped column from `ONLY_FULL_GROUP_BY`; that grouped
shape still returns 1055. See [ANY_VALUE](function/aggregate-window/any_value.md).

For integer and DECIMAL inputs, `AVG` finalizes at its declared input scale plus
`div_precision_increment`. A wider outer CAST pads that rounded result;
it does not expose MySQL's internal decimal guard digits. Parallel DOUBLE
SUM/AVG may differ in the lowest bits as input and partial-sum ordering
changes. DECIMAL aggregation remains exact.

`BIT_AND/OR/XOR` support both unsigned 64-bit numerics and binary-string
aggregates; binary inputs must share one length within the MySQL limit, and
violations return 3513/3514 respectively. Row-based and vectorized aggregates
share one state implementation.

**Windows**
`ROW_NUMBER` `RANK` `DENSE_RANK` `PERCENT_RANK` `CUME_DIST` `NTILE` `LAG` `LEAD` `FIRST_VALUE` `LAST_VALUE` `NTH_VALUE`

**Encryption / hashing / encoding / masking / keyring**
`MD5` `SHA`/`SHA1` `SHA2` `AES_ENCRYPT` `AES_DECRYPT` `RANDOM_BYTES` `COMPRESS` `UNCOMPRESS` `UNCOMPRESSED_LENGTH` `VALIDATE_PASSWORD_STRENGTH` `STATEMENT_DIGEST` `STATEMENT_DIGEST_TEXT`; masking `MASK_INNER` `MASK_OUTER` `MASK_PAN` `MASK_PAN_RELAXED` `MASK_SSN`; keyring `KEYRING_KEY_{FETCH,GENERATE,LENGTH,REMOVE,STORE,TYPE}`

`STATEMENT_DIGEST`/`STATEMENT_DIGEST_TEXT` first validate that the input is a
single parseable SQL statement; illegal SQL and unsupported character sets
return the corresponding MySQL errors. They are volatile/restricted functions
that cannot be used in persistent expressions such as generated columns.

**Networking / UUID / locks / GTID / sequences / session / control flow**
- IP: `INET_ATON` `INET_NTOA` `INET6_ATON` `INET6_NTOA` `IS_IPV4` `IS_IPV4_COMPAT` `IS_IPV4_MAPPED` `IS_IPV6`
- UUID: `UUID` `UUID_SHORT` `UUID_TO_BIN` `BIN_TO_UUID` `IS_UUID`
- Locks: `GET_LOCK` `RELEASE_LOCK` `RELEASE_ALL_LOCKS` `IS_FREE_LOCK` `IS_USED_LOCK`
- GTID: `GTID_SUBSET` `GTID_SUBTRACT`. Replication wait helpers `MASTER_POS_WAIT`, `SOURCE_POS_WAIT`, and `WAIT_FOR_EXECUTED_GTID_SET` are recognized but unsupported (1235).
- Sequences: `NEXTVAL` `CURRVAL` `LASTVAL` `SETVAL`
- Session/info: `DATABASE`/`SCHEMA` `USER` `CURRENT_USER` `SESSION_USER` `SYSTEM_USER` `CURRENT_ROLE` `VERSION` `CONNECTION_ID` `LAST_INSERT_ID` `ROW_COUNT` `FOUND_ROWS` `BENCHMARK` `SLEEP` `ICU_VERSION` `NAME_CONST` `SYS_GET_CONFIG` `MYSQL_QUERY_ATTRIBUTE_STRING`
- perf_schema helpers: `PS_CURRENT_THREAD_ID` `PS_THREAD_ID` `FORMAT_BYTES` `FORMAT_PICO_TIME` `FORMAT_PATH` etc.
- Access-check helpers `CAN_ACCESS_{COLUMN,DATABASE,TABLE,USER,VIEW}` are recognized but unsupported (1235); they are not public privilege-query APIs.
- Control flow / casts: `IF` `IFNULL` `ISNULL` `NULLIF` `COALESCE` `GREATEST` `LEAST` `CAST` `CONVERT`

> A supported function name does not imply that every argument form matches
> MySQL. See the function pages for limitations, including JSON schema validation
> and masking functions.

---

## 7. Stored Routines, Triggers, and Events

### Stored Procedures / Functions

**✅** `CREATE [DEFINER=] {PROCEDURE|FUNCTION} [IF NOT EXISTS]` (IN/OUT/INOUT, RETURNS, DETERMINISTIC, SQL DATA ACCESS, SQL SECURITY, COMMENT), `ALTER`/`DROP`, `CALL`, `SHOW CREATE`, `SHOW {PROCEDURE|FUNCTION} STATUS`, `I_S.ROUTINES` (31 columns)/`PARAMETERS` (16 columns).

Procedure bodies support the full procedural language: `BEGIN…END`, `DECLARE` variables/CONDITION/CURSOR/HANDLER, `SET`, `SELECT…INTO`, `IF`/`CASE`, `LOOP`/`WHILE`/`REPEAT`, `LEAVE`/`ITERATE`, `RETURN`, `OPEN`/`FETCH`/`CLOSE`, `SIGNAL`/`RESIGNAL`, nested `CALL`, in-procedure `PREPARE`/`EXECUTE` dynamic SQL, and `GET [CURRENT|STACKED] DIAGNOSTICS`.

- **Stored functions execute row by row in SQL expressions**: until the compiler can prove purity from the complete call graph, all user-defined functions execute serially per row; `DETERMINISTIC` / `NO SQL` remain declared metadata only and cannot alone trigger inlining or constant folding.
- Static SQL in routine bodies is validated at CREATE and compile-cached by schema_version + creation-time sql_mode; function expressions pin a `RoutineProgram` at parse time, so row-by-row execution no longer re-parses or contends for the global compile-cache lock.
- In-procedure SQL-level `PREPARE` uses a connection-scoped lifetime and pre-parsed ASTs that can execute/deallocate across `CALL`s; dynamic SQL captures no locals, and functions/triggers continue to be rejected with 1336.
- SQL PREPARE and COM_STMT prepared `CALL` support IN/OUT/INOUT placeholders; the binary protocol sets `SERVER_PS_OUT_PARAMS` on the OUT-parameter single-row result set.
- `GET CURRENT/STACKED DIAGNOSTICS` uses a multi-condition, 1-based condition area; an `SQLWARNING` HANDLER can capture class-01 warnings produced by a statement, and named CONDITIONs can be used with SIGNAL/RESIGNAL.
- Direct functions and static nested `CALL`s pin their version within the statement's MDL batch; both direct and nested calls check routine-level `EXECUTE`.
- Block-level locals, conditions, and cursors are lexically scoped; declaration order, duplicate declarations, and `LEAVE` / `ITERATE` labels are validated at creation/recompilation.
- Parameters, locals, OUT/INOUT, and RETURN reuse the DML type-conversion implementation; argument evaluation and IN/INOUT assignment use the caller's mode, while the procedure body/locals/RETURN use the creation-time `sql_mode`; on routine exit the caller's database, security identity, roles, and mode are restored.
- Cursors consume a `ResultCursor` with bounded streaming FETCH; they are released automatically when leaving the declaring block or unwinding an exception.
- HANDLER **specificity precedence** (errno > SQLSTATE > generic class); duplicate conditions in the same block and undeclared named conditions are rejected at CREATE. `SIGNAL`/`RESIGNAL` information items may use locals, parameters, user variables, and expressions.
- SQL SECURITY DEFINER validates routine EXECUTE for both the caller and the DEFINER; the rejection code is 1370, and in-body table privilege denials remain 1142.
- **sql_mode capture + replay** at CREATE/ALTER time (including the empty mode).
- The parser consumes optional sqlparser AST fields explicitly: index/SHOW
  modifiers not yet implemented return a clear Unsupported instead of degrading to the base statement; interior expression fragments must be fully consumed.
- Parser scratch memory and the bulk VALUES worker stack enter the unified governor's
  `ParserTransient` category; parallel workers use a process-level cap and fall back to serial parsing when the budget is insufficient.
- The handshake, `@@version`, binlog, and `/*!MMmmrr ... */` share the 8.0.46 version source of truth; parse errors
  preserve sqlparser's real line/column and the corresponding UTF-8 byte offset.
- `max_sp_recursion_depth` (default 0; procedures exceeding it report 1456; function recursion always reports 1424).
- Inside functions: no result sets (1415), no commits (1422), no dynamic SQL (1336), no writes to the calling statement's tables (1442).
- Non-SQL `LANGUAGE` is explicitly rejected; ❌ deliberately not done: `UNDO` handlers (MySQL lacks them too) and the physical `mysql.proc` table (removed in 8.0).

### Triggers

**✅** `{BEFORE|AFTER} {INSERT|UPDATE|DELETE} ... FOR EACH ROW [{FOLLOWS|PRECEDES}]`, `DROP TRIGGER`, `SHOW CREATE TRIGGER`, `SHOW TRIGGERS`, `I_S.TRIGGERS` (22 columns).

- Full row-level coverage: multi-row INSERT / INSERT…SELECT / ODKU / REPLACE / IGNORE / LOAD DATA, single-table + multi-table UPDATE/DELETE, and partitioned tables.
- NEW is writable only in BEFORE, OLD is never writable (1362); writes to the triggering table are rejected (1442); illegal `OLD` in INSERT / `NEW` in DELETE → 1363; generated columns are readable, assignment → 3105.
- Non-base-table targets: views → 1347, temporary tables → 1361; DEFINER privilege violation → 1227.
- ❌ Deliberately not done: `FOR EACH STATEMENT` (absent from MySQL 8.0), FK cascades firing no triggers (same as MySQL), `TRUNCATE` firing no triggers.

### Event Scheduler

**✅** `CREATE [DEFINER=][IF NOT EXISTS] EVENT [OR REPLACE]`, `ALTER`/`DROP EVENT`, `SHOW EVENTS`/`SHOW CREATE EVENT`, `information_schema.events`, the `event_scheduler` sysvar.

- Persisted `mysql.events` (sql_mode / time_zone snapshots); a per-engine background scheduler thread uses `Notify` wakeups and a bounded worker pool, independent events may run concurrently, and bodies check privileges under the definer identity.
- `ON SCHEDULE AT/EVERY … STARTS/ENDS`, `ON COMPLETION [NOT] PRESERVE`, calendar interval stepping (month-end clamp) + MICROSECOND + 11 compound units, and skipping missed periods.
- CREATE/ALTER compound bodies support `BEGIN … END`, locals, branches, loops, and exception handling; one-shot PRESERVE events keep their definition after completion and become DISABLED.
- Runtime modification after starting with `MYDB_EVENT_SCHEDULER=disabled` reports 1290; ON/OFF can be toggled dynamically.
- AT/STARTS/ENDS do not accept numeric times (including MySQL's compact numeric date spellings); Unix seconds require explicit `FROM_UNIXTIME()`. EVERY rejects non-positive intervals, and each component is capped at 2147483647.
- `LAST_EXECUTED` is shown in the event's time zone, and `DATABASE_COLLATION` stores the owning database's default collation. Syntax is in [CREATE EVENT](statements/programmatic/create-event.md).

---

## 8. Metadata and Observability

### information_schema / performance_schema / sys / mysql

| Schema | Status |
|---|---|
| `information_schema` | ◑ MySQL's fixed catalog lists 79 tables/733 columns; 76 tables/717 columns are supported, the other 3 catalog views are explicitly rejected; some registered tables honestly return empty |
| `performance_schema` | ◑ Tables with producers return real data; registered tables without producers honestly return empty |
| `sys` schema | ◑ Compatibility views built on IS/PS; results are limited by underlying producer coverage |
| `mysql` system schema | ◑ Privilege/help/tz/cost surfaces; tables without producers honestly return empty |

- **PS tables with real data sources**: `threads`, `setup_*`, `events_statements_*`, `events_waits_*`, `events_errors_summary_*`, `data_locks*`, `global_status`, `session_status`, `persisted_variables`.
- The 111 MySQL 8.0.46 Community `performance_schema` table definitions follow
  the MySQL schema; structural alignment does not imply identical metric semantics.
  Another 14 optional plugin/enterprise tables are retained separately. See
  [Performance Schema](performance-schema.md) for collection and memory limits.
- **Row-level privilege filtering**: schemas, tables, columns, views, routines,
  triggers, and events are filtered by the querying user's global, database,
  table, column, and active-role privileges. Diagnostic views enforce `PROCESS`
  where required. Runtime configuration cannot disable privilege filtering.
- **Data dictionary source of truth**: the catalog's source of truth is the data dictionary heap (`mysql.tables/columns/indexes/foreign_keys/…`); the raw DD heap is invisible to clients, exposing only the public SHOW / IS / mysql projections.

`information_schema` uses a single registry that currently pins **99 tables**:
76 MySQL 8.0.46 Community standard tables, 3 historical compatibility tables,
7 optional plugin-compatibility tables, and 13 mydb-native tables. Zone-map
diagnostics are exposed as `MYDB_ZONE_MAPS` and `MYDB_ZONE_MAP_STORAGE`.
RESOURCE_GROUPS, TABLESPACES, and TABLESPACES_EXTENSIONS are not registered and do not participate in SHOW/IS
catalog echoing; direct access explicitly returns 1235.
Every table must explicitly declare a real producer or an `HonestEmpty` with a
reason; a registered table without an implemented producer no longer implicitly
returns an empty set. `SHOW PLUGINS` and `INFORMATION_SCHEMA.PLUGINS` share one
plugin list, and `KEYWORDS` uses the full fixed 751-row dataset from MySQL 8.0.46.

Of the 76 currently supported Community tables, 65 use real producers and 11
honestly return empty. Buffer frames/CLOCK replacement, per-index resident
pages, the current session's temporary tables, and the full-text
configuration/persistent postings selected by the dynamic GLOBAL
`innodb_ft_aux_table` all return real data; the ones still returning empty are
PROFILING, the six page-compression views, the three full-text views for which
mydb has no separate queue/cache, and the standalone session temp tablespace
view. The machine matrix also records each object's data source of truth,
privilege policy, replication semantics, pushdown-eligible fields, and compatibility classification.

Standard table definitions follow MySQL 8.0.46, including character, numeric,
and temporal column metadata.

**System schemas**:

- `information_schema`: the full set of mainstream MySQL 8.0 views — `schemata`/`tables`/`columns`/`statistics`/`table_constraints`/`key_column_usage`/`views`/`routines`/`triggers`/`partitions`/`referential_constraints`/`parameters`/`events`/`check_constraints`/`character_sets`/`collations`/`engines`/`processlist`/`keywords`/`plugins`/`st_spatial_reference_systems`/`st_geometry_columns` plus the various `*_privileges`, `role_*_grants`, and `*_extensions`, along with the complete `innodb_*` family (`innodb_trx`, `innodb_lock_waits`, `innodb_buffer_page`, `innodb_tables`, `innodb_indexes`, `innodb_ft_*`, `innodb_metrics`, …).
- `performance_schema`: `events_statements_*`, `events_waits_*`, `events_transactions_*`, `events_stages_*`, `data_locks`/`data_lock_waits`, `metadata_locks`, `threads`, `setup_*`, `global_status`/`session_status`, `*_variables`, `host_cache`, `file_summary_by_instance`, `status_by_{account,host,thread,user}`, etc.
- `sys`: the `sys_config` base table + all summary/analysis views (`host_summary*`, `user_summary*`, `schema_*`, `statement_analysis`, `memory_*`, `io_*`, `waits_*`, …), each with an `x$` raw variant.

`sys.sys_config` settings are written to `system/sys_config.json` under the
data directory, with a single statement committing via atomic file replacement;
DML inside explicit transactions/XA returns a not-supported error to avoid a
spurious success result on rollback. It does not yet participate in
InnoDB/WAL/replication transactions; the InnoDB name in `SHOW CREATE TABLE`
exists purely for MySQL-compatible display.
- `mysql`: `user`, `db`, `tables_priv`, `columns_priv`, `procs_priv`, `global_grants`, `role_edges`, `default_roles`, `password_history`, `help_*`, `time_zone*`, `server_cost`/`engine_cost`, `innodb_{table,index}_stats`, `slave_*_info`, `gtid_executed`, etc.; `general_log`/`slow_log` are CSV-engine tables.

➕ **mydb-specific IS views** (namespaced by the `mydb_` prefix): `mydb_ddl_jobs`, `mydb_xa_recover`, `mydb_cursors`, `mydb_caches`, `mydb_plan_cache`, `mydb_vector_indexes`, `mydb_memory_advisor`, `mydb_undo_history`, `mydb_temporal_history`, `mydb_stats_health`, `mydb_zone_maps`, `mydb_zone_map_storage`, `mydb_sequences`.

The machine-readable [metadata compatibility matrix](mysql80-metadata-compat-matrix.json)
contains 99 registered surfaces: 76 Community, 3 legacy, 7 optional-plugin and
13 mydb-native entries. `mydb_sequences` exposes catalog-backed recovery and
synchronization state, filtered by SELECT/UPDATE privileges on each sequence.

### SHOW Statements

**✅ MySQL 8.0 P0 + P1 landed.** Coverage: `SHOW PLUGINS`, `SHOW OPEN TABLES`, `SHOW PRIVILEGES` (~60+ rows), `SHOW ENGINE <name> {STATUS|MUTEX}`, `SHOW [EXTENDED] COLUMNS/TABLES`, `SHOW STATUS` (GLOBAL/SESSION distinguished), `SHOW PROCEDURE/FUNCTION STATUS`, `SHOW BINARY LOGS` (with the `Encrypted` column), `SHOW MASTER LOGS`, `SHOW PROCESSLIST` (row-level scoping by the PROCESS privilege), account-privilege-filtered `SHOW {DATABASES,TABLES,TABLE STATUS}`, `SHOW SLAVE/REPLICA STATUS` (legacy column-name mapping + deprecation 1287).

- `SHOW {PROCEDURE|FUNCTION} CODE` reports a clean 1235.
- `SHOW STATUS`'s real collection covers connections/threads, `Bytes_*`, `Com_*` for supported commands,
  Handler/DML, buffer/WAL/lock waits, prepared/slow queries, sort spills, replication, and
  the `Mydb_*` extensions; GLOBAL/SESSION, `FLUSH STATUS`, PFS `status_by_*`, and
  `sys.metrics` share one unified registry. Per-item design and the unfaked gaps are in
  [`show-status.md`](show-status.md).
- ◑ Deferred: `SHOW TABLE STATUS` Checksum and real `SHOW PROFILE(S)` data.

### Observability

- **`SHOW ENGINE MYDB STATUS`** one-stop view: durability / WAL / group commit / checkpoint / buffer pool / dirty / active txns / snapshot SCN / undo / locks / deadlocks / per-cache hit ratios / cursors / XA / recovery / startup-validation, including checkpoint trigger reason/type, the explicit redo lower bound, failure/reclamation/restartpoint counters, and per-phase timings.
- ➕ **`SHOW MYDB MEMORY` / `SHOW MYDB MEMORY SESSIONS`**: the former shows current/peak/limit by functional category — data/index/LOB/vector raw pages, storage scratch, undo, SQL cache/executor, hit ratios, and the statistics scope; governed accounting and cache details are distinguished by an attribution field, with only rows marked YES contributing to totals — while the latter shows every connection's active query budget, spills, and retained prepared/CTE caches. Complex ASTs/plans use stable estimates marked `ESTIMATE`, governed categories are marked `CHARGED`, and workspace reservations are marked `RESERVATION`; `NULL` is returned when the OS cannot be sampled or hit ratios have no samples. The session view contains no SQL text.
- **Three logs** + `log_output={FILE|TABLE|FILE,TABLE|NONE}`: slow log, general log, error log.
- **`KILL CONNECTION|QUERY`**, **controlled shutdown/restart** (`SHUTDOWN` / `COM_SHUTDOWN` / `RESTART`, all requiring the `SHUTDOWN` privilege; SIGINT/SIGTERM follow the same graceful sequence).

---

## 9. Vector Search

➕ **mydb extension** (MySQL 8.0.46 Community Server has no native vector type or vector index).

- **`VECTOR(n, datatype)`** type: FLOAT32 (default), FLOAT16, BFLOAT16, INT8 and BINARY with native payload storage.
- **Distance functions**: L2 / COSINE / DOT / HAMMING / JACCARD + the pgvector infix operators `<->` / `<#>` (`<=>` conflicts with NULL-safe-eq and is deliberately omitted).
- **HNSW**: `CREATE INDEX … USING HNSW`, with `WITH (metric=..., m=..., ef_construction=..., ef_search=..., quantization=...)`; compressed candidates are re-ranked using visible native heap values.
- **IVF**: `CREATE INDEX … USING IVF`, with `WITH (metric=..., nlist=..., nprobe=..., kmeans_iters=..., train_sample=..., quantization=...)`; queries probe by centroid and inverted list, and `OPTIMIZE TABLE` can retrain.
- **Quantization**: `none` (default), SQ8, SQ4, PQ, and scaled centered BQ for numeric L2/COSINE/DOT. BINARY indexes reject quantization. COMMENT is descriptive text and former parameter syntax is rejected.
- **Training and diagnostics**: PQ requires at least `2^pq_bits` training rows; pending models execute exact scans. `EXPLAIN` reports quantization and the effective re-ranking pool; `information_schema.mydb_vector_indexes` reports configuration, training state, model/code bytes, exceptions, and query counters. See [vector datatypes and indexes](vector-indexes.md).
- **Queries**: `ORDER BY DISTANCE(...) LIMIT k` goes through the unified `VectorIndexScan`; no index, small tables, or `NO_VECTOR_INDEX` take the exact `VectorBruteScan` (partitioned tables included).
- **Parameterized KNN**: prepared / auto-param; the plan cache keys the query-vector expression instead of hard-coding the first vector.
- **Transactions and boundaries**: transaction deltas provide read-your-writes; shared vector index changes follow commit, rollback, savepoint, and XA boundaries. Invalid vector values report 50001 and dimension mismatches report 50002; cosine indexes reject zero vectors. LIMIT/OFFSET and visibility rules apply to both exact and approximate paths, but ANN candidate selection does not guarantee the same nearest-neighbor rows or recall as a brute-force scan.
- **Two storage backends** (`MYDB_VECTOR_STORAGE`, default `paged`): HNSW and native IVF use the shared `.hvec` page container, both incorporated into the buffer pool and physical WAL; the `memory` backend uses in-memory graph/inverted structures respectively and serves as the diffing oracle.
- **Operations/import**: `LOAD DATA`/`LOAD XML` accept `[x,…]` text; `MYDB_LOAD_DATA_VECTOR_FORMAT=base64` accepts little-endian f32; `OPTIMIZE TABLE` performs HNSW compaction or IVF retraining.
- **Replication**: mydb row-based binlog encodes VECTOR as a MySQL `LONG BLOB` with a `MYDBVEC2` typed envelope; MySQL can store it losslessly as a BLOB, and a mydb replica validates datatype, dimension and payload.
- GUCs: `mydb_hnsw_*`, `mydb_ivf_*`; hints are `HNSW_EF(n)`, `IVF_NPROBE(n)`, and `NO_VECTOR_INDEX`.

---

## 10. Replication

mydb ships **two fully independent** replication stacks, distinguished in the SQL surface by the `MYDB` keyword.

### MySQL → mydb (as a Replica Receiver)

**◑ Row-based replica receiver.** This capability depends on the `mysql-replica` feature, which is enabled in the default build.

- **IO / SQL threads physically separated**: the IO thread pulls the source binlog into the relay log, and the SQL thread tails the relay and applies per transaction; `START|STOP REPLICA [IO_THREAD|SQL_THREAD]` start/stop them independently.
- `CHANGE REPLICATION SOURCE TO` / `RESET REPLICA`; file/position, GTID auto-positioning, filtering, `SOURCE_DELAY`, `SOURCE_CONNECT_RETRY`, TLS/mTLS, and `START REPLICA UNTIL` genuinely work. `SOURCE_RETRY_COUNT`, custom heartbeats, connection compression, and TLS version/ciphersuite are △ accepted with warnings.
- `SHOW REPLICA STATUS` aligned to 60 columns + separated IO/SQL thread states + real error backfill + `SHOW RELAYLOG EVENTS`.
- The connection requires the source to use `binlog_format=ROW` / `binlog_row_image=FULL`, otherwise 1235.
  **The receiver supports `PARTIAL_JSON`**: JSON_DIFF is applied to the full before-image before writing;
  a missing full image or mismatched target type fails explicitly. This does not mean MINIMAL/NOBLOB is received.
- ◑ Single SQL applier (no MTS multi-threaded/parallel apply; `replica_parallel_*` is advisory); consecutive
  row-only source transactions still commit independently in order but may share a bounded durability barrier at the highest WAL LSN.

### mydb → MySQL (as a Binlog Source)

**◑** `sync_binlog` / `max_binlog_size` / `log_replica_updates` are writable at runtime.
`sync_binlog=N` counts commit groups and `max_binlog_size` controls transaction-boundary
rotation. `max_binlog_cache_size`, `max_binlog_stmt_cache_size`, and `expire_logs_days`
are read-only rather than writable stubs. `binlog_format` / `binlog_row_image` /
`binlog_row_metadata` and friends are
boundary-validated (supported values are idempotent, others report 1231).

GTID dump excludes already executed transactions throughout the stream, including
sets with holes and exclusions spanning bounded batches or rotated files.
Character column row bytes and TABLE_MAP charset metadata follow the declared
column charset; QUERY_EVENT SQL follows its advertised client charset, on both
source encoding and replica decoding. Binary columns retain their original bytes.
Ordinary DDL uses standalone GTID/QUERY events; atomic CTAS, including an empty
result, uses BEGIN/CREATE … START TRANSACTION/rows/XID and the appropriate GTID
statement flag. Statement-based data changes in QUERY_EVENT are rejected even
when a source session overrides the previously checked global ROW setting.
Consistent snapshots pin file position and GTID set together. When restoring logical dumps from a binlog-enabled source, use
`mysqldump --set-gtid-purged=OFF`: importing `gtid_purged` is currently rejected
as read-only (1238). Default `mysqldump` table locking supports views and locks
their underlying tables.

- **Multiple statements per packet**: a single `COM_QUERY` under `CLIENT_MULTI_STATEMENTS` can mix session state, DDL, and DML; autocommit DML each forms its own GTID/XID, explicit transactions are aggregated and published once, and DDL writes an independent QUERY_EVENT at implicit-commit boundaries; the first error stops processing and subsequent statements are not logged.
- **Semisynchronous source side**: `rpl_semi_sync_source_*` (+ the legacy `master_*` aliases) are real sysvars, `Rpl_semi_sync_source_*` SHOW STATUS is real, and the synchronous-ack release-commit loop works end to end . **OFF by default**.
- ❌ The source supports only `binlog_row_image=FULL` (no MINIMAL/NOBLOB or `PARTIAL_JSON`); ❌ the source supports only the ROW format (no STATEMENT/MIXED).

### mydb → mydb Native Physical Replication

➕ **mydb extension** (`MYDB_REPL_PORT` 3316): protocol v2, format fingerprints,
mTLS, received/durable/applied watermarks, slots/retention, staged base backups, timeline/fencing,
and manual controlled switchover. SQL surface: `CHANGE MYDB MASTER TO` / `START|STOP|RESET MYDB REPLICATION` /
`STOP|RESUME MASTER WRITES` / controlled or `FORCE FENCED` `PROMOTE TO MASTER` /
`DEMOTE TO REPLICA` / `SHOW MYDB {MASTER|REPLICA} STATUS|REPLICAS`. No automatic leader election;
downstream MySQL binlog positions are not guaranteed to be continuous after promotion. See [`native-replication.md`](native-replication.md) for details.

---

## 11. Storage Engine and Persistence

mydb has a single physical storage engine (named `MYDB`) that implements MVCC,
WAL, undo, and crash recovery. `SHOW ENGINES` and `information_schema.ENGINES`
also show the `InnoDB` compatibility alias;
`ENGINE=InnoDB` is still executed by the mydb engine. It neither reads nor
writes InnoDB tablespaces, redo/undo, or data dictionary formats.

- **MVCC**: the source of truth for visibility is the heap row header; index-only scans check the heap MVCC
  header when necessary and can short-circuit when the B-tree leaf `page_max_trx_id` satisfies the safety condition. The project has removed
  visibility-map files.
- **WAL**: segmented, group commit, platform-adaptive sync method (`MYDB_WAL_SYNC_METHOD`: `fsync` by default on macOS, `fdatasync` on Linux; true power-loss resistance requires an explicit `fullfsync`). The redo phase of crash recovery can replay in parallel.
- **undo**: segmented + two-tier images (in-memory cache + on-disk segments), with `MYDB_UNDO_*` controlling retention/compression; a per-transaction undo cap guards against runaway large transactions.
- **LOB / overflow**: the inline ↔ overflow threshold is `mydb_lob_inline_max`; up to 4 GiB is supported; optional transparent lz4 compression (`mydb_lob_compression`).
- **Zone Map**: ➕ enabled by default for persistent user tables; persisted per storage object as **v6 `.zm` paged files**,
  with standard page v3 (16 KiB), managed through the shared buffer pool, page checksum/LSN, and physical redo.
  There is no separate summary cache or v5 manifest publication chain. Typed bounds, NULLs, and Exact/Bloom
  membership drive serial/parallel/vectorized pruning, always retaining the heap residual; it falls back conservatively when data is missing, summaries are untrusted,
  snapshot boundaries are unmet, or the optional memory budget is insufficient. Incompatible formats still refuse to start, and physical corruption is handled under the
  storage verification/recovery rules — no promise that all corruption can be ignored.
  `mydb_buffer_pool_quota_zone_map_pct` defaults to `0` (general eviction policy); the old private cache variables
  have been removed. See [Storage Format](storage-format.md) for current
  layouts and constraints.
- **Buffer pool and governor**: `MYDB_MEMORY_TARGET` derives the raw page and executor
  budgets; buffers, storage scratch, undo, SQL cache, executor, and others use unified reservations
  billed by allocation capacity. Optional caches are evicted/skipped, spillable operators go to disk,
  and non-reclaimable allocation failures return 1037/HY001.
- **Durability modes**: `MYDB_DURABILITY` (`strict` by default / `relaxed` for stress testing / `memory` for tests).

### Disk Layout

```
system/wal/                    WAL segments
system/undo/                   undo manifest + segments
system/replication/            role / progress / relay log / slots
system/tmp/                    temporary table/index/spill files
system/commit-ledger.v1        source of truth for AS OF commit-timeline correctness
system/history-index.v1        degradable sidecar that accelerates historical old keys
ddl/                           DDL intent log (crash safe)
objects/tables/<storage_id>/<generation>.{heap,lob,zm}
objects/indexes/<storage_id>/<generation>/<index_id>.{idx,hnsw,hvec}
mysql.sequences / mysql.sequence_values   sequence definitions and recovery state in catalog heaps
```

### Persistent Format Versions

Current format versions are listed in
[Storage Format Versions](storage-format-versions.md).
WAL v28 includes the generic PageDelta, and Zone Map pages are covered by physical backups, checkpoints, and native replication.

> mydb **keeps no legacy dev storage compatibility paths**. Version mismatch → use a new `MYDB_DATA_DIR` or rebuild. Physical format details for tables and B-trees are in [docs/table-and-btree-storage-format.md](table-and-btree-storage-format.md).

### Time Travel Queries

**◑** Standard table-level `FOR SYSTEM_TIME AS OF <expr>` is supported. The expression may use time literals,
prepared parameters, `NOW()`/`CURRENT_TIMESTAMP`, and deterministic temporal arithmetic, and is evaluated exactly once
at statement start. Base tables inside JOINs, subqueries, CTEs, UNIONs, and view definitions may have independent time points; views, CTEs,
derived tables, temporary tables, system tables, locking reads, and DML target tables themselves do not accept AS OF.
The history window is controlled by `mydb_undo_retention_ms`, default `0`; structural DDL boundaries return 50041, windows that are too old return
50040, and invalid times return 50042/22007. Historical KNN on HNSW/IVF deterministically falls back to an exact heap scan.

The persistent commit ledger and undo restart recovery are implemented; transaction-level `HistoryIndexBatch`, the persistent
`history-index.v1`, WAL tail repair, and corruption degradation let ordinary B-trees still hit historical old keys after restart.
On-disk sorted-run LSM, partition storage_ids, versioned FULLTEXT BM25, and the full performance gates are unfinished,
so this item remains marked partially supported.

---

## 12. Privileges and Security

**✅/◑ MySQL 8.0-style privilege model.**

Privilege checks cover DML, view and event definers, trigger pseudo-rows,
columns read by DML, role revocation, and object DDL and maintenance operations.
Cached plans do not bypass the current account's privileges.
The support markers in this section do not mean the privilege system has passed full production security acceptance.

- Static + dynamic privileges (`SHOW PRIVILEGES` exposes the full set, ~60+ rows).
- **Roles**: `CREATE ROLE`, `GRANT`/`REVOKE` (requires `ROLE_ADMIN` or `WITH ADMIN OPTION`, otherwise 3523), `SET ROLE ALL EXCEPT`, `SET DEFAULT ROLE`, `mandatory_roles`, `activate_all_roles_on_login`. The role closure is reflected in `SHOW GRANTS`.
- **Account-management atomicity**: multi-account `CREATE/DROP/RENAME` fully validates before committing; statement-wide SSL/resource/password options apply to all accounts, and `IF [NOT] EXISTS` produces a 3162/3163 Note; target conflicts never overwrite accounts.
- **Dynamic and system privileges**: `SYSTEM_USER`, `ROLE_ADMIN`, `CONNECTION_ADMIN`, and friends are unified as registered dynamic privileges; the SQL system account is separated from the internal recovery/apply principal, and modifying the system account requires `SYSTEM_USER`.
- **Authorization extensions**: `GRANT ... AS ... WITH ROLE`, `CREATE USER ... DEFAULT ROLE`, scope validation, mandatory-role delete protection, and restriction propagation are wired up; `mysql.proxies_priv`/`password_history` return real rows. There is currently no proxy authentication plugin; `GRANT PROXY` returns 1235 (not supported); `REVOKE PROXY` can clean up existing grants.
- **Column-level grants**: column privileges are checked across subqueries; references that cannot be resolved to individual columns require table-level privileges.
- **Privilege enforcement**: checks include active roles and SUPER implications. `read_only`, `SET GLOBAL`, and row visibility in `SHOW` use the account's effective privileges rather than treating the name `root` as special.
- **Password policy**: the validate_password component (see [§5](#5-set-and-system-variables)); `SET PASSWORD … REPLACE/RETAIN CURRENT PASSWORD`; `default_password_lifetime` drives login-time password expiry.
- **Credential protection**: password history stores plugin hashes and checks salted hashes via the plugin verifier; `SET PASSWORD TO RANDOM` uses the OS CSPRNG; credential DDL is uniformly masked in general/slow/audit/PFS/console logs.

---

## 13. Key Boundaries and Unsupported Items

### SHOW PROCESSLIST and Thread States

`SHOW [FULL] PROCESSLIST`, `information_schema.PROCESSLIST`,
`performance_schema.processlist/threads`, and `sys.processlist` share one live
snapshot. `State`/`Info` return SQL `NULL` for connections with no default
database selected and for idle connections; foreground connections include the
client source port in `Host`, and non-`FULL` `Info` is truncated to 100 Unicode
characters. When a protocol prepared statement executes, `Command=Execute` and
`Info` keeps the original PREPARE SQL.

Wired foreground states include `starting`, `checking permissions`, `Opening tables`,
`optimizing`, `preparing`, `executing`, `update`, `updating`,
`Sorting for order`, `Sorting for group`, `Removing duplicates`, `User sleep`,
`User lock`, the main DDL/maintenance states, object-type-differentiated
metadata lock waits, `Waiting on cond`, `Rolling back`, `Sending to client`,
`query end`, and `Killed`.
Row-lock waits reuse the current DML state (for example `updating`), consistent
with MySQL 8.0.

Only service threads visible in the MySQL process list are shown in the
background: the event scheduler and its execution workers, Binlog Dump, and the
replication IO/SQL threads. Replication states use MySQL 8.0.26+
source/replica terminology; the replication SQL thread's `Time` is the seconds
since the last applied source event. Internal maintenance threads such as
ordinary undo purge, checkpoint, automatic statistics, and Zone Map are not
shown. Background observation rows are governed by the unified `SystemMetadata`
memory budget; on insufficient budget the optional row is skipped without
stopping the background service. This change modifies no persistent format
versions.

Unsupported features normally return `ER_NOT_SUPPORTED_YET` (1235/42000). Syntax errors, invalid variable values, and
object states use more specific MySQL errnos; unwired statements must not
degenerate into no-ops.

**SQL and Objects**

- The `HANDLER` statement.
- Cross-base-table UPDATE (1393) and DELETE (1395) on join views and other unimplemented shapes;
  single-target UPDATE over INNER/CROSS JOIN directly projected views is supported — see §2 for the specific limitations.
- UPDATE/DELETE `RETURNING` that genuinely modifies multiple target tables; single-target JOIN DML is available.
- `MATCH ... AGAINST ... WITH QUERY EXPANSION`.
- The non-standard spelling `GROUP BY ... WITH CUBE`; use the mydb extension
  `GROUP BY CUBE(...)` instead. `ROLLUP(...)`, CUBE, and row constructors themselves are already supported.
- MERGE, PostgreSQL `ON CONFLICT`, UPDATE FROM, and data-modifying CTEs.
- Non-SQL LANGUAGE for stored routines and UNDO handlers; triggers have no `FOR EACH STATEMENT`
  (MySQL 8.0 itself also lacks the latter two).
- GIS geographic area/length/distance, full EPSG transforms, some polygon set
  operations, and Buffer combinations that are not yet supported return clear errors.

**Transactions and Storage**

- InnoDB gap/next-key/predicate locks under RR; use SERIALIZABLE table-level
  shared locks for no-insert-in-range semantics.
- XA JOIN/RESUME/SUSPEND.
- The MEMORY, CSV, ARCHIVE, MyISAM, and other physical engines for user tables;
  the corresponding `ENGINE=` returns 1286 and is not accepted as a metadata
  placeholder. CSV-compatible display of system log tables does not mean CSV
  user tables can be created.
- InnoDB tablespace/redo/undo/data dictionary file compatibility, and the InnoDB plugin ABI.
- DDL, settings, and dedicated catalog views for TABLESPACE and RESOURCE GROUP return explicit not-supported errors; no name catalogs are maintained and no metadata-level placeholder operations are accepted. INNODB physical table file views still reflect actual storage.

**Protocol and Ecosystem**

- MySQL X Protocol, X DevAPI, and Document Store; `mysqlsh --sql` works with
  ordinary client capabilities when using the classic
  protocol — X mode is unavailable.
- Group Replication, NDB Cluster, the MySQL Router management protocol, and the MySQL Clone plugin.

**Replication**

- STATEMENT/MIXED formats as a binlog source.
- MINIMAL/NOBLOB and PARTIAL_JSON for the source row image (currently only FULL is emitted).
- MTS parallel apply for a MySQL replica; the current single SQL applier executes in order.
- `CHANGE REPLICATION SOURCE TO` clauses that are merely advisory, such as
  replica connection compression and custom heartbeat/retry-count limits.

---

## 14. Version and Runtime

- **Product version**: `1.0.0`; the MySQL handshake/SQL compatibility baseline remains 8.0.46, and the version string may append the product version and build date.
- **Default port**: 8738; **pre-created database**: `test` (`DATABASE()` is NULL when the connection specifies no database, and unqualified table names report 1046); **default data directory**: `./data`.
- **Default account**: `'root'@'localhost'` / `root` (bootstrap only; changeable via `MYDB_ROOT_PASSWORD`);
  the local TCP loopback matches localhost, and no remote root account is created by default.
- **Default authentication plugin**: `caching_sha2_password`.
- **Default build**: `tls`, `fast-alloc` (mimalloc), `mysql-replica`; the last also enables
  the MySQL client and zstd. Capabilities of a `--no-default-features` build must be judged
  by the actual features; tool binaries additionally need `--features tools`.

```bash
# Start the server
MYDB_DATA_DIR=/tmp/mydb/compat-data cargo run -- --host 127.0.0.1 --port 3307

# Smoke test
mysql -h127.0.0.1 -P3307 -uroot -proot test -e "SELECT 1"
```

JDBC: `jdbc:mysql://127.0.0.1:3307/test?useSSL=false&allowPublicKeyRetrieval=true`.
Startup configuration and TLS options are in [`configuration.md`](configuration.md).

---

## 15. Intentional Differences and Migration Notes

### Functions and Expressions

The following expression differences are retained:

- String vs. DECIMAL comparisons keep exact fixed-point semantics instead of simulating the precision loss of a DOUBLE conversion.
- Regex continues to use a resource-bounded automaton engine. Backreferences report explicit errors; Unicode case folding uses simple folding
  and does not promise ICU's multi-character full folding (for example `ss`/`ß`). This is the trade-off between resource controllability and full ICU compatibility.
- A negative occurrence in `REGEXP_REPLACE` explicitly reports 1210, keeping strict parameter checking.
- `aes-*-ctr` remains a mydb extension.
- The base `utf8mb4_0900_ai_ci` of `WEIGHT_STRING` uses Unicode 9.0 DUCET primary weight data;
  legacy UTF-8 `_bin` emits three-byte code point weights. `utf8mb3_general_ci` and
  `utf8mb4_general_ci` use two-byte BMP primary weights, including non-ASCII characters.
  Unimplemented UCA versions/language tailoring rules explicitly return 1235.
- Unconstrained FLOAT text-protocol output preserves the stored f32 value when parsed as
  f64, instead of truncating it to six significant digits. Explicit fixed-decimal formatting,
  ZEROFILL, and character-assignment formatting retain their existing rules.
- AND/OR chains are balanced without changing operand evaluation order and have no separate
  fixed operator-count cap; memory, packet and syntax admission still apply. Scalar-expression
  depth is limited to 1,024; pure parentheses do not consume this semantic depth budget.
  Set-operation branches are stack-derived, clamped to 128–1,024: the default 8 MiB stack
  admits 128 with debug assertions and 1,024 in optimized builds. See [limits](limits.md#sql-language-limits).
  Stored procedures support `max_sp_recursion_depth=255` with governed stack growth.
- Error/warning texts are not guaranteed to match verbatim; diagnostic counts for the same class and the nullability/defaults of views/CTAS still differ — see the raw measured results.

### Metadata Lifecycle and Identifier Case

- Database, table, and other object names are stored lowercased uniformly and looked up case-insensitively;
  `@@global.lower_case_table_names` is fixed read-only at `1`, with no support for `0` (case-preserving and case-sensitive)
  or `2` modes. `CREATE DATABASE MyCaseDb` shows up as `mycasedb` in SHOW and information_schema.
  This policy is retained for cross-platform consistency and differs from MySQL's Linux default configuration; see the
  [MySQL system variable documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lower_case_table_names).
- After a table or view a view depends on is dropped, mydb keeps an explicit invalidation marker and queries report 1356.
  Recreating a same-named table does not automatically restore the view; `CREATE OR REPLACE VIEW` must rebind explicitly.
  This policy prevents a new object with different semantics from taking over an old view merely because the name matches; the cost is incompatibility with MySQL's
  query-time dependency checking and same-name object recovery flow. This difference is retained and must not be read as implemented MySQL automatic recovery.
- Event runtime watermarks and scheduler-updated states persist but do not change the event definition version;
  a user-issued ALTER EVENT updates the definition version. The definition version deduplicates scheduled tasks and protects watermark write-back and post-completion DROP/DISABLE, preventing stale tasks from modifying a post-ALTER definition.

### Query Engine

- `TIME` vs. string comparisons preserve operand microsecond precision instead of re-rounding the comparison constant to the stored column's fsp; time literals also compare as time values. Hence `TIME '10:00:00' = '10:00:00.000000'` is true. This keeps exact-value semantics and requires index, scan, and row-based/vectorized agreement.
- An UPDATE target table may also appear in a scalar subquery; non-correlated scalar subqueries in reports materialize before writes, avoiding the extra derived-table rewrites demanded by MySQL's 1093 restriction.
- `PI()` returns the double-precision value `3.141592653589793` and is not truncated to six decimals to match the display form.
- Error codes and SQLSTATEs are aligned; messages keep mydb's clear descriptions and position hints and are not required to copy MySQL's text verbatim.
- `ONLY_FULL_GROUP_BY` is on by default; loose grouping requires explicitly removing it from the session `sql_mode`. `DISTINCT` does not allow ordering by unprojected source columns.

## Related Documentation

- [Product Feature Overview](feature-overview.md) — capability map and product boundaries
- [Startup Configuration](configuration.md) — configuration files, environment variables, and precedence
- [ALTER TABLE](statements/ddl/alter-table.md) — supported actions, algorithms, and locking boundaries
- [docs/hints.md](hints.md) — query hint syntax and behavior
- [DML RETURNING](design/dml-returning.md) — PostgreSQL 18-style extension semantics
- [docs/table-and-btree-storage-format.md](table-and-btree-storage-format.md) — physical storage format for tables and B-trees
- [docs/mysql80-metadata-compat-matrix.json](mysql80-metadata-compat-matrix.json) — machine-readable metadata compatibility matrix
