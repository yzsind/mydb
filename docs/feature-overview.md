# mydb Product Feature Overview

This document summarizes mydb's product capabilities in the default build. It describes what mydb supports, while [MySQL Compatibility](mysql80-compatibility.md) explains the differences from MySQL 8.0.46.

mydb is a single-node general-purpose relational database serving clients over the MySQL classic protocol. It is currently in development toward its first product release and is suitable for feature validation, compatibility testing, and evaluation in controlled environments; it should not yet be regarded as a release that can directly replace production MySQL.

## 1. Capability Map

| Area | Current implementation |
| --- | --- |
| Client access | MySQL protocol v10, TLS/mTLS, three MySQL authentication plugins, text/binary result sets, prepared statements, read-only server-side cursors, multi-statements |
| SQL | Common DDL/DML, complex SELECT, CTE/recursive CTE, set operations, subqueries, window functions, ROLLUP/CUBE/GROUPING SETS, views, routines, triggers, events |
| Data and indexes | Mainstream MySQL scalar types, JSONB, GIS, `VECTOR(n)`; B-tree, FULLTEXT, SPATIAL, HNSW, IVF |
| Query execution | CBO, statistics/histograms, join order selection, Hash Join/INLJ/Adaptive Join, parallel execution, row-wise and vectorized execution, spill |
| Transactions and storage | MVCC, WAL/redo, undo, checkpoint, buffer pool, row locks/deadlock detection, SAVEPOINT, XA 2PC, crash recovery, table-level time travel |
| Security and administration | Accounts, roles, static/dynamic privileges, column-level privileges, password policies, system variable persistence, system schemas, logging, diagnostics, and graceful restart |
| Replication | mydb as a MySQL ROW/FULL binlog source; MySQL→mydb replica receiver included in every build; a separate mydb→mydb physical replication path |

## 2. SQL and Database Objects

### 2.1 DDL

- Databases, tables, temporary tables, indexes, views, sequences, stored procedures/functions, triggers, and events all have CREATE/ALTER/DROP or corresponding lifecycle statements.
- `CREATE TABLE` supports column-level and table-level primary keys, unique keys, CHECK, foreign keys, generated columns, invisible columns, default expressions, `ON UPDATE`, column positions, and common table options.
- CTAS supports explicit column definitions, table options, and partitioning; plain SELECT sources are loaded block by block in streaming fashion. Persistent CTAS stays crash-atomic through DDL intents and data dictionary generations.
- `ALTER TABLE` supports ADD/DROP/RENAME/MODIFY/CHANGE COLUMN, index, and constraint actions, and recognizes `ALGORITHM=INSTANT|INPLACE|COPY|DEFAULT`. Online ADD INDEX permits concurrent access during the Building phase, but the SQL statement returns only at Public or on failure; internal workers never report OK early.
- RANGE, LIST, HASH, and KEY partitioning plus their COLUMNS variants can be created, pruned, and reorganized; repartitioning uses build-then-swap with an intent log.
- `ANALYZE TABLE` supports sampling across multiple tables; a single table supports UPDATE/DROP HISTOGRAM.
- `CHECK`, `REPAIR`, `OPTIMIZE`, and `CHECKSUM TABLE` provide maintenance entry points shaped like MySQL's. REPAIR does not perform MyISAM-style repair on the native transactional engine; OPTIMIZE performs an online physical rebuild of heap/index/LOB storage, refreshes statistics and Zone Maps, and rebuilds special indexes. See [supported OPTIMIZE scope](mysql80-compatibility.md#optimize-table).
- TABLESPACE and RESOURCE GROUP are currently unsupported. Related DDL/SET statements, named tablespace options, and dedicated catalog views explicitly return 1235; no name or attribute catalog is maintained.

mydb has a single native transactional storage engine. `ENGINE=InnoDB` and `ENGINE=mydb` use the same implementation; any other name not registered in `SHOW ENGINES` returns MySQL error 1286, and a physical engine promise is never silently downgraded to metadata.

### 2.2 DML

- INSERT supports VALUES, SET, INSERT…SELECT, multiple rows, `IGNORE`, the `DELAYED` compatibility hint, `ON DUPLICATE KEY UPDATE`, and MySQL 8.0.19 row aliases.
- REPLACE implements the MySQL semantics of "delete all conflicting rows, then insert", covering multiple unique keys, triggers, foreign-key cascades, and auto-increment.
- UPDATE/DELETE support single-table and multi-table JOIN forms, `ORDER BY`, `LIMIT`, `PARTITION`, CTE-driven sources, and `IGNORE`. The non-preserved side of a LEFT JOIN can be the target, but virtual rows produced by NULL padding are never written.
- `LOAD DATA [LOCAL] INFILE` supports character sets, field/line formats, user variables, SET expressions, sql_mode conversions, and progress; `LOAD XML` supports attributes, nested elements, and `<field name=...>`.
- `SELECT ... INTO @var`, `SELECT ... INTO OUTFILE|DUMPFILE`, and `DO expr` are available; `LOAD_FILE()` and file exports are uniformly governed by the FILE privilege, `secure_file_priv`, file size limits, and the unified memory governor.

mydb also offers a PostgreSQL 18-style `INSERT|UPDATE|DELETE ... RETURNING` extension that can reference `OLD`/`NEW` row images and returns results through a MySQL result set. RETURNING works for single-target JOIN DML and updatable views; an UPDATE/DELETE RETURNING that actually modifies multiple target tables fails explicitly with 1235. See [`design/dml-returning.md`](design/dml-returning.md) for detailed semantics.

### 2.3 Queries

- INNER/LEFT/RIGHT/CROSS/NATURAL JOIN with USING; derived tables and implicitly lateral `JSON_TABLE`. The mydb extension `generate_series` supports integers, exact fixed-point numbers, DATETIME+INTERVAL, `WITH ORDINALITY`, JOIN, and vectorized execution.
- Scalar, IN, EXISTS, and ANY/ALL subqueries; correlated subqueries can be rewritten into semi/anti/join or materialized plans, with a correct correlated execution path retained when rewriting does not apply.
- `UNION`, `INTERSECT`, `EXCEPT`, and their ALL variants.
- Plain CTEs, recursive CTEs, shared materialization, predicate pushdown, projection pruning, and result cache invalidation.
- Aggregation, DISTINCT, HAVING, and functional-dependency grouping; `WITH ROLLUP`, standard functional `ROLLUP(...)`, `CUBE(...)`, `GROUPING SETS(...)`, and `GROUPING()`.
- Window functions, ROWS/RANGE/GROUPS frames with EXCLUDE; ORDER BY/GROUP BY ordinal and alias rules.
- Row constructor comparisons, NULL-safe equality, LIKE/REGEXP, full-text MATCH, and expression semantics involving MySQL sql_mode, character sets, and collations.
- `FOR UPDATE`, `FOR SHARE`, NOWAIT, SKIP LOCKED.
- `EXPLAIN` in TRADITIONAL/JSON/TREE formats, `EXPLAIN ANALYZE`, DML EXPLAIN, `EXPLAIN FOR CONNECTION`, and `EXPLAIN TABLE`.

## 3. Data Types and Retrieval

### 3.1 Mainstream MySQL Types

The implemented types include:

- Integers: TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT, BOOL/BOOLEAN, including UNSIGNED and ZEROFILL metadata plus AUTO_INCREMENT.
- Numeric: FLOAT, DOUBLE, DECIMAL/NUMERIC; `DECIMAL(P,S)` covers MySQL's `P<=65, S<=30`, with `P<=38` using fixed-width exact coefficients and `P=39..65` using a wide fixed-point representation; fixed-point arithmetic preserves exact decimal semantics.
- Character and binary: CHAR/VARCHAR, BINARY/VARBINARY, four TEXT/BLOB sizes, ENUM, SET, BIT.
- Temporal: DATE, TIME, DATETIME, TIMESTAMP, YEAR, with support for 0–6 fractional-second digits.
- Structured types: the JSON and GEOMETRY families.
- mydb extension: `VECTOR(n)`.

Character sets, collations, PAD SPACE/NO PAD, binary strings, and the `BINARY` attribute on column definitions all participate in comparisons, sorting, LIKE, functions, and index encoding. `TIMESTAMP` round-trips through UTC using the session `time_zone`, while DATETIME preserves wall-clock time; MySQL 8's default `explicit_defaults_for_timestamp=ON` can be switched to the legacy OFF mode.

### 3.2 Indexes

| Index | Capabilities |
| --- | --- |
| B-tree | Primary key, unique, composite, covering with INCLUDE, prefix, descending, invisible, functional/expression indexes |
| Zone Map | Table/partition-level typed min/max, NULL, string-prefix, and selective-membership summaries; serial/parallel/vectorized scans all retain the heap residual |
| JSON MVI | Multi-valued keys via `CAST(json_expr AS ... ARRAY)`; one MVI part can combine with a regular part to form a composite key |
| FULLTEXT | Natural-language BM25, boolean mode, ngram parser; MATCH and safe LIKE ngram pre-filtering; [immutable backup queries](backup-and-recovery.md#51-fulltext-queries) |
| SPATIAL | Persistent MBR B-tree plus a governor-managed derived R-tree, with a final heap lookup to recheck exact predicates |
| HNSW | Graph-based ANN over native numeric/BINARY vectors; explicit none/SQ8/SQ4/PQ/BQ payloads, visible heap re-ranking, memory/paged backends |
| IVF | Inverted-file ANN over native numeric/BINARY vectors; nlist/nprobe, explicit none/SQ8/SQ4/PQ/BQ payloads, retraining, memory/paged backends |

### 3.3 JSON, GIS, and Vector

- JSON uses an internal binary JSONB, providing JSON paths, modification/query functions, JSON_VALUE, JSON_TABLE, schema validation, aggregation, and row-binlog binary JSON.
- GIS uses `SRID + WKB`, supporting Geometry subtypes, column-level SRIDs, construction/access/measurement/MBR and exact predicates, GeoJSON/Geohash, spatial indexes, and `ST_COLLECT`. Some geographic computations, projection transforms, and polygon set operations still fail with explicit errors, so this is not complete GIS compatibility.
- `VECTOR(n, datatype)` supports FLOAT32, FLOAT16, BFLOAT16, INT8, and BINARY. Numeric vectors support L2/COSINE/DOT; BINARY supports HAMMING/JACCARD with `quantization=none`. Numeric index quantization is explicit and defaults to `none`; PQ indexes without enough training rows execute exact scans. See [vector datatypes and indexes](vector-indexes.md).
- Vector supports exact brute-force KNN plus HNSW/IVF ANN. The planner recognizes `ORDER BY distance LIMIT k` and supports prepared parameters, partitioning, transaction deltas, visible native heap re-ranking, and the `NO_VECTOR_INDEX` differential oracle. `EXPLAIN` reports quantization and the effective re-ranking pool; `information_schema.mydb_vector_indexes` exposes configuration, training state, and runtime counters.
- [Hybrid search](hybrid-search.md) adds an SQL RRF/weighted-RRF technical preview: 2–8 independently ordered inputs, typed composite identities, relational output, prepared parameters, per-branch score explanations, and Python retrieval/reranking/evidence examples.

## 4. Query Optimization and Execution

- The CBO uses table/column statistics, histograms, selectivities, function costs, a cache model, a spill model, and subquery costs to choose access paths, join orders, and physical operators.
- Scan paths include sequential scans, primary/secondary index lookups and ranges, covering/index-only scans, index merge, FULLTEXT, SPATIAL, Zone Map, and vector indexes.
- Zone Map v6 uses 16 KiB buffer-pool pages (page version 3), typed endpoints, selective membership summaries, physical full-page images and WAL page deltas, and background partition-level rebuilds. When data is missing, corrupted, type-incompatible, untrusted, or the memory budget is insufficient, it deterministically falls back to a full heap scan; the summaries are never allowed to produce false negatives.
- Joins include Hash Join, Grace Hash Join, Nested Loop, Index Nested Loop, semi/anti, and outer joins, with adaptive selection where appropriate.
- Sort, Top-N, Hash Aggregate, Hash Join, DISTINCT, window, and CTE operators support budget-bounded in-memory execution with disk spill.
- `MYDB_VECTORIZED=off|on|auto` selects scalar or typed/vector kernels inside the same batch operator graph; `off` does not force the legacy row executor. The row-wise evaluator remains the semantic oracle, and expressions unsupported by compiled kernels use the scalar lane before consuming input.
- Parallel scan, join, aggregation, window, and sort operators use a unified statement context to propagate parameters, session semantics, cancellation, timeouts, memory budgets, and EXPLAIN ANALYZE metrics.
- Auto-parameterization, prepared plans, the plan cache, the statement cache, and CTE/view/routine caches all carry schema/session invalidation conditions. Compiled single-table and unique-lookup operators reuse column decode plans, projection/filter programs, and the prepared-text cache; range aggregation can accumulate directly inside the batched index-read visitor, falling back to the general physical plan when a proven shape does not hold. Physical plans never take live RSS, cache hit rates, or concurrent load as input, guaranteeing stable plans for identical semantic inputs.
- MySQL table-level index hints and `/*+ ... */` hints for scans, joins, parallelism, timeouts, SELECT INTO, and HNSW/IVF are supported; see [`hints.md`](hints.md) for details.

## 5. Transactions, Storage, and Memory

- The default isolation level is READ COMMITTED; REPEATABLE READ implements snapshot isolation, and READ UNCOMMITTED and SERIALIZABLE are also available.
- Row versions are managed by the heap MVCC header. Index-only scans look back at the heap header, and a B-tree leaf's `page_max_trx_id` can short-circuit visibility checks when safe; the project no longer has visibility-map files.
- WAL, group commit, checkpoints, dirty page flushing, redo crash recovery, segmented undo, SAVEPOINT, row locks, and deadlock detection form the transaction durability pipeline.
- SERIALIZABLE uses table-level shared locks to provide range no-insert semantics; RR does not implement InnoDB gap/next-key locks.
- XA supports START/END/PREPARE/COMMIT/ROLLBACK/RECOVER with crash recovery; JOIN/RESUME/SUSPEND are not supported.
- Persistent user base tables support `FOR SYSTEM_TIME AS OF <expr>`. Each table reference can bind an independent point in time; JOIN, subquery, CTE, UNION, view definitions, and prepared parameters all work. The commit timeline is recovered from WAL v22 and `commit-ledger.v1`, and old B-tree keys can be accelerated by `history-index.v1`; historical index-only paths and current-state Zone Map/HNSW/IVF/FULLTEXT/SPATIAL paths are never misused for historical results. The history window is controlled by `mydb_undo_retention_ms`, default `0`; targets crossing a structural epoch are rejected explicitly.
- Heap, B-tree, LOB overflow, vector page, and temporary files each have their own file format; development builds keep no backward-compatibility paths for old formats.
- `MYDB_MEMORY_TARGET` is an advisory working-set target for caches and spill watermarks. Ordinary governed SQL admission has a separate hard boundary of 80% of effective OS/cgroup memory, with the next 10% reserved for rollback/compensation. Long-lived or bulk allocations such as buffer raw pages, storage scratch, undo, SQL caches, and the executor go through unified reservations billed by allocation capacity; optional caches can be skipped or evicted, spillable operators spill at their watermarks, and unreclaimable admission failures return 1037/HY001.

Memory diagnostics entry points are `SHOW MYDB MEMORY`, `SHOW MYDB MEMORY SESSIONS`, and `SHOW ENGINE MYDB STATUS`.

## 6. Protocol, Security, and Administration

- `caching_sha2_password`, `mysql_native_password`, and `sha256_password` are supported. TLS is advertised by default; temporary self-signed certificates, explicit certificates, or mTLS account constraints can be used.
- Prepared statements support parameter metadata, binary values, read-only server-side cursors, and optional result-set metadata; SQL-level PREPARE/EXECUTE is also available.
- `init_connect` runs after successful authentication on every new connection: multiple statements execute in order, session side effects are preserved, and result sets are discarded; `CONNECTION_ADMIN`, `SUPER`, and password-expired accounts skip it. See [startup configuration](configuration.md).
- Account management covers CREATE/ALTER/DROP/RENAME USER, password rotation and history, failed-login policies, `max_user_connections`, and the validate_password component.
- The privilege model covers static/dynamic privileges, roles and role closures, default/mandatory roles, column-level grants, DEFINER/INVOKER security contexts, and INFORMATION_SCHEMA row-level filtering.
- `SET GLOBAL|SESSION|PERSIST|PERSIST_ONLY`, `RESET PERSIST`, and `mysqld-auto.cnf` are wired up; session bits relevant to semantics or plans are folded into the corresponding cache keys.
- information_schema, performance_schema, mysql, and sys provide the table surfaces that clients and ORMs commonly use. Tables with a producer return real data; PS/system tables registered only for compatibility but without a producer return empty sets rather than fabricated metrics.
- General/slow/error logs, FILE/TABLE output, SHOW/KILL, SHUTDOWN, RESTART, COM_SHUTDOWN, and a graceful shutdown flow shared with SIGINT/SIGTERM are supported.
- The MySQL X Protocol, the Document Store API, and the X DevAPI path of MySQL Shell are not supported.

## 7. Replication

### 7.1 mydb as a MySQL binlog source

The default build can emit MySQL row-based binlog, supporting GTID, checksums, rotate/retention, COM_BINLOG_DUMP, transaction boundaries, and source-side semi-sync ACK. It currently fixes `binlog_format=ROW` and `binlog_row_image=FULL`; STATEMENT/MIXED and MINIMAL/NOBLOB row images, as well as PARTIAL_JSON, are not emitted.

### 7.2 MySQL as source, mydb as replica

Every build includes the MySQL replica receiver. The IO thread pulls binlog into the relay log and the SQL thread applies it in order; they can be started and stopped independently and support file/position and GTID sources, filtering, `SOURCE_DELAY`, UNTIL, TLS, and status queries. The source must use ROW/FULL with `binlog_row_value_options=''`. There is currently a single SQL applier; MySQL MTS parallel apply is not implemented.

### 7.3 Native mydb physical replication

mydb provides native WAL physical replication independent of MySQL binlog, managed with `MYDB` keywords. It includes protocol v2/mTLS, async/semi-sync, four-watermark ACKs, slots, timeline/fork detection, retention, staged base physical backups, manually controlled promote/demote, and status queries.

### 7.4 Online Backup and PITR

mydb provides online physical backup independent of the replication topology: full physical baselines with incremental recovery through archived WAL, complete index files, optional AES-256-GCM, asynchronous SQL jobs, continuous durable WAL archiving, plus `mydb-backup` verification, single-file import/export, fast restore, and point-in-time recovery by LSN/HLC. See [`backup-and-recovery.md`](backup-and-recovery.md) for operations and limitations.

## 8. Build, Runtime, and Maturity

The default features are `tls`, `fast-alloc`, and `zstd-compression`: `fast-alloc` uses the mimalloc global memory allocator, and `zstd-compression` enables zstd protocol compression. The MySQL replica receiver and its client dependencies are included in every build; `tools` adds tool binaries and must be enabled explicitly. To disable `fast-alloc` while keeping TLS and zstd protocol compression, use `--no-default-features --features tls,zstd-compression`.

```bash
cargo build
cargo build --features tools
cargo build --release
```

Current product boundaries:

- The single-node SQL/storage kernel and both replication paths are evolving rapidly, with no production-grade compatibility or operational commitments yet.
- The compatibility goal with MySQL is the classic protocol, common SQL, and tooling — not replicating the InnoDB physical format, lock implementation, plugin ABI, or the entire operational ecosystem.
- A syntax being accepted does not necessarily mean identical physical semantics; metadata compatibility items must produce warnings.
- Unsupported syntax, protocols, replication formats, or function shapes must return clear errors rather than succeed silently.
- Data format version changes require a new data directory or a rebuild; see the README and [`mysql80-compatibility.md`](mysql80-compatibility.md) for specific versions.

The current incompatible format versions are listed in [`storage-format-versions.md`](storage-format-versions.md). Version mismatches are never migrated silently.
