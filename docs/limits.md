# Server Limits and Specifications

mydb targets MySQL 8.0 semantics for common SQL limits. This page lists fixed,
configurable, and storage-derived limits, together with known differences.
A missing explicit cap does not imply unlimited capacity: memory, disk space,
packet size, and other resource budgets still apply.

Limits fall into three classes:

- **Fixed** — not configurable (e.g. page size, column cap).
- **Configurable** — startup options / environment variables / sysvars. SQL-variable ranges below are the *registered* bounds unless noted; startup parsing, runtime validation, and resource admission can impose additional constraints.
- **Derived** — not a constant but implied by an integer width (e.g. 32-bit page IDs → 64 TiB heap cap).

## At a Glance

| Dimension | Limit | Class |
| --- | --- | --- |
| Databases per instance | No explicit cap | — |
| Tables per database | No explicit count cap; table IDs use 64 bits | — |
| Max heap file size | **64 TiB** (2^32 pages × 16 KiB), per physical table/partition leaf | Derived |
| Max rows per table | No explicit row-count cap; bounded by physical heap capacity and row layout | Derived |
| Columns per table | **1017** (`ER_TOO_MANY_FIELDS` 1117) | Fixed |
| Secondary indexes per table | **64**, plus the primary key (`ER_TOO_MANY_KEYS` 1069) | Fixed |
| Columns per index (also composite PK/UNIQUE) | **16** (`ER_TOO_MANY_KEY_PARTS` 1070) | Fixed |
| B-tree index key width | **3072 bytes** of MySQL DDL width accounting (`ER_TOO_LONG_KEY` 1071); see exceptions below | Fixed |
| Partitions per table | **8192 physical leaves**; subpartitions replace their logical parents in this count (`ER_TOO_MANY_PARTITIONS_ERROR` 1499) | Fixed |
| Identifier length | **64 characters** for validated names; some constraint/partition names use bytes and some names are not checked (see below) | Fixed |
| Inline encoded row size | **16,347 bytes**, including row metadata (one heap page minus header/slot) | Fixed |
| Max single LOB value | **4 GiB − 1** (LONGTEXT/LONGBLOB parity) | Fixed |
| Max memory | No fixed cap; hard admission at **80%** of `min(host, cgroup)` memory | Configurable |
| Buffer pool | Automatic sizing floor **16 MiB** (1024 pages); direct overrides can be smaller; bounded by memory admission | Configurable |
| Max CPU / cores used | No artificial cap; all sizing derives from available CPU cores | Derived |
| Max connections | **1,000,000** registered maximum; default 1000 | Configurable |
| Transaction admission | Requested default `cores × 4` (min 8), reduced to available blocking-worker capacity; explicit setting capped at 4096 | Configurable |
| Parallel query degree | Session sysvar range **1–256**; startup and hint requests have separate bounds | Configurable |
| Max packet / statement size | **1 GiB** (`max_allowed_packet`), default 64 MiB | Configurable |
| Expression nesting depth | **1,024** (left-deep operator chains count one level per operator) | Fixed |
| SELECT nesting depth | **63** | Fixed |
| Set-operation branches | **128 in debug / 1,024 in optimized builds** with the default 8 MiB worker stack; stack-derived, clamped to 128–1,024 | Derived |
| DECIMAL precision / scale | **65 / 30** | Fixed |
| VECTOR value dimensions | FLOAT32: **16,383**; FLOAT16/BFLOAT16: **32,766**; INT8: **65,532**; BINARY: **524,256 bits**; separate heap/index layout limits apply | Fixed |
| JSON document depth | **100** | Fixed |
| View nesting depth | **32** | Fixed |

## Schema Objects and Identifiers

### Databases and tables

- No explicit count cap on databases, tables per database, or views per
  database. Object names must be unique within their scope.
- Table IDs are monotonically increasing 64-bit values, so ID space is not a
  practical constraint.

### Identifier length (64)

The 64-character identifier limit is enforced for:

| Object | Note |
| --- | --- |
| Database name (CREATE DATABASE) | — |
| Table name (CREATE TABLE / LIKE / TEMP LIKE) | Also checked by ALTER / RENAME TABLE |
| Column name | Checked by CREATE and ALTER, including MODIFY / CHANGE / RENAME COLUMN |
| Index name | Checked in CREATE TABLE and ALTER TABLE ADD / RENAME INDEX; standalone CREATE INDEX has a gap below |
| View name | Checked by CREATE / ALTER VIEW |
| Sequence name | Checked by CREATE SEQUENCE / ALTER SEQUENCE RENAME |
| CHECK constraint name | byte-based, see caveat |
| Foreign key name | byte-based |
| Partition / subpartition name | byte-based, non-empty, unique per table |
| Routine (procedure/function) name and parameter names | — |

Account identifiers: user name ≤ **32** chars, host ≤ **255** chars
(error 1470, `ER_WRONG_STRING_LENGTH`, on overflow).

No explicit identifier-length validation: trigger names, event names, table
aliases / correlation names, and index names introduced by standalone
`CREATE INDEX`. These are enforcement gaps; statement and resource budgets
still apply.

**Caveat:** most names are counted in characters while CHECK/FK/partition
names are counted in bytes; multi-byte identifiers can pass one check
and fail the other at the same visual length.

### Comment lengths

Table 2048 / column 1024 / index 1024 / partition 1024 characters. Routine comments ≤ 64.

## Table Structure

| Limit | Value |
| --- | --- |
| Columns per table | 1017; checked by CREATE TABLE, CREATE TABLE LIKE, TEMPORARY LIKE, and ALTER TABLE; dropped column slots do not count |
| Secondary indexes per table | 64 |
| Key parts per index | 16; also applies to composite PRIMARY KEY and UNIQUE constraints |
| Total B-tree index key width | 3072 bytes; uses MySQL storage-width accounting per key part, with exceptions below |
| Physical partition leaves | 8192; logical parents with subpartitions are not counted separately |
| ENUM members (unique) | 65,535 |
| SET members (unique) | 64 |
| Single ENUM/SET member length | 255 chars |
| Triggers per table | No cap; names must be unique |
| Foreign keys per table | No cap; names must be unique |
| CHECK constraints per table | No cap; names must be unique |
| Generated columns per table | No separate cap; included in the 1017-column limit |

Index-related caveats:

- The three index caps are validated when the DDL creates or re-validates the
  table; catalogs loaded from disk are not re-checked.
- Prefix indexes have no separate prefix-length constant; the prefix counts
  against the 3072-byte budget. Prefix `0` and non-character prefix parts are
  rejected with `ER_WRONG_SUB_KEY`.
- Indexing a BLOB/TEXT without a key length fails with `ER_BLOB_KEY_WITHOUT_LENGTH`
  unless the environment variable
  `MYDB_LOB_FULL_COLUMN_INDEX` is enabled (`1`, `true`, or `on`). This rule
  applies to B-tree indexes; FULLTEXT indexes accept whole text columns.
- The 3072-byte DDL budget excludes functional key parts and full-column LOB
  keys enabled by that override. FULLTEXT, SPATIAL, and vector indexes use
  their own type-specific checks. Internal encoded B-tree entries must still
  fit the index page format.
- Functional-index expressions reject subqueries, window functions,
  placeholders, user/system variables, `DEFAULT`, `GROUPING`, aggregates, and
  23 nondeterministic functions.
- Partitioned tables cannot participate in foreign keys
  (`ER_FOREIGN_KEY_ON_PARTITIONED`), and temporary
  tables cannot be partitioned.

## Rows, Pages, and Storage Capacity

### Page size

**16 KiB, fixed**. There is no runtime page-size configuration or large-page
mode. Physical replication requires the same page size.

### Per-heap-file cap: 64 TiB

Row addresses contain a 32-bit page ID and a 16-bit slot ID. Heap extension hard-rejects page counts beyond
2^32 with `"heap page id exceeds supported range"`, so:

> **Maximum heap file size = 2^32 pages × 16 KiB = 64 TiB.**

An unpartitioned table has one heap; a partitioned table has a separate heap
for each physical partition/subpartition leaf. Thus 64 TiB is not an aggregate
limit on a partitioned table.

LOB overflow descriptors also use 32-bit page IDs. Each physical table/leaf has
a separate LOB file with its own approximately 64 TiB address space; heap and
LOB files do not share one 64 TiB budget. LOB metadata and allocation overhead
reduce usable payload capacity. **B-tree index files use 64-bit page IDs** and
are not bound by this page-ID cap.

### Row count

There is no explicit row-count limit. The slot address space is
2^32 pages × 2^16 slots ≈ 2.8 × 10^14 slots; the practical bound is the 64 TiB
heap-file size and encoded row/slot overhead, per physical leaf. The slot-ID
width is only an address-space bound: a 16 KiB page cannot hold 65,536 slots.
A page has a 16-bit slot count.

### Row size

- **Inline encoded row limit: 16,347 bytes** = 16,384 page − 32-byte page
  header − 5-byte slot directory entry. This includes the row's own metadata,
  NULL bitmap, and any external-value descriptors, not just user data.
  Oversized INSERT/UPDATE rows return
  `ER_TOO_BIG_ROWSIZE`; DDL also checks fixed-size row footprints.
- There is **no MySQL-style 65,535-byte column-sum check** at DDL time; wide
  rows fail at write time or get externalized (below) instead.
- Variable-length columns larger than `mydb_lob_inline_max` (default **4096** bytes) are stored out-of-line
  in the LOB overflow file. Smaller variable-length values may also be
  externalized when needed to fit the row in one page.
- An UPDATE that no longer fits its page migrates the row to a piece chain;
  forwarding chains are capped at 8 steps.
- Oversized in-row encodings and variable values > `4,294,967,295` are rejected with
  `ER_TOO_BIG_ROWSIZE`.

### LOB limits

- On-disk LOB descriptors use 32-bit page IDs and total lengths —
  **max single LOB value = 4 GiB − 1**, matching
  LONGTEXT/LONGBLOB.
- Type maxima at DDL: TINYTEXT/TINYBLOB 255 bytes, TEXT/BLOB 65,535 bytes,
  MEDIUMTEXT/MEDIUMBLOB 16,777,215 bytes, LONGTEXT/LONGBLOB 4,294,967,295 bytes.
- Overflow pages carry 16,344 payload bytes each; chains
  are immutable and may be lz4-compressed.
- JSON, GEOMETRY, and dimensionless VECTOR values are variable-length columns
  and are externalized like LOBs; fixed-dimension `VECTOR(n)` never
  externalizes.
- LOB extent sizing: `mydb_lob_extent_min_pages` default 4; extent scan window 8192 pages.

### Data type maxima

| Type | Maximum |
| --- | --- |
| VARCHAR / VARBINARY | 65,535 **encoded bytes** (utf8mb4: 16,383 chars); non-strict mode promotes oversized declarations to an appropriate TEXT / BLOB tier |
| CHAR / BINARY | 255 characters / bytes respectively |
| BIT(M) | 64 |
| DECIMAL(p,s) | p ≤ 65, s ≤ 30 |
| FLOAT/DOUBLE(M,D) | M ≤ 255 (bit precision ≤ 53), D ≤ 30 |
| Temporal fsp | 6 |
| Integer display width | 255 |
| Character length argument | 4,294,967,295 (u32) |
| AUTO_INCREMENT | u64 (overflow → `ER_AUTOINC_READ_FAILED` 1467) |
| SEQUENCE min/max | i64 (further narrowed by `AS smallint\|integer\|bigint`) |

Index-side DECIMAL keys pack at most 28 signed bytes per value.

### WAL, undo, and checkpoint capacity

| Item | Value |
| --- | --- |
| WAL segment size | 256 MiB default (`MYDB_WAL_SEGMENT_BYTES`) |
| WAL LSN | u64 — no practical exhaustion |
| Single WAL frame payload | u32 → 4 GiB − 1 |
| WAL segments / total volume | No count cap (u64 ids, `wal-{:016x}.log`) |
| WAL write buffer | 256 KiB default, 0..=1 GiB |
| Undo segment size | 64 MiB default |
| Single undo frame payload | 64 MiB |
| Undo write shards | 1..=15 (default 4) |
| Undo bytes per transaction | 0 = unlimited (`MYDB_UNDO_TXN_MAX_BYTES`) |
| Undo tablespace disk cap | 0 = unlimited (`MYDB_UNDO_TABLESPACE_MAX_BYTES`) |
| Undo resident payload cache | 64 MiB default (0 = fully resident); record-directory metadata remains resident and is charged separately |
| Checkpoint interval / WAL trigger | 60 s / 4 GiB defaults |
| Auto-checkpoint debounce | clamped to 5–15 s |

## Connections and Sessions

| Setting | Default | Range |
| --- | --- | --- |
| `max_connections` | 1000 | 1 – 1,000,000; excess connections return error 1040 |
| Admin connection reserve | 10 extra slots for `CONNECTION_ADMIN` | fixed |
| Effective fd-derived ordinary-connection cap | `min(config, max(1, nofile − reserve − 10))`; reserve defaults to 512 (`MYDB_OPEN_FILES_RESERVE`) | — |
| `max_user_connections` | 0 (unlimited) | 0 – 100,000 |
| `back_log` | 4096 | 1 – 65,535; OS listen backlog uses MYDB_LISTEN_BACKLOG, default 4096 |
| `wait_timeout` / `interactive_timeout` | 28,800 s | 1 – 31,536,000 |
| `net_read_timeout` / `net_write_timeout` | 30 / 60 s | SQL: 1 – 31,536,000; GLOBAL values enforced on the wire; startup `MYDB_NET_WRITE_TIMEOUT=0` disables the write timeout |
| `connect_timeout` | 10 s | SQL: 2 – 31,536,000; startup `MYDB_HANDSHAKE_TIMEOUT=0` disables the handshake timeout |
| `max_execution_time` | 0 (off) | 0 – 18,446,744,073,709,551,615 ms |
| `max_prepared_stmt_count` | 16,382 | 0 – 1,048,576 |
| Placeholders per prepared statement | 65,535 (u16 wire encoding) | Excess parameters return ER_PS_MANY_PARAM |
| Result columns per prepared statement | 65,535 (u16 prepare-response field) | Excess columns return ER_TOO_MANY_FIELDS; no equivalent explicit text-query column cap |
| Cursors per session / global | 64 / 4096 | 1 – platform address-size limit; controlled by MYDB_CURSOR_PER_SESSION_MAX and MYDB_CURSOR_GLOBAL_MAX |
| Session temporary tables | 1024 tables / 1 GiB storage / 64 MiB undo | 0 = unlimited |
| OS file-descriptor limit request (`MYDB_OPEN_FILES_LIMIT`) | 65,535 | Positive u64 startup request; constrained by the OS hard limit |
| `open_files_limit` sysvar | 65,535 | 0 – 4,294,967,295; advisory, changing it does not change the OS limit |

Account-level quotas (`GRANT ... WITH MAX_...`): `max_questions`,
`max_updates`, `max_connections_per_hour`, `MAX_USER_CONNECTIONS` are all u32
with 0 = unlimited.

`thread_cache_size`, `table_open_cache` (4000, 1–524,288), and
`table_definition_cache` (2000, 400–524,288) are registered but **advisory** in
mydb.

## Transactions, Locks, and Concurrency

- **Transaction admission**: `MYDB_TXN_ADMISSION` gates concurrently executing
  transactions. Default (unset) requests `cores × 4`, min 8; an explicit
  numeric value is capped at 4096. Both are reduced to
  `max(1, blocking-worker ceiling − 1)`; `0` disables admission control.
- **Transaction count** has no fixed slot limit. Transaction IDs are 64-bit;
  GTIDs combine a server UUID and a 64-bit transaction number.
- **Row/unique locks**: `MYDB_MAX_LOCKS` default **0 = unlimited**; when set,
  exceeding it raises `ER_LOCK_TABLE_FULL`. No
  per-transaction lock count cap. Row-lock sharding default 64 shards.
- **Metadata locks (MDL)**: no fixed count limit; memory budgets still apply.
- **Lock wait**: `innodb_lock_wait_timeout` 50 s (1–1,073,741,824);
  `lock_wait_timeout` 31,536,000 s (1–31,536,000). Storage-side wait
  default `MYDB_LOCK_WAIT_MS` 30,000 ms.
- **Savepoints**: unlimited, named per transaction.
- **XA**: concurrent XA transaction count is unlimited; XID `gtrid` 1–64 bytes,
  `bqual` 0–64 bytes.
- **FK cascade depth**: `mydb_fk_max_depth` default 64, range 1 – 4,294,967,295.

## CPU, Threads, and Parallelism

There is no CPU license or artificial core cap. All sizing derives from
available CPU cores (cgroup-aware where applicable):

| Item | Default / formula | Bound |
| --- | --- | --- |
| Connection worker threads (`MYDB_TOKIO_WORKERS`) | cores × 4, min 16 | Positive `usize`, subject to OS/thread resources; connections are assigned to a fixed set of workers on Unix |
| Blocking pool baseline | cores × 4, clamp(16, 128), further limited by stack memory | Explicit baseline up to 4096; ordinary growth ceiling up to **8192** (`MYDB_BLOCKING_POOL_MAX`), also limited by stack memory |
| Parallel query degree (`MYDB_PARALLEL_DEGREE`) | 4 | sysvar `mydb_parallel_degree` 1 – **256**; startup degree and positive `/*+ PARALLEL(n) */` requests use u32 without that 256 clamp; hint 0 selects the default; actual DOP depends on operator/resource gates |
| Parallel admission threshold | 100,000 rows (`MYDB_PARALLEL_MIN_PAGES` 64 pages) | — |
| CPU-bound operator DOP | min(degree, `MYDB_PARALLEL_CPU_BOUND_WORKERS` or P-cores on macOS) | — |
| Parallel index build DOP | 2 | ≥ 1 |
| LOAD DATA parallelism | 2 | ≥ 1 |
| Bulk INSERT parse parallelism | cores (divided across active loads) | 1 – cores |
| Hash join probe parallelism | min(DOP, 8) | 1 – 64 (`MYDB_HASH_JOIN_PARALLELISM`) |
| Hash join grace partitions / depth | 128 / 4 | 2 – 256 / 1 – 8 |
| Buffer pool IO threads | max(cores/2, 1) | 1 – 16 |
| Buffer pool shards | next_pow2(cores) | 4 – 64 |
| Event scheduler workers | min(cores, 4) | 1 – 32 |
| HNSW build threads | all cores | 1 – 32 |
| Backup job workers / queue | 2 / 64 per database | workers 1–8; queued tasks 1–256 |
| Backup copy/hash/sync bulk slots | ceil(cores/2), capped at 64 and `open_files/4` | 1–64 before open-file cap; process-wide admission |

Blocking-pool stack capacity normally uses at most 5% of effective memory.
With automatic pool sizing, the server may reserve additional managed memory
for stacks to support transaction admission; available headroom can still
reduce the admitted concurrency.

Background work also includes checkpointing, undo purge, memory-pressure
monitoring, auto-analyze, zone-map maintenance, archiving, and binlog retention.
The parallel-read admission gate allows up to `max(1, ceil(cores / degree))`
independent eligible analytical statements, using `MYDB_PARALLEL_DEGREE`
(fallback 2) for this gate. During parallel-read pressure, mixed-workload write
shaping uses 1 slot on hosts with at most 2 cores, otherwise
`clamp(cores − degree, 2, 16)`.

## Memory

mydb has no fixed maximum-memory setting; memory is governed in layers:

1. **Effective limit** = `min(physical memory, cgroup memory.max)`.
2. **Operational limit** = 80% of effective — hard admission ceiling for
   ordinary SQL work.
3. **Managed limit** = 90% — rollback/compensation may use up to this; the
   last 10% is reserved for stacks, runtime, and allocator.
4. Performance Schema category: a separate cap of 5% of effective memory,
   within the managed limits above, not an extra allocation allowance.
5. `MYDB_MEMORY_TARGET` (default = 50% of effective, fallback 1 GiB) is the
   *advisory* sizing anchor for caches and budgets; explicit values are always
   truncated to the effective limit.

Key configurable ranges:

| Setting | Default | Min / Max |
| --- | --- | --- |
| Buffer pool pages (`MYDB_BUFFER_POOL_PAGES`) | adaptive 20–60% of memory target | Automatic sizing floor **1024 pages (16 MiB)**; a positive direct page override bypasses this floor; startup/runtime memory validation still applies |
| Buffer pool pct (`MYDB_BUFFER_POOL_PCT`) | adaptive | 1 – 100 |
| Executor work-area target | 30% of memory target | floor 16 MiB |
| Per-query spill watermark | 42% of executor target | Derived default floor 8 MiB; explicit positive MiB settings can be smaller |
| Process-wide query spill watermark | = executor target | Setter rejects 0; this is not the process hard memory ceiling |
| Work-area oversubscription | 100% | 100 – 100,000% |
| Temp (spill) disk total | derived from fs free − 10% reserve | 0 – 18,446,744,073,709,551,615 |
| Spill per query | 0 = unlimited | 0 – 18,446,744,073,709,551,615 |
| CTE resident threshold | = query budget (floor 256 KiB) | 0 = spill immediately |
| Catalog cache (`MYDB_CATALOG_CACHE_MB`) | 5% of target (2% if target ≤ 1 GiB), capped 256 MiB | 0 = disabled |
| Plan cache entries | 4096 per cache (the caches use the same configured capacity) | > 0 |
| Routine program cache | 1024 | ≥ 1 |
| HNSW bulk build memory | 80% of memory target | > 0 |
| Allocator | mimalloc (`fast-alloc` feature) | idle reclaim at 110% of target |

The classic MySQL buffer sysvars — `sort_buffer_size` (256 KiB),
`join_buffer_size` (256 KiB), `read_buffer_size` (128 KiB),
`read_rnd_buffer_size` (256 KiB), `bulk_insert_buffer_size` (8 MiB),
`tmp_table_size` (16 MiB), `max_heap_table_size` (16 MiB),
`range_optimizer_max_mem_size` (8 MiB) — are registered with 0 – 18,446,744,073,709,551,615
ranges but are **advisory**;
actual enforcement uses the unified memory and work-area budgets described above.
Query/process spill watermarks can be crossed by non-spillable work; governed
allocations still have to pass the operational hard-admission limit.

## Network and Packets

| Setting | Default | Range |
| --- | --- | --- |
| `max_allowed_packet` | 64 MiB (67,108,864) | 1,024 – 1,073,741,824 (1 GiB) |
| `net_buffer_length` | 16,384 | 1,024 – 16,777,216 |
| Protocol port | 8738 | 0 – 65,535 |
| Multi-statements | require `CLIENT_MULTI_STATEMENTS`; no per-packet statement count cap | — |

Statement size has no separate constant: it is bounded by `max_allowed_packet`
(≤ 1 GiB). String builders such as `REPEAT`, `LPAD`, `RPAD`, `SPACE`, `CONCAT`,
and `CONCAT_WS` return NULL with warning 1301 when their predicted result
exceeds it.

## SQL Language Limits

Parsing, semantic conversion, catalog encoding, and execution have separate
guards. Passing one guard does not bypass the others; resource admission can
also reject a statement before a fixed complexity limit is reached.

| Limit | Value |
| --- | --- |
| Expression nesting depth | 1,024 guarded expression levels (`ER_STACK_OVERRUN_NEED_MORE` on overflow); the compiled evaluator accepts up to 64 AST levels, with deeper admissible expressions falling back to scalar evaluation |
| Upstream parser recursion budget | 2,300 recursive parser steps, shared by expression/query/parenthesis parsing; this is not a guarantee of 2,300 SQL nesting levels (`ER_STACK_OVERRUN_NEED_MORE` on overflow) |
| SELECT nesting depth | 63 active query blocks, including the outer SELECT; nested subqueries and derived tables count, sibling set-operation branches do not (`ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT` on overflow) |
| Stored-program statement parse nesting | 64 active statement-parser calls, including blocks, IF/CASE/loop bodies, and handler statements; not just BEGIN...END blocks (`ER_PARSE_ERROR` on overflow) |
| View semantic-query encode depth | Expression depth 1,024; SELECT depth 63; query-expression depth 1,087; encoding JSON depth 8,444 |
| View semantic-query encoding size / structure | 64 MiB encoded JSON, 1,000,000 JSON containers, and 4,000,000 JSON scalars (including object keys); these are catalog-codec limits, separate from SQL text length |
| View expansion depth | 32 |
| GROUP BY expressions | 64 distinct expression ASTs after parser deduplication; repeated identical expressions do not consume additional key slots |
| GROUPING SETS / ROLLUP / CUBE expansion | 4,096 generated grouping sets, including products of grouping elements; a single CUBE accepts at most 12 elements (2^12 = 4,096). Overflow returns `ER_PARSE_ERROR` |
| Set operations | A single chain accepts 128–1,024 branches, depending on worker stack size and build mode (128/1,024 in debug/optimized builds with the default 8 MiB stack). The token guard counts UNION / INTERSECT / EXCEPT across the entire statement, including nested queries and CTEs; it permits at most branch-limit − 1 such operators (`ER_STACK_OVERRUN_NEED_MORE` on overflow) |
| Ordinary operator-chain complexity | Token-level budget of 1,024 arithmetic/comparison/other guarded operators, including nested subtree complexity; expression-depth guards also apply (`ER_STACK_OVERRUN_NEED_MORE` on overflow) |
| AND / OR chains | No separate operand-count cap; homogeneous chains are balanced, so their depth does not grow linearly with operand count. Nested expressions still obey the depth guards |
| MySQL right-nested JOIN rewrite | 128 loop iterations and a 4 MiB cumulative rewritten-text scan budget; applies only to this syntax adaptation, not to all SQL text (`ER_PARSE_ERROR` on overflow) |
| Recursive CTE iterations | `cte_max_recursion_depth` default 1000, range 0 – 4,294,967,295; counts recursive iterations after the anchor. Zero permits no recursive iteration, and a nonempty anchor needing iteration raises `ER_CTE_MAX_RECURSION_DEPTH`; mutually recursive CTEs are unsupported |
| Stored procedure recursion | `max_sp_recursion_depth` default 0, range 0 – 255; counts recursive calls to the same procedure, excluding its outer call. Zero disables recursion. A shared routine-call depth guard also applies, configured by `MYDB_ROUTINE_MAX_DEPTH` (default 255, capped at 255). Stored functions cannot recurse |
| Trigger call-depth guard | `MYDB_TRIGGER_MAX_DEPTH` default 32; checked against the shared active routine/trigger depth, so enclosing stored-routine calls also consume depth. Zero rejects trigger execution |
| `group_concat_max_len` | Default 1,024 bytes, registered range 4 – 18,446,744,073,709,551,615; over-limit results are truncated at a character boundary with warning 1260 |
| `sql_select_limit` | Default 18,446,744,073,709,551,615 (u64::MAX) = no implicit limit; 0 yields no rows when an implicit limit applies; explicit LIMIT takes precedence |
| LIMIT / OFFSET numeric literals | Non-negative integers fitting usize (0 – 18,446,744,073,709,551,615 on 64-bit builds); invalid or overflowing literals return `ER_PARSE_ERROR` |
| Integer literals | No separate parser digit-count cap; values beyond the integer range become DECIMAL literals. Evaluation, arithmetic, and assignment still follow bounded DECIMAL normalization and overflow rules |
| Table options `PCTFREE`/`PCTUSED` | 0 – 50 |

There is no separate parser byte/token-count cap on ordinary SQL text. On the
wire, `max_allowed_packet` bounds the command; nesting, complexity, and memory
guards still apply, as do the additional encoding limits when saving a view.

There is no explicit JOIN source-count rejection. Inner-join dynamic
programming is limited to at most 12 relations and a planning work budget;
13–31 relations use the multi-start incumbent, and 32 or more use a greedy
fallback. These are optimizer routing thresholds, not SQL table-count caps.

No separate count/length cap is imposed on IN lists, WITH-clause CTE count,
ORDER BY expressions, DISTINCT keys, text-query SELECT-list columns, routine
parameters/cursor declarations/handler declarations/body statements, hint
count or text length, event count, savepoint count, XA transaction count, or
HEX/string literal length. The guards above and object-specific rules still
apply. In particular, prepared-statement parameter and result-column metadata
have the 65,535 limits described under [Connections and Sessions](#connections-and-sessions),
and live cursor objects have separate quotas.

Implementation references: [parser depth guards](../src/parser/budget.rs),
[token complexity guards](../src/parser/adapter/operators.rs),
[GROUP BY conversion](../src/parser/convert.rs), and
[view encoding bounds](../src/storage/view_query.rs).

## Vector, JSON, GIS, and Full-Text

### Vector

| Item | Value |
| --- | --- |
| VECTOR dimension range | FLOAT32: 1 – 16,383; FLOAT16/BFLOAT16: 1 – 32,766; INT8: 1 – 65,532; BINARY: 1 – 524,256 bits |
| Native VECTOR payload | at most 65,532 bytes; large heap values use overflow storage |
| HNSW WITH m | default 16; accepts 2 – 1,024, subject to paged-node capacity |
| HNSW WITH ef_construction | default 200; accepts m – 65,535 |
| HNSW WITH ef_search | optional index default, accepts 1 – 1,000,000; session default `mydb_hnsw_ef_search` is 64 |
| HNSW maximum layer index | 16 (layers 0 through 16, up to 17 layers) |
| IVF WITH nlist | default 0 = auto (√N); accepts 0 – 1,048,576, build count clamped to 1 – N |
| IVF WITH nprobe | optional index default, accepts 1 – 1,048,576; session default 8, execution clamps to 1 – nlist |
| IVF WITH kmeans_iters | default 15; accepts 1 – 1,000 |
| IVF WITH train_sample | default 256; accepts 1 – 65,536; coarse training samples at most `min(N, train_sample × nlist)` rows |
| Index quantization | none (default), sq8, sq4, pq, bq; numeric types only |
| WITH rerank_mult | default 4; accepts 1 – 1,024 |
| PQ subspaces (`pq_m`) | default `ceil(dimension/8)`; accepts 1 – dimension |
| PQ code bits (`pq_bits`) | 4 or 8; default 8 |
| PQ training (`pq_train_samples`, `pq_iterations`) | defaults 65,536 / 25; accepts `2^pq_bits` – 1,048,576 samples / 1 – 1,000 iterations |
| Paged vector record | complete HNSW node or IVF entry must fit one 16 KiB page; HNSW adjacency capacity also depends on m |
| KNN k | no separate fixed cap (`k = LIMIT + offset`); search and memory budgets apply |

Floating vector values must remain finite after conversion (NaN and ±Inf are
rejected); INT8 and BINARY have their own strict value rules. Heap overflow
allows large fixed-dimension values, while paged indexes have separate record
capacity checks, including on empty tables. Invalid WITH options are rejected
rather than silently clamped. See [vector datatypes and indexes](vector-indexes.md)
for the complete option table, datatype/metric combinations, and PQ training
fallback behavior.

### Hybrid rank fusion

| Item | Value |
| --- | --- |
| RRF branches per node | 2–8; explicit ORDER BY and LIMIT on every branch |
| RANK_CONSTANT | Exact integer, 0–4,294,967,295; default 60 |
| Weights | One finite nonnegative value per branch; at least one positive; finite maximum score |
| Candidate and output windows | No implicit 1,000-row cap; shared memory and statement limits apply |
| Branch OFFSET | Absent or zero; ordinary outer OFFSET supported |
| Fusion spill | Not supported; budget exhaustion fails the statement |

Fusion identity domains, unsupported inputs, and application window policies
are described in [hybrid search](hybrid-search.md). The example Python client
has its own bounded request limits, independent of the SQL engine.

### JSON

- Document nesting depth ≤ **100**.
- No dedicated document-size constant; practical bound = `max_allowed_packet`
  (≤ 1 GiB) on the wire and the 4 GiB − 1 LOB cap on disk. Binary-format
  internals: object size ≤ 4,294,967,295, key length ≤ 65,535, small-form element
  count ≤ 65,535.

### GIS

- SRID range 0 – 2^32−1.
- WKB/WKT/GeoJSON nesting depth ≤ 32; only 2-D geometries (Z/M/EWKB are rejected).
- No absolute WKB byte cap — bounded by packet and LOB limits.

### Full-text

- Token length: `min_token_size` 3, `max_token_size` 84, `ngram_size` 2
  (`MYDB_FT_{MIN,MAX}_TOKEN_SIZE`, `MYDB_FT_NGRAM_SIZE`); all ≥ 1, no upper clamp.
- Boolean-mode term count: unlimited.
- BM25 parameters: k1 ≥ 0, b ∈ [0,1].

## Replication

### Native WAL-based replication

| Item | Default | Range |
| --- | --- | --- |
| Replication slots | **no count limit** (keyed by replica UUID) | — |
| Concurrent replica connections | 16 (`MYDB_REPL_MAX_CONNECTIONS`) | 1 – 1,024 |
| Concurrent base-backup slots | 1 | 1 – 16 |
| Retained WAL | 16 GiB (`MYDB_REPL_MAX_RETAINED_BYTES`); overflow truncates and invalidates lagging slots | u64 |
| Slot idle timeout | 300,000 ms | — |
| Wire frame payload | 64 MiB absolute | — |
| Chunk bytes | 1 MiB | 4,096 – (64 MiB − 1 KiB), additionally capped by the apply-queue byte budget |
| Apply queue | 16 MiB | 64 KiB – 1 GiB |
| Semi-sync timeout / ack count | 10,000 ms / 1 | ≥ 1 ms / ≥ 1 |
| Heartbeat | 1,000 ms | ≥ 1 ms |
| Replication LSN | u64 | — |

### MySQL-protocol replication compatibility

- Replication channels: no explicit count limit. `CHANGE REPLICATION SOURCE`
  is rejected while replication threads are running.
- Relay/progress/apply batching: `repl_relay_sync_txns` 32 (1 – 1,000,000),
  `repl_relay_sync_interval_ms` 100 (1 – 60,000), `repl_progress_sync_txns` 64,
  `repl_apply_batch_txns` 16 (1 – 1,000,000), `repl_apply_batch_bytes` 4 MiB
  (1 – 1 TiB).
- Binlog: single-file position limit 4,294,967,295; row events default 64 MiB clamped 1,024 – 4,294,967,295−64; rotation governed by `MYDB_BINLOG_ROTATE_BYTES`
  (default: no size rotation) and retention by
  `MYDB_BINLOG_RETENTION_{HOURS,BYTES}` (default: none).
  `max_binlog_size` is a real GLOBAL runtime control for transaction-boundary
  rotation, range 4,096–1,073,741,824 bytes; `SET GLOBAL` updates the live
  threshold without splitting transactions.
- GTID: 128-bit server UUID + u64 GNO.

## LOAD DATA and Backup

- LOAD DATA: serial commit batch 50,000 rows, parallel
  batch 500,000 rows / 64 MiB target with 2,048-row minimum; batch byte budget =
  memory_target/16 clamped to 1–256 MiB. No row/field-size
  constants (fields bounded by the 1017-column cap) and **no error-abort
  threshold** — IGNORE mode skips and counts without a MySQL-style 65,535-error
  cutoff.
- Backup: bulk files use bounded 4 MiB chunks. Each database has a bounded
  worker pool (default 2) and queue (default 64); copy/hash/sync operations
  share process-wide bulk slots and memory, file-handle, bandwidth, spool,
  and freeze budgets. Different job kinds still enforce their capture/archive
  state conflicts; the worker count is not a promise that arbitrary jobs run
  concurrently. See [backup resource budgets](configuration.md#backup-resource-budgets)
  for configurable limits.

## Known Limit-Enforcement Gaps

Current differences and enforcement gaps (see also the caveats inline
above):

1. Trigger and event names, table aliases / correlation names, and index names
   introduced by standalone `CREATE INDEX` have no explicit identifier-length
   validation.
2. Identifier length is counted by characters for most objects but by bytes
   for CHECK/FK/partition names.
3. Index caps (64/16/3072) are not re-validated when a catalog is loaded from
   disk.
4. The 3072-byte B-tree DDL budget does not account for functional key parts
   or explicitly enabled full-column LOB keys.
5. `max_relay_log_size` is a display-only compatibility setting. Binlog
   rotation has real startup and runtime controls: `MYDB_BINLOG_ROTATE_BYTES`
   and `max_binlog_size`.
