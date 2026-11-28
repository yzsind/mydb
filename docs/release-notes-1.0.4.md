# MyDB 1.0.4 — Public Preview

## Downloads

| Platform | Archive |
| --- | --- |
| macOS Apple Silicon | [mydb-1.0.4-macos-arm64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.4/mydb-1.0.4-macos-arm64.tar.gz) |
| Linux x86_64 (glibc 2.17 baseline) | [mydb-1.0.4-linux-x86_64.tar.gz](https://github.com/yzsind/mydb/releases/download/v1.0.4/mydb-1.0.4-linux-x86_64.tar.gz) |
| Windows x64 | [mydb-1.0.4-windows-x86_64.zip](https://github.com/yzsind/mydb/releases/download/v1.0.4/mydb-1.0.4-windows-x86_64.zip) |

[Download SHA256SUMS](https://github.com/yzsind/mydb/releases/download/v1.0.4/SHA256SUMS)
to verify the archives.

Each archive includes `mydb`, `mydb-cli`, `mydb-backup`, the Apache-2.0
license, third-party notices, and corresponding MPL source archives.
Windows executables require compatible Visual C++ and Universal CRT runtimes.

## Overview

MyDB 1.0.4 adds **property graphs for GraphRAG and image embeddings**, extends
online index creation and schema maintenance, and improves recursive queries,
pagination, MySQL compatibility, and replication correctness. These notes cover
source changes since
[MyDB 1.0.3](https://github.com/yzsind/mydb/releases/tag/v1.0.3).

- **GraphRAG:** use GQL and SQL over one MySQL connection and transaction to
  combine graph traversal, vector retrieval, and document lookups.
- **Image embeddings:** generate native vectors from image URLs or binary
  inputs through DashScope multimodal and vLLM embedding adapters.
- **Online DDL:** build FULLTEXT, SPATIAL, HNSW, and IVF indexes with concurrent
  writes, and reduce writer exclusion during COPY and partition maintenance.
- **Query execution:** reduce recursive CTE, DISTINCT, pagination, sorting,
  and scalar-expression overhead, and bound index-join cache memory.
- **Correctness and operations:** fix prepared statements, JSON indexes,
  triggers, diagnostics, binlog atomicity, and native replica recovery.

The release groups the changes described here into **7 enhancements, 56 bug-fix
groups, and 20 performance optimizations**. Counts are by change theme rather
than commits or test cases; enhancements and performance improvements can overlap.

**Release maturity:** 1.0.4 remains a public preview for feature validation,
compatibility testing, and evaluation in controlled environments. The MySQL
compatibility baseline remains **8.0.46**. Upgrading from 1.0.3 requires logical
migration to a fresh data directory because the WAL format changes; see the
upgrade instructions below.

## Property graphs and GraphRAG

Create a property graph in an existing database and use GQL alongside SQL:

```sql
CREATE GRAPH knowledge ANY PROPERTY GRAPH;
SESSION SET GRAPH knowledge;
CREATE UNIQUE GRAPH INDEX entity_key ON knowledge NODE LABEL Entity (key STRING);

INSERT (source:Entity {key: 'p1'}),
       (target:Entity {key: 'p2'}),
       (source)-[:CITES]->(target);

MATCH p = TRAIL (start:Entity {key: 'p1'})-[:CITES]->{1,3}(related:Entity)
RETURN DISTINCT CAST(related.key AS STRING) AS passage_key,
                PATH_LENGTH(p) AS hops
ORDER BY hops, passage_key LIMIT 20;
```

- Graphs contain labeled nodes, directed and undirected edges, and scalar
  properties. Parallel edges and self-loops retain distinct identities.
- The supported GQL subset includes MATCH, OPTIONAL MATCH, graph updates,
  bounded WALK/TRAIL/ACYCLIC traversal, ANY SHORTEST, named paths, DISTINCT,
  ordering, limits, and grouped COUNT results. Unsupported forms return
  explicit syntax or feature errors.
- Node property indexes support STRING and INT64 keys, including uniqueness.
  Optional node and edge property projections pack selected fields for repeated
  traversal. Updates retire affected projection blocks transactionally.
- Graph statements support positional prepared parameters, SQL transactions,
  savepoints, and database-level privileges. Graph data uses the existing
  transactional heap, indexes, WAL, and undo. Graph writes require session
  MySQL binary logging to be off; graph DDL requires autocommit outside an
  active transaction.
- Traversals enforce a 16-hop limit per path pattern, a one-million-adjacency
  examination limit per statement, query memory limits, and deadlines.

The GraphRAG example retrieves vector seeds, expands them with parameterized
GQL, and fetches passages and citations within one REPEATABLE READ snapshot.
See [property graphs and GraphRAG](graph-rag.md) and the
[retrieval example](../examples/graph-rag/retrieve.py).

## Image embeddings

- `AI_EMBED` and `AI_EMBED_DETAIL` accept `type=image` for models that explicitly
  declare image capability. DashScope multimodal embeddings and vLLM Chat
  Embeddings join the existing text adapters.
- Inputs can be HTTP/HTTPS image URLs or binary image values, including BLOB
  columns and bound parameters. Recognized binary signatures include JPEG,
  PNG, WebP, BMP, and TIFF. Results retain fixed dimensions and the native
  FLOAT32 VECTOR type for storage and similarity search.
- URL arrays support ordered DETAIL batches, including duplicate inputs.
  Automatic row batching accounts for encoded request size and model defaults;
  requests remain subject to AI memory, timeout, retry, and cancellation limits.
- Binary images are limited to 5 MiB. Local paths and caller-supplied Base64 or
  Data URI strings are rejected; use binary values or `FROM_BASE64` for bytes.
  The model service fetches remote URLs, so its image-fetch policy and size
  checks apply. A URL without `type=image` remains ordinary text.

External-call transaction and DML restrictions remain in effect. Generate
vectors outside transactions, then store them through ordinary DML.
See [image embeddings](ai.md#image-embeddings),
[AI_EMBED](function/ai/ai_embed.md), and
[AI_EMBED_DETAIL](function/ai/ai_embed_detail.md).

## Online DDL and maintenance

- FULLTEXT, SPATIAL, HNSW, and IVF index creation allows concurrent DML by
  default and with `LOCK=NONE`, including supported atomic mixed-index batches.
  Vector builds cover partition-local, typed, quantized, and partial indexes.
  Private indexes remain unavailable to queries until publication succeeds.
- Scanning, index construction, vector training, initial image persistence,
  and most change replay run outside writer exclusion. Preparation and
  publication retain brief barriers. Capture limits, cancellation, or a tail
  that cannot converge abort the candidate while preserving committed writes.
- Supported column COPY plans extend concurrent writes to FULLTEXT and SPATIAL
  sources, primary-key replacements, non-strict conversion, Memory storage,
  and added AUTO_INCREMENT columns. Existing-row AUTO_INCREMENT assignments
  survive replay, and publication advances the allocator highwater.
- DROP COLUMN operations that rebuild dependent B-trees build and replay
  private indexes outside the publication barrier. Retired indexes, files,
  and FULLTEXT counter generations are reclaimed after publication through
  deferred or restartable bounded work.
- COPY logs complete private file baselines before catch-up and commits file
  bindings with the catalog. File preparation and bulk data synchronization
  move outside source writer exclusion, with retained redo and recovery checks
  protecting incomplete synchronization.
- Supported partition rebuild, optimize, repair, truncate, and row-moving
  operations allow writes during preparation. TRUNCATE removes all rows in
  selected leaves at publication, including concurrent commits; writes to
  retained leaves survive. Supported EXCHANGE operations prepare both tables
  concurrently and retry changed inputs before atomic publication.
- CHECK TABLE, supported CHECK PARTITION, and ANALYZE PARTITION reduce scan-time
  writer exclusion while validating snapshot consistency. Persistent changes
  can exhaust bounded retries and return an explicit error.

Explicit `LOCK=SHARED` and `LOCK=EXCLUSIVE` retain their blocking contracts.
Online support depends on the complete action list; unsupported combinations
fail explicitly. Reduced writer exclusion does not promise shorter total DDL
time or a hard bound on operating-system I/O latency.

See [CREATE INDEX](statement/ddl/create-index.md),
[ALTER TABLE](statement/ddl/alter-table.md),
[online column changes](online-column-type-change.md), and
[online vector index creation](vector-indexes.md#online-index-creation).

## Query execution and memory

- Recursive CTEs use bounded native batches, consume vectorized delta streams,
  reuse compiled scalar steps, and reduce DISTINCT key overhead. Recursive
  assignment preserves anchor types, cycle termination, depth limits, and
  memory-budget fallback behavior.
- Pagination and window execution reduce intermediate row materialization and
  sort-key overhead. Eligible primary-key ranges and ordered index offsets
  bound heap fetching while preserving residual filters and MVCC visibility.
- Scalar and vectorized expressions reduce repeated conversion and allocation
  work, including borrowed comparisons and native arithmetic. Prepared numeric
  domains and precision settings remain correctly scoped across executions,
  shared caches, and dependent DDL reprepare.
- Hash joins accept eligible `generate_series` table-function sources.
- Range index-join caches account for retained keys, boundaries, and container
  capacity. Each cache is limited to one eighth of the query allowance and at
  most 64 MiB; optional-cache pressure evicts or bypasses entries instead of
  retaining unbounded range results.
- Prepared EXPLAIN releases catalog read admission before statistics
  publication, avoiding a catalog-lock deadlock.

See [optimizer and execution](optimizer-and-operators.md),
[generate_series](generate-series.md), and
[resource configuration](configuration-resources.md).

## SQL compatibility and diagnostics

- Prepared functional and multi-valued JSON index lookups use the same
  conversion domains as stored keys. Composite multi-valued index prefixes
  preserve numeric/text comparison semantics instead of missing qualifying rows.
- JSON_TABLE defaults strictly parse JSON and preserve the distinction between
  JSON null, SQL NULL, Boolean text, and unquoted JSON strings for target types.
- BEFORE-trigger assignments to generated NEW columns are ignored while later
  trigger statements continue; final generated values follow the base columns.
  INSERT ... ON DUPLICATE KEY UPDATE IGNORE handles view CHECK OPTION failures
  per row with warnings.
- UPDATE, DELETE, and VALUES-based conflict updates validate column qualifiers
  before scanning, including empty targets. Trigger, routine, and event names
  containing colons no longer collide across schema/object boundaries.
- Additional fixes cover charset-introduced literal defaults, DECIMAL synonyms
  with UNSIGNED/ZEROFILL, conditional result types, named windows, correlated
  scopes, NULLIF, temporal precision and invalid-date arithmetic, and regular
  expression regions and vertical whitespace.
- INSERT IGNORE and LOAD DATA diagnostics retain source-row positions and
  warning order. Stored-routine cursors, local assignments, nested SIGNAL,
  and user-supplied error text preserve their diagnostic context. Failed SELECT
  binding does not add an empty result set.

See [MySQL compatibility](mysql80-compatibility.md),
[data types](data-types.md), and [error codes](error-codes.md).

## Binlog, replication, and recovery

- DDL executed through prepared statements, SQL EXECUTE, routines, and events
  uses the atomic binlog path and preserves source SQL spelling and session
  context. Mixed permanent/temporary DROP operations report binlog failures
  instead of returning success with only a warning.
- The binlog source emits MySQL-compatible ENUM/SET table-map metadata.
  Oversized events fail before commit with error 1118; each event must fit the
  1 GiB replication packet limit, including framing.
- The MySQL receiver accepts large events within that limit and treats packet
  overflow as a fatal diagnostic. ROW/FULL fixes cover padded CHAR/BINARY
  images, unsigned TIMESTAMP2 seconds, ENUM zero, geometry SRIDs, virtual
  generated keys, partition routing, and idempotent batch replay. Available
  ENUM/SET member metadata is checked before applying values.
- Incremental channel changes preserve the correct committed position.
  UNTIL handles numeric binlog file ordering, already-reached targets, and
  thread restarts. Reconnect status and lag no longer claim a disconnected or
  stale source is caught up; transport timeouts and duplicate server-ID errors
  are explicit. Quoted DDL identifiers and empty source SQL modes replay
  correctly.
- Native role changes clear stale local catalog locators. Persistent index
  replay uses the live buffer pool, and retried transaction batches distinguish
  complete physical frames from partially applied logical prefixes.
- Native seeding excludes initializing slots from semi-sync quorum and retries
  transient backup-capacity rejection. Authentication, identity, and format
  rejection stop immediately. Ready ACKs are processed under sustained writes.
- `START MYDB REPLICATION FORCE_RESYNC` checks authenticated source eligibility
  before scheduling restart. Failed preflight preserves SQL sessions and does
  not mark the replica for resync. Native promotion still requires external
  fencing.

See [MySQL replication](statement/replication/README.md),
[native replication](native-replication.md), and
[backup and recovery](backup-and-recovery.md).

## Moving from 1.0.3 to 1.0.4

**Use a fresh data directory.** The WAL format advances from **41 to 42** for
transactional private-to-final COPY file bindings. The catalog format remains
**75**, but the data-directory admission fingerprint changes. A 1.0.4 binary
cannot directly reuse a 1.0.3 data directory or physical backup; native physical
replication is not a cross-format migration path.

1. Preserve the 1.0.3 binary, external keys, startup configuration, and a verified
   backup that can be restored with the original compatible binary.
2. Export supported logical schema and typed data, then load them into an
   independent 1.0.4 instance with a new `MYDB_DATA_DIR`. Recreate instance
   objects such as users, grants, routines, events, and policies as needed.
3. Validate row counts, data, representative queries, transactions, indexes,
   privileges, and application behavior before switching writes.
4. Recreate native replicas from the migrated primary. Plan rollback before
   accepting new writes; the old copy will not contain subsequent changes.

The default isolation level remains READ COMMITTED. REPEATABLE READ uses
snapshot isolation without InnoDB gap or next-key locks. MySQL replication
requires ROW/FULL. Graph writes require session MySQL binary logging to be off.

See [changing versions](changing-versions.md), the
[storage format registry](storage-format-versions.md), and
[feature boundaries](feature-overview.md).

## Downloads, documentation, and feedback

- [GitHub Releases](https://github.com/yzsind/mydb/releases) for published
  packages, checksums, and package-specific validation details
- [Source changes since 1.0.3](https://github.com/yzsind/mydb/compare/v1.0.3...v1.0.4)
- [Documentation](README.md) and [issue reporting](reporting-issues.md)
- [Report an issue](https://github.com/yzsind/mydb/issues)
- [Apache License 2.0](../LICENSE)
