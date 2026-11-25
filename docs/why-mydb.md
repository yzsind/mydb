# Why MyDB

**AI Agents. HTAP mixed workloads. MySQL compatibility.**

MyDB is a general-purpose database built in Rust around three directions:
AI agents, hybrid transactional/analytical processing (HTAP), and MySQL
compatibility. Transactional reads and writes share the same engine and
business data with analytical queries, native retrieval, and SQL access to
external models.

| Direction | What it gives you |
| --- | --- |
| [AI Agents](#ai-agents) | Native retrieval and model calls, REST/MCP access, adaptable schemas, and a recycle bin for eligible destructive DDL |
| [HTAP mixed workloads](#htap-mixed-workloads) | Transactional storage, parallel and vectorized queries, automatic Zone Maps, unified memory management, backup, and native replication |
| [MySQL compatibility](#mysql-compatibility) | Familiar clients, drivers, common SQL, and ROW/FULL binlog integration, with additional SQL capabilities |

For teams building business applications and AI agents, this means fewer data
copies to synchronize, less application code to connect retrieval with business
data, and more database work expressed through SQL.

MyDB 1.0.3 is a public-preview release intended for feature validation and
controlled-environment evaluation. The [Feature Overview](feature-overview.md)
and [MySQL Compatibility](mysql80-compatibility.md) describe supported behavior,
release maturity, and detailed boundaries.

## AI Agents

### Business data and AI retrieval together

Keep document text, embeddings, business attributes, and relational references
together. SQL can combine retrieval with joins, filters, and application data
without requiring a separate synchronization pipeline for every retrieval
store.

| Capability | Application benefit |
| --- | --- |
| Native `VECTOR` with FLOAT32, FLOAT16, BFLOAT16, INT8, and BINARY | Choose a representation for the precision and storage needs of the application |
| Exact nearest-neighbor search plus HNSW and IVF | Select between exact results and approximate search, with tunable recall and cost |
| SQ8, SQ4, PQ, and BQ index quantization for numeric vectors | Explore compressed index representations for larger vector collections |
| Full-text search with BM25, boolean mode, and ngram | Retrieve by keywords as well as semantic similarity |
| SQL RRF and weighted RRF | Combine independently ranked retrieval branches and inspect their score contributions |
| `AI_GENERATE` and `AI_EMBED` | Call external generation and embedding services from SQL; embeddings return a native vector |

The RRF extension is a technical preview. It supports two to eight ordered
branches, weights, composite identities, prepared parameters, and relational
consumers. An application can fuse keyword and vector candidates, then join
the resulting identities to document metadata in the same SQL statement.
Apply the required authorization filters to every retrieval branch before its
candidate limit.

Vector indexes re-rank candidates using visible heap values. Quantization and
search parameters provide explicit tuning choices; approximate search still
requires recall evaluation on the application's own data. Model inference runs
in external services, so applications can select a provider independently of
database storage.

See the [Vector Search Guide](vector-search.md),
[Vector Index Reference](vector-indexes.md),
[Full-Text Search](fulltext-search.md), [Hybrid Search](hybrid-search.md), and
[LLM and Embedding Calls](ai.md).

### Direct access with shared permissions

MyDB provides three entry points into the same SQL engine: the MySQL classic
protocol, a REST SQL API, and an MCP server. Teams can use SQL drivers for
existing applications, HTTP for services, and MCP tools for agents.

REST and MCP provide parameter binding, batches, persistent SQL sessions,
transactions, timeouts, and cancellation. MCP tools also expose database and
table discovery, table descriptions, and query plans. These native interfaces
reduce the work needed to build and maintain a separate SQL gateway or agent
adapter.

API keys inherit their owning account's current privileges, default roles,
Host restrictions, TLS requirements, and quotas. Revocation and expiry
invalidate access and close affected sessions. The same permission checks
apply across interfaces, including row-security policies when configured;
the supported scope and current boundaries of row-level security are listed
in [Row-Level Security](row-level-security.md).

REST and MCP are optional and enabled explicitly; HTTPS is the default for
their shared listener. See [REST SQL API](rest-api.md), [MCP Server](mcp.md),
and [Row-Level Security](row-level-security.md).

### Schemas that evolve with the task

MyDB stores rows in heap tables with separate indexes. Adding a primary key
does not require reorganizing the heap into a clustered table. This storage
choice supports a broader goal: keeping business access available during
supported schema changes.

- **Online primary-key and B-tree index builds** allow concurrent DML during
  construction. The initiating SQL statement returns only after the index
  becomes public or the operation fails.
- **Supported column type changes** use shadow storage and incremental
  catch-up, allowing reads and writes during the build before publication.
- **Supported CHECK and foreign-key changes** allow concurrent reads and
  writes, with validation and atomic publication of the candidate definition.

These capabilities reduce the need to arrange long write outages for supported
changes. Preparation and publication still require barriers; some physical
rewrites and partition operations wait for readers. Supported combinations of
types, indexes, algorithms, and lock modes are documented in
[ALTER TABLE](statement/ddl/alter-table.md) and
[Online Column Type Change](online-column-type-change.md).

### Inspect and recover destructive DDL

Agents that manage schemas also need ways to inspect and recover mistakes.
The default-on [recycle bin](recycle-bin.md) retains tables removed by eligible
DROP TABLE, TRUNCATE TABLE, and DROP DATABASE operations. Retained tables can
be queried and restored without first recovering the whole instance.

The default retention target is seven days, subject to expiration and
disk-pressure reclamation. Triggers, foreign keys, row policies, and grants
are not restored with the table. The recycle bin complements the backups and
PITR described under [HTAP mixed workloads](#htap-mixed-workloads).

### Published vector-search evidence

The November 24, 2026 Cohere HNSW benchmark compares one million
768-dimensional FLOAT32 vectors with cosine Top-100 retrieval and MyDB
query-candidate memoization disabled. At 16 clients, MyDB delivered
**5,121 vs 2,142 queries/s (2.39x)** under a 32 GiB database memory
limit, and **458 vs 139 queries/s (3.29x)** under a 2 GiB limit,
compared with PostgreSQL 18.6 and pgvector 0.8.7.

Recall@100 was 94.741% vs 94.395% at 32 GiB and
95.331% vs 94.341% at 2 GiB. Both engines used the same
16-vCPU host, `m=16`, `ef_construction=64`, effective `ef_search=200`, and
durable commits. MyDB uses auxiliary 8-bit navigation codes with exact candidate
rescoring. See the [Cohere 1M benchmark](vector-cohere-1m-hnsw-benchmark-2026-11-24.md) for build times,
latency, measurement details, and per-round results.

## HTAP mixed workloads

### Transactions and queries on one dataset

MyDB brings transactional operations and analytical queries together over one
dataset. Heap storage, B-trees, MVCC, row locks, WAL, and undo support business
reads and writes. A shared optimizer and execution framework select access
paths for point lookups, updates, joins, and aggregations within the mixed
workload.

Automatic data skipping, parallel execution, and vectorized execution reduce
the work required by eligible analytical queries. Unified resource budgets
coordinate memory use across the engine as transactional and analytical
requests share the instance.

MyDB defaults to READ COMMITTED. Its REPEATABLE READ uses snapshot
isolation without InnoDB gap/next-key locks, avoiding the insertion blocking
caused by those range locks. Applications must account for the corresponding
semantic differences: range inserts and write skew are possible. SERIALIZABLE
uses table-level shared locks. See [MVCC and Locking](mvcc-and-locking.md).

### Read less and compute in parallel

**Automatic Zone Maps reduce the need to index every filtering pattern.**
MyDB maintains summaries that can rule out data blocks before rows are fully
examined. Eligible serial, parallel, and vectorized scans use these summaries
and retain final predicate checks. Applications can benefit without creating
an explicit index for each predicate. The benefit depends on data layout,
distribution, and selectivity; unavailable or untrusted summaries fall back
to a correct heap scan.

**Parallel operators put multicore hardware to work.** Scans, joins,
aggregations, windows, and sorts have parallel execution paths. Hash Join and
Grace Hash Join support large relational queries, while partition-wise joins
and aggregation can exploit compatible partition layouts. Parallel import
provides another path for using available CPU capacity during data loading.

**Vectorized execution reduces repeated interpretation.** Batch operators
use typed kernels where supported and scalar evaluation where needed. A
cost-based optimizer uses statistics and histograms to choose access paths,
join orders, and physical operators. Plan caches reuse work with schema and
session invalidation.

**Memory management supports larger working sets.** Unified allocation
accounting, query budgets, cache reclamation, and spill-capable operators
help queries handle data beyond their in-memory work areas. Operators such as
sort, Hash Join, aggregation, and windows can spill to disk. Memory and engine
diagnostics expose resource use and execution behavior; the configured memory
target is an advisory working-set target, not a hard process RSS limit.

Actual execution depends on the query, supported operators, statistics, and
available budgets. [EXPLAIN and execution diagnostics](optimizer-and-operators.md)
make those choices inspectable.

### Historical inspection, backup, and native replication

MyDB provides several ways to answer different recovery questions, from
inspecting historical row values to restoring an independent instance.

| Need | Native capability | Practical advantage |
| --- | --- | --- |
| Investigate earlier row values | `FOR SYSTEM_TIME AS OF` | Join and query historical data within the retained history window |
| Inspect a physical backup | Direct read-only backup queries | Open a sealed base without copying the whole database or replaying WAL |
| Recover beyond a full backup | Continuous or manual WAL archiving and PITR | Restore to a supported time, LSN, or latest confirmed position |
| Validate recovery inputs | Backup verification, recoverability checks, and recovery rehearsals | Inspect the recovery chain and test a restore before relying on it |
| Maintain a native standby | WAL physical replication | Replicate native persistent state independently of MySQL logical binlog |

Time travel requires retained history and cannot cross unsupported structural
changes. Direct backup queries inspect a sealed base at its fixed position,
while PITR creates a recovered instance at a supported target.

Online physical backup captures an instance-wide baseline, including its
indexes. Subsequent incremental recovery uses archived WAL; archiving must be
enabled separately. Native replication supports asynchronous and
semi-synchronous operation with explicit progress watermarks and controlled
role changes. Promotion requires operational fencing of the former primary;
MyDB does not provide consensus leader election.

See [Time Travel Queries](mysql80-compatibility.md#time-travel-queries),
[Backup and Recovery](backup-and-recovery.md), and
[Native Physical Replication](native-replication.md).

### Published workload measurements

Published experiments show measured advantages on individual workloads;
they do not measure a concurrent HTAP workload. The following MySQL comparisons
come from completed development runs on the same 16-vCPU host, with 10 GiB
per-database cgroup memory limits and synchronous durable commits.

| Workload | MyDB | MySQL 8.0.46 | Measured advantage |
| --- | ---: | ---: | ---: |
| Point queries, 100M rows, 20 clients | 316,968 queries/s | 174,566 queries/s | **1.82x throughput** |
| Read-only transactions, 100M rows, 20 clients | 14,323 transactions/s | 7,294 transactions/s | **1.96x throughput** |
| TPC-H-derived analytics, SF=10, sum of 22 query medians | 57.727 s | 882.784 s | **15.29x speedup** |
| TPC-DS-derived analytics, SF=10, 55 matching query files | 113.968 s | 2,576.552 s | **22.61x speedup on that subset** |
| TPC-H-derived bulk import, SF=10, LOAD DATA LOCAL INFILE | 25.708 s | 357.495 s | **13.91x speedup** |

These October 22–24, 2026 experiments cover different development builds.
TPC-derived results are unaudited, the TPC-DS figure is a matching-result subset
rather than a full-suite speedup, and the import figure excludes secondary-index
creation and ANALYZE. They are workload-specific historical observations, not
current-release guarantees. The [Performance Comparison](performance-comparison.md)
provides measurement procedures, correctness checks, conditions, and limitations.

## MySQL compatibility

### Familiar tools and integration

MyDB targets MySQL classic-protocol clients, common SQL, prepared statements,
stored programs, and ROW/FULL binlog integration. Applications can evaluate
new capabilities through familiar tools and drivers.

### More expressive SQL

The extensions below add useful building blocks beyond the MySQL Community
Server 8.0.46 comparison baseline:

| Extension | What it simplifies |
| --- | --- |
| FULL OUTER JOIN | Comparing two datasets while retaining unmatched rows on both sides |
| CUBE and GROUPING SETS | Computing multiple levels of summary in one query |
| INSERT / UPDATE / DELETE RETURNING with OLD / NEW | Returning modified rows without a separate follow-up SELECT; multi-target restrictions apply |
| `generate_series` for integers, exact decimals, and temporal values | Building time buckets, filling missing periods, and constructing sequences |
| TABLESAMPLE with row/page and repeatable sampling | Exploring and checking data through explicit sampling |
| Sequences with nextval / currval / lastval / setval | Allocating identifiers independently of a table's AUTO_INCREMENT |
| B-tree INCLUDE columns | Expressing covering columns separately from search keys |
| [Partial indexes](partial-indexes.md) | Indexing an eligible subset and enforcing uniqueness only within that membership |
| TIMESTAMP through year 9999 | Representing long-lived validity periods with timezone-aware timestamps |
| SQL ROWID | Locating heap rows directly in suitable workflows; physical addresses can be reused |
| CREATE OR REPLACE procedures and functions | Replacing parameters, return types, and bodies while retaining routine grants; invalid replacements preserve the existing definition |

MySQL compatibility does not imply identical InnoDB locking or physical files.
MySQL-format replication requires ROW/FULL, and native extensions may require
native replication rather than a MySQL receiver. The
[compatibility reference](mysql80-compatibility.md) covers these differences,
including extended TIMESTAMP replication limits and unsupported ecosystem
components.

### Evaluate with your application

Start with the [Quick Start](../README.md#quick-start), choose a representative
application workload, and check its SQL and isolation requirements against the
[MySQL migration guide](mysql-migration.md). The
[documentation index](README.md) links to configuration, deployment, search,
security, and operations references.
