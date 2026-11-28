# Current Source Changes

This page describes the source checkout as reviewed on November 28, 2026.
The package version is `1.0.4`; see the [1.0.4 release notes](release-notes-1.0.4.md)
for changes since `v1.0.3` and migration requirements. A product version string
alone does not identify compatible physical formats. For release behavior,
use the documentation at its Git tag. MyDB remains a public preview for
controlled evaluation.

## Physical compatibility

This checkout uses catalog format 75 and WAL format 42. MyDB 1.0.3 uses the
same catalog version but WAL format 41; 1.0.4 adds transactional private-to-final
COPY file bindings. A 1.0.3 directory or physical backup cannot be opened as a
format upgrade. Start an independent fresh directory and transfer reviewed
logical schema and typed data. Recreate native replicas from the migrated
primary; matching physical formats are required on both peers.

Keep the original binary, data and keys for recovery, and validate the transfer
before switching writers. See [changing versions](changing-versions.md) and the
[generated format registry](storage-format-versions.md). Release notes describe
the release's migration requirements, rather than later development formats.

## SQL and retrieval

| Capability | Current behavior and reference |
| --- | --- |
| Partial indexes | `WHERE` predicates apply to B-tree, unique, FULLTEXT, SPATIAL, HNSW and IVF indexes. Membership and uniqueness are maintained by DML; query use requires an implication proof. [Guide](partial-indexes.md) |
| FULLTEXT corpus statistics | Persisted MVCC counters cover the complete visible corpus, including rows outside a partial index. Current-view cold scoring reads bounded counter shards; old snapshots and historical reads retain visible-text fallbacks. [Scoring](fulltext-search.md#transactional-scoring-statistics) |
| Joined UPDATE/DELETE | INNER/LEFT/RIGHT chains preserve each ON scope and skip NULL-padded write targets. Uncorrelated derived queries, including grouping and set operations, are read-only sources. [UPDATE](statement/dml/update.md), [DELETE](statement/dml/delete.md) |
| Online view replacement | Readers bind complete immutable definitions. A later statement can see a replacement even inside the same RR transaction. [ALTER VIEW](statement/ddl/alter-view.md) |
| Automatic Zone Maps | Scans preserve read-your-writes and snapshot visibility; missing or unsafe summaries fall back conservatively. [Guide](zone-maps.md) |
| HNSW memory use | Packed navigation edges, compact native payloads and reclaimable mapped buffers reduce navigation/build memory. Exact heap reranking and ANN recall boundaries still apply. [Index guide](vector-indexes.md) |

The default `sql_mode` includes `ONLY_FULL_GROUP_BY`, `STRICT_TRANS_TABLES`,
`NO_ZERO_IN_DATE`, `NO_ZERO_DATE`, `ERROR_FOR_DIVISION_BY_ZERO`, and
`NO_ENGINE_SUBSTITUTION`. `TEXT(M)` chooses its LOB tier using the effective
charset's maximum encoded width. See [data types](data-types.md).
Binary COLLATE inputs require a compatible charset or explicit conversion;
prepared executions validate their bound domains. Conditional numeric results,
year-0000 arithmetic, window positions and affected-row counts have documented
[intentional differences from MySQL](mysql80-compatibility.md#intentional-sql-differences).

## Credentials, recovery, and external calls

- Configuring a MySQL receiver channel enables `mydb_replica_read_only` by
  default, including before START. STOP alone can leave the guard enabled.
  Channel passwords are plaintext metadata protected by ownership, private
  permissions and safe path handling. See [receiver setup](statement/replication/change-replication-source.md)
  and [security deployment](security-deployment.md).
- Native replication requires a shared protected `MYDB_REPL_AUTH_TOKEN` of
  16–4096 bytes, including on loopback; remote transport also requires mutual
  TLS. See [native replication](native-replication.md).
- Full backups capture persisted SQL variables at the full-base boundary.
  Later WAL does not reconstruct later SET PERSIST changes. Independent
  restores omit the original node identity and local binary-log history.
  See [backup scope](backup-and-recovery.md#1-capabilities-and-recovery-scope).
- Backup `gc` protects catalog-reachable objects even when their payload is
  damaged. Use `verify` to diagnose payloads and `prune` to change retention.
  See [repository maintenance](backup-and-recovery.md#13-recovery-planning-diagnostics-and-interruption-handling).
- AI call admission counts sibling and conditional calls before provider work
  when their inputs can be determined. ALTER AI MODEL configuration replaces
  the complete document and can only tighten capabilities. See
  [AI execution boundaries](ai.md#execution-boundaries).

These are source changes, not an additional published release or new benchmark
claim. Dated [performance reports](performance-comparison.md) retain their tested
revisions, settings and limitations.
