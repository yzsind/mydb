# MyDB 1.0.3 — Public Preview

## Overview

MyDB 1.0.3 adds **partial indexes and transactional full-text scoring statistics**,
reduces DELETE and HNSW overhead, and improves MySQL compatibility, recovery,
and replication security. These notes cover source changes since
[MyDB 1.0.2](https://github.com/yzsind/mydb/releases/tag/v1.0.2).

- **Partial indexes:** index only qualifying rows, including unique, full-text,
  spatial, HNSW, and IVF indexes.
- **Full-text search:** maintain corpus statistics in the same transaction as
  document changes, preserving snapshot and rollback semantics.
- **Execution and memory:** reduce transactional index cleanup and vector
  navigation overhead, with improvements to vectorized expression evaluation.
- **MySQL compatibility:** extend joined UPDATE/DELETE and correct function,
  metadata, prepared-statement, and diagnostic behavior.
- **Error messages:** identify offending columns, input rows, and invalid values
  more precisely, and preserve conversion warnings across execution paths to
  help AI agents diagnose failures more efficiently and make targeted corrections.
- **Operations:** strengthen replication credential protection, replica recovery,
  physical backup handling, and REST session cleanup.

**Release maturity:** 1.0.3 remains a public preview for feature validation,
compatibility testing, and evaluation in controlled environments. The MySQL
compatibility baseline remains **8.0.46**. Upgrading from 1.0.2 requires logical
migration to a fresh data directory; see the upgrade instructions below.

## Partial indexes

Index definitions accept a `WHERE` predicate:

```sql
CREATE INDEX active_email ON users(email) WHERE active = 1;
CREATE UNIQUE INDEX active_username ON users(username) WHERE active = 1;
```

- Ordinary and unique B-tree indexes, FULLTEXT, SPATIAL, HNSW, and IVF support
  predicates. Primary keys cannot be partial, and foreign keys still require
  complete supporting indexes.
- Only rows for which the predicate is SQL TRUE enter the index. Updates to
  predicate columns maintain membership even when indexed keys do not change.
  Unique constraints apply only to qualifying rows.
- Predicates support deterministic local scalar expressions. Unsupported
  expressions, including subqueries, variables, and volatile functions, return
  explicit errors. Creation-time expression settings are persisted.
- The optimizer uses a partial index only when the query proves its predicate.
  Prepared access paths reconsider current parameter values; `FORCE INDEX`
  cannot bypass membership requirements.
- `SHOW CREATE TABLE`, `CREATE TABLE ... LIKE`, column renames, recovery, and
  physical replication preserve predicate definitions and semantics.

See [partial indexes](partial-indexes.md).

## Full-text search and vector retrieval

- FULLTEXT indexes maintain document count, total token length, and term
  document frequencies in persisted MVCC counters. Compatible indexes share
  the same whole-table corpus, including rows outside a partial index.
- Counter updates participate in data transactions, savepoints, rollback, XA,
  WAL recovery, physical replication, and backups. Scoring honors read snapshots
  and a transaction's own changes. Snapshots predating index publication retain
  a visible-document fallback.
- Current-view scoring reads bounded counter ranges for the requested terms
  instead of scanning unrelated user rows to initialize statistics.
- HNSW construction and navigation use more compact edge and payload layouts,
  bounded prefetching, and improved scratch-memory reclamation. Optional
  navigation-cache allocation can fall back to the durable paged index under
  memory pressure.
- Linux memory reclamation better distinguishes useful clean mapped pages from
  memory required for active or admitted allocations, reducing unnecessary
  eviction of hot vector data.
- Full-text fixes preserve ngram wildcard and phrase boundaries. Both spellings
  of query expansion return an explicit unsupported-feature error.

See [full-text search](fulltext-search.md), [vector search](vector-search.md),
and [vector indexes](vector-indexes.md).

## SQL compatibility and execution

- Joined UPDATE and DELETE support INNER/CROSS/LEFT/RIGHT chains and
  uncorrelated derived SELECT sources, including aggregates and set operations.
  Outer-join NULL padding never becomes a writable target. Derived results
  remain read-only, and LATERAL DML sources remain explicitly unsupported.
- Trigger, view, generated-column, constraint, and RETURNING fixes preserve
  statement atomicity, affected-row reporting, and final row images.
  RETURNING results are charged to the query memory budget; admission failure
  rolls back the statement.
- DELETE reduces repeated heap, B-tree, and transactional index-cleanup work.
  Prepared range operations preserve parameter rebinding, current reads,
  savepoint rollback, and constraint checks.
- Scalar and vectorized execution receive fixes for aggregate boundaries,
  grouping, derived-column types, text/binary lanes, JSON, temporal conversion,
  and result metadata. Parallel projection releases workers on cancellation
  and stream closure.
- Integer arguments to `ROUND`, `TRUNCATE`, `FORMAT`, and `CONV` follow MySQL's
  type-sensitive conversion: exact DECIMAL rounds half away from zero,
  approximate numbers round ties to even, and strings truncate toward zero.
  Overflow diagnostics are preserved.
- Additional fixes cover auto-increment exhaustion, INSERT IDs, conversion
  warnings, incomplete dates, invalid leap seconds, collation validation,
  regular-expression validation order, binary SOUNDEX input, and text result
  types for `QUOTE` and `TO_BASE64`.
- SHOW and prepared metadata more accurately preserve collations, numeric
  defaults, expression domains, and table timestamps. NUL-containing
  identifiers are rejected.
- Zone Map pruning accounts for transaction visibility so a summary cannot
  exclude rows required by the active read view.

See [MySQL compatibility](mysql80-compatibility.md),
[UPDATE](statement/dml/update.md), [DELETE](statement/dml/delete.md), and
[optimizer and execution](optimizer-and-operators.md).

## Error messages and diagnostics

Richer error context is designed to help AI agents diagnose failures more
efficiently. Identifying the offending table, column, input row, and value helps
an agent locate the cause and choose a targeted SQL or data correction, reducing
guesswork and repeated diagnostic queries.

- INSERT validation errors identify the offending column and input row for
  out-of-range values, oversized strings, and invalid ENUM values. For example,
  an invalid TINYINT value in the second input row reports
  `Out of range value for column 'ti' at row 2`. Column/value count mismatches
  also report the failing input row.
- Assigning a non-DEFAULT value to a generated column names both the column
  and table. ALTER TABLE conversion errors retain the original invalid value,
  column name, and source row, making failed schema changes easier to diagnose.
- Integer assignment distinguishes truncated numeric input from invalid input.
  Conversion warnings preserve repeated expression occurrences, and cached
  expressions replay DECIMAL overflow warnings instead of losing diagnostics.
- Statement information follows MySQL's `Records`, `Duplicates`, and `Warnings`
  formatting and respects intentionally empty trailers. Protocol responses no
  longer reuse information left by a previous statement on the execution worker.

See [error codes](error-codes.md) and
[SHOW WARNINGS](statement/metadata/show-diagnostics.md).

## Recovery, replication, security, and APIs

- Backup and restore fixes preserve temporal history and include persisted SQL
  variables in the full-base snapshot. Later `SET PERSIST` changes are not
  reconstructed by WAL continuation. Restores retain imported upstream GTID
  receipts without inheriting local binlog files or execution history.
- Backup garbage collection validates catalog reachability before deleting
  objects and keeps damaged reachable objects protected. Retention pruning
  continues to verify retained recovery payloads.
- Native replica recovery requires a consistent re-seed when progress metadata
  is missing and preserves invisibility of recovered in-flight transactions.
  RESET keeps a physical replica read-only. Promotion reserves the replication
  listener before changing the durable role, so a bind failure leaves the
  previous role intact.
- Configuring a MySQL replication channel enables `mydb_replica_read_only` by
  default, including before START REPLICA. Staging a channel on a writable
  server requires explicitly disabling this protection.
- Replication credential metadata uses private Unix directories and files,
  rejects unsafe ownership, permissions, symbolic links, and hard-linked
  credential files, and uses private temporary files with atomic replacement.
  Channel passwords remain unencrypted at rest; Windows deployments require
  appropriate server-account ACLs.
- REST request bodies have a 10-second read deadline. Readiness reflects
  shutdown and MVCC fail-stop. Listener failure handling closes affected
  services and retains transaction cleanup responsibilities.
- REST session deletion reports `SESSION_CLEANUP_FAILED` when bounded cleanup
  cannot finish, retains context for background retries, and does not claim a
  confirmed rollback. AI item budgeting includes sibling and conditional calls
  before contacting external providers when the bound can be established.
- URL dependency updates address a security advisory. Authentication and
  replication diagnostics receive additional credential-redaction coverage.

See [backup and recovery](backup-and-recovery.md),
[native replication](native-replication.md), [configuration](configuration.md),
[REST SQL API](rest-api.md), and [AI calls](ai.md).

## Moving from 1.0.2 to 1.0.3

**Use a fresh data directory.** The catalog format advances from **72 to 75**
for partial-index predicates, transactional FULLTEXT corpora, and publication
fences. The data-directory admission fingerprint rejects the earlier format.
A 1.0.3 binary cannot directly reuse a 1.0.2 data directory or physical backup;
native physical replication is not a cross-format migration path.

1. Preserve the 1.0.2 binary, external keys, startup configuration, and a verified
   backup that can be restored with the original compatible binary.
2. Export supported logical schema and data, then load them into an independent
   1.0.3 instance with a new `MYDB_DATA_DIR`.
3. Validate row counts, data, representative queries, transactions, indexes,
   privileges, and application behavior before switching writes.
4. Recreate native replicas from the migrated primary. Plan rollback before
   accepting new writes; the old copy will not contain subsequent changes.

The default isolation level remains READ COMMITTED. REPEATABLE READ uses
snapshot isolation without InnoDB gap or next-key locks. MySQL replication
requires ROW/FULL, and native promotion requires external fencing.

See [changing versions](changing-versions.md), the
[storage format registry](storage-format-versions.md), and
[feature boundaries](feature-overview.md).

## Downloads, documentation, and feedback

- [GitHub Releases](https://github.com/yzsind/mydb/releases) for published
  packages, checksums, and package-specific validation details
- [Source changes since 1.0.2](https://github.com/yzsind/mydb/compare/v1.0.2...24bbc95f45eb1d7450e6f2801a986c06a7111a67)
- [Documentation](README.md) and [issue reporting](reporting-issues.md)
- [Report an issue](https://github.com/yzsind/mydb/issues)
- [Apache License 2.0](../LICENSE)
