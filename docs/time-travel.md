# Time Travel Queries

`FOR SYSTEM_TIME AS OF` reads committed table data at an earlier time using
retained MVCC undo. Use it to inspect changes, compare current and historical
rows, or build an application-level repair. It does not restore a table or
replace a backup.

## Enable retention and query

The history window, `mydb_undo_retention_ms`, defaults to `0`; configure it
before the changes whose history you need. This administrative example uses
a disposable table in `test` and changes the node's global retention policy:

```sql
SET GLOBAL mydb_undo_retention_ms = 3600000;
SET time_zone = '+00:00';
DROP TABLE IF EXISTS test.history_demo;
CREATE TABLE test.history_demo(id INT PRIMARY KEY, value INT);
INSERT INTO test.history_demo VALUES (1, 10);

SET @history_time = NOW(6);
UPDATE test.history_demo SET value = 20 WHERE id = 1;

PREPARE history_read FROM
  'SELECT value FROM test.history_demo FOR SYSTEM_TIME AS OF ?';
EXECUTE history_read USING @history_time;
SELECT value FROM test.history_demo;
```

The historical query returns `10`; the current query returns `20`. The time
must fall after the original commit and before the later commit. Use explicit
precision and a consistent session time zone when exchanging timestamps with
clients. Prepared parameters and deterministic temporal arithmetic are also
accepted; each AS OF expression is evaluated once at statement start.
User variables cannot appear directly in AS OF: bind their values through
EXECUTE USING as above, or submit a temporal literal.

`SET GLOBAL` changes the live node. Use `SET PERSIST` if the policy must survive
a restart. Increasing retention cannot recreate history that has already been
purged. Shrinking it can immediately make older targets unavailable. Retained
versions consume undo, metadata and disk space; see
[undo configuration](configuration-resources.md#undo-and-history). Enabling
retention from zero establishes a new visibility baseline; earlier targets
remain unavailable. Active write transactions can make enabling return 1205;
finish those transactions and retry.

Capacity and retention-guarantee settings also affect available history. Monitor
the earliest available time rather than treating the configured duration as a
backup guarantee; shrinking the window or purging history is irreversible.

## Compare current and historical rows

After the setup above, each base-table reference can select its own time:

```sql
PREPARE history_compare FROM
  'SELECT current_row.id, old_row.value AS previous_value,
       current_row.value AS current_value
FROM test.history_demo AS current_row
JOIN test.history_demo FOR SYSTEM_TIME AS OF ? AS old_row
  ON old_row.id = current_row.id';
EXECUTE history_compare USING @history_time;
```

This returns `(1, 10, 20)`. Historical base tables can appear in joins,
subqueries, CTEs, set operations and view definitions within the ordinary
query's supported shapes. A view definition
can contain historical base references; an AS OF suffix on a view itself is
not supported. Current transaction writes are not retroactively committed into
an earlier historical read.

## Schema and execution boundaries

- AS OF applies to persistent user base tables. CTE/derived results, views,
  temporary and system tables, locking reads, and DML targets do not accept
  the suffix. Read a historical result first, then issue a separately reviewed
  ordinary DML statement to repair current data.
- Structural schema epochs are boundaries, including index DDL, RENAME and
  TRUNCATE. A retained timestamp does not make the earlier schema readable
  across such a boundary.
  Use the [recycle bin](recycle-bin.md) for eligible retained dropped tables,
  or [backup recovery](backup-and-recovery.md) for an independent recovery image.
- Ordinary historical B-tree key lookup can use the persistent history index,
  with heap/undo fallback when that accelerator is degraded. Historical queries
  do not use current-only index coverage or Zone Map summaries as historical
  proof.
- HNSW/IVF historical KNN uses exact heap scoring. FULLTEXT historical queries
  build membership and BM25 from visible text. Special-index historical paths
  can therefore cost a scan and more memory than current queries.
- WAL, the commit ledger and retained undo recover historical visibility after
  restart. Their physical formats must match the binary; file deletion and
  format-marker edits are not recovery procedures.

## Diagnostics

```sql
SELECT @@GLOBAL.mydb_undo_retention_ms;
SELECT * FROM information_schema.MYDB_TEMPORAL_HISTORY;
SELECT * FROM information_schema.MYDB_UNDO_HISTORY;
```

Error `50040` means the target is outside available history; `50041` identifies
a structural epoch boundary; `50042` / SQLSTATE `22007` identifies an invalid
time. Preserve the requested time, build identity, schema changes and relevant
history settings when [reporting a failure](reporting-issues.md). A successful
AS OF query does not prove that every table has the same retained window.

See [MVCC and locking](mvcc-and-locking.md),
[compatibility](mysql80-compatibility.md#time-travel-queries), and
[storage formats](storage-format-versions.md).
