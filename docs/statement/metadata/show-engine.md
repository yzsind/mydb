# `SHOW ENGINE`

## Description

Returns a concise report of the native mydb engine, or its mutex metadata.
`InnoDB` is a compatibility alias for the native engine monitor.

## Syntax

```sql
SHOW ENGINE MYDB STATUS;
SHOW ENGINE INNODB STATUS;
SHOW ENGINE MYDB MUTEX;
```

Engine names are case-insensitive. Unknown engines return error 1286.
The existing MYDB MUTEX result has the same three-column schema and zero rows;
per-latch measurements are not implemented. INNODB MUTEX is not an alias.

## Privileges and Results

STATUS requires global `PROCESS` and returns exactly one row:

| Column | Value |
| --- | --- |
| `Type` | `mydb` for MYDB, `InnoDB` for INNODB |
| `Name` | Empty string |
| `Status` | Multi-line `MYDB MONITOR OUTPUT` report |

Use the MySQL client's vertical output for readability:

```text
mysql> SHOW ENGINE MYDB STATUS\G
*************************** 1. row ***************************
  Type: mydb
  Name:
Status: ----------------------------------------------------------------
MYDB MONITOR OUTPUT
...
```

`SHOW ENGINE INNODB STATUS\G` returns the same native report with `Type: InnoDB`.
The report contains these sections in order:

| Section | Contents |
| --- | --- |
| OVERVIEW | UTC collection time, uptime, durability and connection capacity |
| TRANSACTIONS | Active storage transactions, oldest snapshot, prepared XA and oldest transactions |
| LOCKS | Current waits, cumulative waits/deadlocks, wait durations and blocker identities |
| WAL AND CHECKPOINT | Next allocatable and durable LSNs, checkpoint/redo bounds, WAL sync and group commit, last successful checkpoint |
| BUFFER POOL AND I/O | Page capacity/residency, dirty and pinned pages, hits/misses, reads/writes, flushes and errors |
| MEMORY AND EXECUTION | Controlled memory, advisory working-set target, operational ceiling, peak RSS, executor memory, spills and row mutations |
| UNDO AND PURGE | Resident/disk undo, records, retention, pressure and purge errors |
| CACHES | Statement, logical/physical plan and prepared DML cache summaries |

Transaction and wait-edge lists each show at most ten entries, oldest first,
with deterministic transaction-ID tie breaking. Omitted counts are explicit.
Transaction details include connection identity when available, state, age,
access mode and undo presence. SQL excerpts are limited to 256 Unicode characters;
control characters are escaped. Connection identity can be `unknown` after a
concurrent disconnect or for a detached transaction. A wait can have multiple blockers.

Time units and IEC capacity units are explicit. Times are UTC. Unsampled ratios
are `N/A`; an absent successful checkpoint in the current server run is `never`. Totals are cumulative for
the lifetime of their data source; gauges describe current state. Separate
subsystem snapshots can observe concurrent changes and are not globally atomic.
Reading the report does not commit user DML. LSN distances are not byte counts.
RSS and controlled memory have different accounting boundaries and must not be added.

## Complete Structured Metrics

The report intentionally summarizes core operation. Full diagnostic metrics,
including executor, online DDL, Zone Map, vector-index and recovery details, are
available from the read-only `information_schema.MYDB_ENGINE_STATUS` table:

| Column | SQL type | Meaning |
| --- | --- | --- |
| `METRIC_NAME` | VARCHAR | Diagnostic name, including per-index names |
| `METRIC_VALUE` | LONGTEXT | Original unscaled value; numbers are decimal strings |

```sql
SELECT METRIC_NAME, METRIC_VALUE
FROM information_schema.MYDB_ENGINE_STATUS
WHERE METRIC_NAME LIKE 'checkpoint%'
ORDER BY METRIC_NAME;

SELECT CAST(METRIC_VALUE AS UNSIGNED) AS current_lsn
FROM information_schema.MYDB_ENGINE_STATUS
WHERE METRIC_NAME = 'current_lsn';
```

All metrics formerly returned as individual SHOW ENGINE rows are available in
this table. Metric names retain their unit suffixes and existing semantics;
composite diagnostic strings remain strings. Without global `PROCESS`, the
table returns zero rows. Ordinary SQL projection, filtering and explicit
numeric conversion are supported. The table is a live, near-time diagnostic
snapshot and does not commit user DML.

See [ordered columns and wire types](result-columns.md#show-engine).
